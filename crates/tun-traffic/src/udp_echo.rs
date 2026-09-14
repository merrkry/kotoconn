//! Batched UDP echo with an explicit socket buffer, independent of host defaults.
use anyhow::{Result, ensure};
use std::{
    io,
    mem::{MaybeUninit, size_of},
    os::fd::AsRawFd,
};
use tokio::{io::Interest, net::UdpSocket, sync::oneshot, task::JoinHandle};

pub fn receive_buffer(socket: &UdpSocket, requested: u32) -> io::Result<u32> {
    let mut bytes = libc::c_int::try_from(requested).map_err(io::Error::other)?;
    let mut length = size_of::<libc::c_int>() as libc::socklen_t;
    if requested != 0 {
        // SAFETY: The live socket owns the fd; setsockopt copies this integer
        // synchronously. This changes only the benchmark server's socket.
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&bytes as *const libc::c_int).cast(),
                length,
            )
        };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
    }

    // SAFETY: The writable integer and length have the size required by
    // SO_RCVBUF. Both remain live during this synchronous getsockopt call.
    let result = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&mut bytes as *mut libc::c_int).cast(),
            &mut length,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    debug_assert_eq!(length as usize, size_of::<libc::c_int>());
    u32::try_from(bytes).map_err(io::Error::other)
}

struct Batch {
    buffers: Vec<Vec<u8>>,
    peers: Vec<MaybeUninit<libc::sockaddr_storage>>,
    peer_lengths: Vec<libc::socklen_t>,
    lengths: Vec<usize>,
}

impl Batch {
    fn new(count: usize) -> Self {
        Self {
            buffers: (0..count).map(|_| vec![0; 65536]).collect(),
            peers: (0..count).map(|_| MaybeUninit::uninit()).collect(),
            peer_lengths: vec![0; count],
            lengths: vec![0; count],
        }
    }

    fn messages(
        &mut self,
        offset: usize,
        count: usize,
        receive: bool,
    ) -> ([libc::iovec; 32], [libc::mmsghdr; 32]) {
        debug_assert!(offset + count <= self.buffers.len() && count <= 32);
        let vectors = std::array::from_fn(|index| {
            if index >= count {
                return libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                };
            }
            let slot = offset + index;
            libc::iovec {
                iov_base: self.buffers[slot].as_mut_ptr().cast(),
                iov_len: if receive {
                    self.buffers[slot].len()
                } else {
                    self.lengths[slot]
                },
            }
        });
        let messages = std::array::from_fn(|index| {
            let (peer, length) = if index < count {
                let slot = offset + index;
                (
                    self.peers[slot].as_mut_ptr().cast(),
                    if receive {
                        size_of::<libc::sockaddr_storage>() as _
                    } else {
                        self.peer_lengths[slot]
                    },
                )
            } else {
                (std::ptr::null_mut(), 0)
            };
            libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: peer,
                    msg_namelen: length,
                    msg_iov: std::ptr::null_mut(),
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            }
        });
        (vectors, messages)
    }

    fn receive(&mut self, socket: &UdpSocket) -> io::Result<usize> {
        let count = self.buffers.len();
        let (mut vectors, mut messages) = self.messages(0, count, true);
        // Set pointers after the returned arrays reach their final stack location.
        for (message, vector) in messages.iter_mut().zip(&mut vectors).take(count) {
            message.msg_hdr.msg_iov = vector;
        }
        // SAFETY: Each message points to a distinct writable payload and peer
        // buffer. All iovecs and messages remain live throughout this synchronous
        // call. MSG_DONTWAIT and a null timeout keep the syscall nonblocking.
        let received = unsafe {
            libc::recvmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                count as u32,
                libc::MSG_DONTWAIT,
                std::ptr::null_mut(),
            )
        };
        if received < 0 {
            return Err(io::Error::last_os_error());
        }
        debug_assert!(received as usize <= count);
        for (index, message) in messages.iter().enumerate().take(received as usize) {
            if message.msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
                return Err(io::Error::other("truncated batched UDP echo"));
            }
            debug_assert!(message.msg_len as usize <= self.buffers[index].len());
            debug_assert!(
                message.msg_hdr.msg_namelen as usize <= size_of::<libc::sockaddr_storage>()
            );
            self.lengths[index] = message.msg_len as usize;
            self.peer_lengths[index] = message.msg_hdr.msg_namelen;
        }
        Ok(received as usize)
    }

    fn send(&mut self, socket: &UdpSocket, offset: usize, count: usize) -> io::Result<usize> {
        let (mut vectors, mut messages) = self.messages(offset, count, false);
        for (message, vector) in messages.iter_mut().zip(&mut vectors).take(count) {
            message.msg_hdr.msg_iov = vector;
        }
        // SAFETY: The previous receive initialized each payload and sockaddr
        // prefix to the recorded lengths. No receive can replace them until the
        // caller finishes sending this batch. All descriptor arrays are local.
        let sent = unsafe {
            libc::sendmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                count as u32,
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        if sent == 0 {
            return Err(io::Error::other("batched UDP echo made no progress"));
        }
        debug_assert!(sent as usize <= count);
        for (index, message) in messages.iter().enumerate().take(sent as usize) {
            debug_assert_eq!(message.msg_len as usize, self.lengths[offset + index]);
        }
        Ok(sent as usize)
    }
}

pub fn serve(
    socket: UdpSocket,
    mut stopping: oneshot::Receiver<()>,
    count: usize,
) -> JoinHandle<Result<u64>> {
    tokio::spawn(async move {
        ensure!(
            (1..=32).contains(&count),
            "UDP echo batch must be in 1..=32"
        );
        let mut batch = Batch::new(count);
        let mut controls = 0;
        loop {
            let received = tokio::select! {
                _ = &mut stopping => return Ok(controls),
                received = socket.async_io(Interest::READABLE, || batch.receive(&socket)) => received?,
            };
            for index in 0..received {
                let bytes = &batch.buffers[index][..batch.lengths[index]];
                ensure!(
                    !bytes.starts_with(b"INVALID"),
                    "malformed datagram reached outbound"
                );
                controls += u64::from(bytes.starts_with(b"CONTROL"));
            }

            let mut offset = 0;
            while offset < received {
                offset += socket
                    .async_io(Interest::WRITABLE, || {
                        batch.send(&socket, offset, received - offset)
                    })
                    .await?;
            }
            tokio::task::yield_now().await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn echo_preserves_peers_and_datagram_boundaries() {
        for address in ["127.0.0.1:0", "[::1]:0"] {
            let socket = UdpSocket::bind(address).await.unwrap();
            let peer = socket.local_addr().unwrap();
            let a = UdpSocket::bind(address).await.unwrap();
            let b = UdpSocket::bind(address).await.unwrap();
            let (stop, stopping) = oneshot::channel();
            let server = serve(socket, stopping, 32);
            a.send_to(b"", peer).await.unwrap();
            b.send_to(&vec![42; 60000], peer).await.unwrap();
            let mut bytes = vec![0; 65536];
            let n = tokio::time::timeout(Duration::from_secs(5), a.recv(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(n, 0);
            let n = tokio::time::timeout(Duration::from_secs(5), b.recv(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&bytes[..n], vec![42; 60000]);
            stop.send(()).unwrap();
            assert_eq!(server.await.unwrap().unwrap(), 0);
        }
    }
}

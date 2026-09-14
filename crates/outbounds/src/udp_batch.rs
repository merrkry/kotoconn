use kotoconn_protocol::{Packet, pool::Lease};
use std::{io, os::fd::AsRawFd};
use tokio::net::UdpSocket;

pub(super) fn send(socket: &UdpSocket, packets: &[Packet]) -> io::Result<usize> {
    debug_assert!(!packets.is_empty() && packets.len() <= 32);
    // SAFETY: Zero is a valid empty msghdr/iovec. All used pointers below borrow
    // immutable packets for this synchronous syscall; no pointer crosses await.
    let mut headers: [libc::mmsghdr; 32] = unsafe { std::mem::zeroed() };
    let mut vectors = std::array::from_fn::<_, 32, _>(|_| libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    });
    for (index, packet) in packets.iter().enumerate() {
        vectors[index] = libc::iovec {
            iov_base: packet.payload.as_ptr().cast_mut().cast(),
            iov_len: packet.payload.len(),
        };
        headers[index].msg_hdr.msg_iov = &mut vectors[index];
        headers[index].msg_hdr.msg_iovlen = 1;
    }
    // SAFETY: This connected socket owns its destination; headers and all their
    // payload vectors remain live for the call. sendmmsg does not modify payload.
    let n = unsafe {
        libc::sendmmsg(
            socket.as_raw_fd(),
            headers.as_mut_ptr(),
            packets.len() as u32,
            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        )
    };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub(super) fn receive(
    socket: &UdpSocket,
    buffers: &mut [Lease],
    lengths: &mut [usize; 32],
) -> io::Result<usize> {
    debug_assert!(!buffers.is_empty() && buffers.len() <= 32);
    // SAFETY: Zeroed headers have no ancillary/address buffers. The connected
    // socket supplies the peer, and each mutable lease is exclusively borrowed.
    let mut headers: [libc::mmsghdr; 32] = unsafe { std::mem::zeroed() };
    let mut vectors = std::array::from_fn::<_, 32, _>(|_| libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    });
    for (index, buffer) in buffers.iter_mut().enumerate() {
        let bytes = buffer.as_mut();
        vectors[index] = libc::iovec {
            iov_base: bytes.as_mut_ptr().cast(),
            iov_len: bytes.len(),
        };
        headers[index].msg_hdr.msg_iov = &mut vectors[index];
        headers[index].msg_hdr.msg_iovlen = 1;
    }
    // SAFETY: The 32 descriptors point to distinct writable leases with the stated
    // lengths. MSG_DONTWAIT and a null timeout make this a nonblocking operation.
    let n = unsafe {
        libc::recvmmsg(
            socket.as_raw_fd(),
            headers.as_mut_ptr(),
            buffers.len() as u32,
            libc::MSG_DONTWAIT,
            std::ptr::null_mut(),
        )
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    for index in 0..n as usize {
        lengths[index] =
            if headers[index].msg_hdr.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
                usize::MAX
            } else {
                headers[index].msg_len as usize
            };
    }
    Ok(n as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kotoconn_protocol::{pool::Pool, target};
    use tokio::io::Interest;

    #[tokio::test]
    async fn batches_preserve_empty_datagrams_and_a_failed_suffix() {
        for address in ["127.0.0.1:0", "[::1]:0"] {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let a = UdpSocket::bind(address).await.unwrap();
                let b = UdpSocket::bind(address).await.unwrap();
                a.connect(b.local_addr().unwrap()).await.unwrap();
                b.connect(a.local_addr().unwrap()).await.unwrap();
                let destination = target(b.local_addr().unwrap());
                let packet = |n: usize| Packet {
                    target: destination.clone(),
                    payload: vec![n as u8; n].into(),
                };
                let packets = [
                    packet(0),
                    packet(1472),
                    packet(64000),
                    packet(65536),
                    packet(1),
                ];
                let sent = a
                    .async_io(Interest::WRITABLE, || send(&a, &packets))
                    .await
                    .unwrap();
                assert_eq!(sent, 3);
                assert!(send(&a, &packets[sent..]).is_err());
                assert_eq!(send(&a, &packets[sent + 1..]).unwrap(), 1);
                let pool = Pool::default();
                let mut buffers: Vec<_> = (0..4).map(|_| pool.acquire(65536)).collect();
                let mut lengths = [0; 32];
                let mut received = Vec::new();
                while received.len() < 4 {
                    let count = b
                        .async_io(Interest::READABLE, || {
                            receive(&b, &mut buffers, &mut lengths)
                        })
                        .await
                        .unwrap();
                    for i in 0..count {
                        received.push(buffers[i].as_ref()[..lengths[i]].to_vec());
                    }
                }
                assert_eq!(
                    received,
                    [Vec::new(), vec![192; 1472], vec![0; 64000], vec![1]]
                );
                assert_eq!(send(&a, &packets[1..2]).unwrap(), 1);
                let mut small = [pool.acquire(1)];
                b.async_io(Interest::READABLE, || receive(&b, &mut small, &mut lengths))
                    .await
                    .unwrap();
                assert_eq!(lengths[0], usize::MAX);
            })
            .await
            .unwrap();
        }
    }
}

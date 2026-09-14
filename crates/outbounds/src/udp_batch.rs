use kotoconn_protocol::{Packet, pool::Lease};
use std::{io, os::fd::AsRawFd};
use tokio::net::UdpSocket;

pub(super) struct Sender {
    gso: bool,
}

impl Default for Sender {
    fn default() -> Self {
        Self { gso: true }
    }
}

impl Sender {
    pub fn send(&mut self, socket: &UdpSocket, packets: &[Packet]) -> io::Result<usize> {
        if self.gso && packets.len() > 1 && !packets[0].payload.is_empty() {
            let size = packets[0].payload.len();
            let count = packets
                .iter()
                .take_while(|packet| packet.payload.len() == size)
                .count()
                .min(65535 / size);
            if count > 1 {
                match send_gso(socket, &packets[..count], size as u16) {
                    Ok(()) => return Ok(count),
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::EINVAL | libc::EIO | libc::ENOPROTOOPT | libc::EMSGSIZE)
                        ) =>
                    {
                        // No datagram was accepted by this sendmsg. Retry the same
                        // prefix normally and remember unsupported route offload.
                        self.gso = false;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        send(socket, packets)
    }
}

fn send_gso(socket: &UdpSocket, packets: &[Packet], size: u16) -> io::Result<()> {
    let mut vectors = std::array::from_fn::<_, 32, _>(|_| libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    });
    for (vector, packet) in vectors.iter_mut().zip(packets) {
        *vector = libc::iovec {
            iov_base: packet.payload.as_ptr().cast_mut().cast(),
            iov_len: packet.payload.len(),
        };
    }
    // SAFETY: A zeroed msghdr is valid. usize storage gives cmsghdr alignment,
    // and the fixed array exceeds CMSG_SPACE(sizeof(u16)) on supported Linux ABIs.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    let mut control = [0usize; 8];
    message.msg_iov = vectors.as_mut_ptr();
    message.msg_iovlen = packets.len() as _;
    message.msg_control = control.as_mut_ptr().cast();
    // SAFETY: CMSG_SPACE performs size arithmetic only.
    message.msg_controllen = unsafe { libc::CMSG_SPACE(2) } as _;
    debug_assert!(message.msg_controllen as usize <= std::mem::size_of_val(&control));
    // SAFETY: The control buffer is aligned, initialized and large enough for one
    // UDP_SEGMENT u16. All packet vectors remain live throughout sendmsg.
    let sent = unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::SOL_UDP;
        (*header).cmsg_type = libc::UDP_SEGMENT;
        (*header).cmsg_len = libc::CMSG_LEN(2) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<u16>(), size);
        libc::sendmsg(
            socket.as_raw_fd(),
            &message,
            (libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) as _,
        )
    };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    debug_assert_eq!(
        sent as usize,
        packets.iter().map(|p| p.payload.len()).sum::<usize>()
    );
    Ok(())
}

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
            (libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) as _,
        )
    };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub(super) fn enable_gro(socket: &UdpSocket) -> io::Result<()> {
    let enabled: libc::c_int = 1;
    // SAFETY: setsockopt reads one initialized integer and retains no pointer.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_UDP,
            libc::UDP_GRO,
            (&enabled as *const libc::c_int).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn receive(
    socket: &UdpSocket,
    buffers: &mut [Lease],
    lengths: &mut [usize; 32],
    segments: &mut [u16; 32],
) -> io::Result<usize> {
    debug_assert!(!buffers.is_empty() && buffers.len() <= 32);
    // SAFETY: Zeroed headers have no ancillary/address buffers. The connected
    // socket supplies the peer, and each mutable lease is exclusively borrowed.
    let mut headers: [libc::mmsghdr; 32] = unsafe { std::mem::zeroed() };
    let mut vectors = std::array::from_fn::<_, 32, _>(|_| libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    });
    let mut controls = [[0usize; 8]; 32];
    for (index, buffer) in buffers.iter_mut().enumerate() {
        let bytes = buffer.as_mut();
        vectors[index] = libc::iovec {
            iov_base: bytes.as_mut_ptr().cast(),
            iov_len: bytes.len(),
        };
        headers[index].msg_hdr.msg_iov = &mut vectors[index];
        headers[index].msg_hdr.msg_iovlen = 1;
        headers[index].msg_hdr.msg_control = controls[index].as_mut_ptr().cast();
        headers[index].msg_hdr.msg_controllen = std::mem::size_of_val(&controls[index]) as _;
    }
    // SAFETY: The 32 descriptors point to distinct writable leases with the stated
    // lengths. MSG_DONTWAIT and a null timeout make this a nonblocking operation.
    let n = unsafe {
        libc::recvmmsg(
            socket.as_raw_fd(),
            headers.as_mut_ptr(),
            buffers.len() as u32,
            libc::MSG_DONTWAIT as _,
            std::ptr::null_mut(),
        )
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    for index in 0..n as usize {
        segments[index] = 0;
        lengths[index] =
            if headers[index].msg_hdr.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
                usize::MAX
            } else {
                headers[index].msg_len as usize
            };
        // SAFETY: The kernel filled at most the supplied aligned control storage.
        // CMSG_NXTHDR checks each subsequent header against msg_controllen.
        unsafe {
            let message = &headers[index].msg_hdr;
            let mut control = libc::CMSG_FIRSTHDR(message);
            while !control.is_null() {
                if (*control).cmsg_level == libc::SOL_UDP && (*control).cmsg_type == libc::UDP_GRO {
                    if (*control).cmsg_len as usize >= libc::CMSG_LEN(4) as usize {
                        let size = std::ptr::read_unaligned(libc::CMSG_DATA(control).cast::<u32>());
                        if size == 0 || size > u16::MAX as u32 {
                            lengths[index] = usize::MAX;
                        } else {
                            segments[index] = size as u16;
                        }
                    } else {
                        lengths[index] = usize::MAX;
                    }
                }
                control = libc::CMSG_NXTHDR(message, control);
            }
        }
    }
    Ok(n as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kotoconn_protocol::{pool::Pool, target};
    use tokio::io::Interest;

    #[tokio::test]
    async fn gso_and_gro_preserve_individual_payloads_in_both_families() {
        for address in ["127.0.0.1:0", "[::1]:0"] {
            for gro in [false, true] {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let a = UdpSocket::bind(address).await.unwrap();
                    let b = UdpSocket::bind(address).await.unwrap();
                    a.connect(b.local_addr().unwrap()).await.unwrap();
                    b.connect(a.local_addr().unwrap()).await.unwrap();
                    if gro {
                        enable_gro(&b).unwrap();
                    }
                    let packets: Vec<_> = (0..32)
                        .map(|i| Packet {
                            target: target(b.local_addr().unwrap()),
                            payload: vec![i; 1024].into(),
                        })
                        .collect();
                    let mut sender = Sender::default();
                    let sent = a
                        .async_io(Interest::WRITABLE, || sender.send(&a, &packets))
                        .await
                        .unwrap();
                    assert_eq!(sent, 32);
                    let pool = Pool::default();
                    let mut buffers: Vec<_> = (0..32).map(|_| pool.acquire(65536)).collect();
                    let mut lengths = [0; 32];
                    let mut segments = [0; 32];
                    let mut received = Vec::new();
                    while received.len() < packets.len() {
                        let count = b
                            .async_io(Interest::READABLE, || {
                                receive(&b, &mut buffers, &mut lengths, &mut segments)
                            })
                            .await
                            .unwrap();
                        for i in 0..count {
                            let bytes = &buffers[i].as_ref()[..lengths[i]];
                            let size = if segments[i] == 0 {
                                bytes.len()
                            } else {
                                usize::from(segments[i])
                            };
                            received.extend(bytes.chunks(size).map(<[u8]>::to_vec));
                        }
                    }
                    assert_eq!(
                        received,
                        packets
                            .iter()
                            .map(|p| p.payload.to_vec())
                            .collect::<Vec<_>>()
                    );
                })
                .await
                .unwrap();
            }
        }
    }

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
                let mut segments = [0; 32];
                let mut received = Vec::new();
                while received.len() < 4 {
                    let count = b
                        .async_io(Interest::READABLE, || {
                            receive(&b, &mut buffers, &mut lengths, &mut segments)
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
                b.async_io(Interest::READABLE, || {
                    receive(&b, &mut small, &mut lengths, &mut segments)
                })
                .await
                .unwrap();
                assert_eq!(lengths[0], usize::MAX);
            })
            .await
            .unwrap();
        }
    }
}

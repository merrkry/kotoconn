use anyhow::Result;
use futures_util::future::BoxFuture;
use kotoconn_config::TunInboundConfig;
use kotoconn_protocol::ServerContext;

pub struct BoundTun {
    pub name: String,
    pub run: BoxFuture<'static, Result<()>>,
}

#[cfg(target_os = "linux")]
pub fn bind(options: TunInboundConfig, context: ServerContext) -> Result<BoundTun> {
    use anyhow::{Context, bail, ensure};
    use rustix::{
        io::Errno,
        net::{self, AddressFamily, SocketFlags, SocketType, netdevice::name_to_index},
    };
    use std::{collections::HashSet, net::IpAddr};

    ensure!(
        !options.name.is_empty()
            && options.name.len() < 16
            && options.name != "."
            && options.name != ".."
            && options
                .name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c)),
        "invalid TUN interface name"
    );
    ensure!(
        (1280..=65535).contains(&options.mtu),
        "TUN MTU must be between 1280 and 65535"
    );
    ensure!(
        !context.udp_idle_timeout.is_zero(),
        "UDP idle timeout must be positive"
    );

    let query = net::socket_with(
        AddressFamily::INET,
        SocketType::DGRAM,
        SocketFlags::CLOEXEC,
        None,
    )
    .context("open interface query socket")?;
    match name_to_index(&query, &options.name) {
        Ok(_) => bail!("interface {} already exists", options.name),
        Err(Errno::NODEV | Errno::NXIO) => {}
        Err(error) => return Err(error).context("check TUN interface name"),
    }

    let mut builder = tun_rs::DeviceBuilder::new()
        .name(&options.name)
        .mtu(options.mtu)
        .offload(true)
        .multi_queue(false)
        .packet_information(false);
    let mut addresses = HashSet::new();
    let mut ipv4 = false;

    for address in options.addresses {
        ensure!(
            crate::packet::unicast(address.address.into()),
            "TUN address must be unicast"
        );
        ensure!(addresses.insert(address.address), "duplicate TUN address");
        match address.address {
            IpAddr::V4(ip) => {
                ensure!(!ipv4, "TUN supports one IPv4 interface address");
                ensure!(address.prefix <= 32, "invalid TUN IPv4 address or prefix");
                ipv4 = true;
                builder = builder.ipv4(ip, address.prefix, None);
            }
            IpAddr::V6(ip) => {
                ensure!(address.prefix <= 128, "invalid TUN IPv6 address or prefix");
                builder = builder.ipv6(ip, address.prefix);
            }
        }
    }

    let device = builder
        .enable(true)
        .build_async()
        .context("create Linux TUN interface")?;

    let name = device.name().context("read TUN interface name")?;
    Ok(BoundTun {
        name,
        run: Box::pin(crate::run(
            crate::offload::OffloadDevice::new(device),
            usize::from(options.mtu),
            context,
        )),
    })
}

#[cfg(not(target_os = "linux"))]
pub fn bind(_: TunInboundConfig, _: ServerContext) -> Result<BoundTun> {
    anyhow::bail!("TUN inbounds are supported only on Linux")
}

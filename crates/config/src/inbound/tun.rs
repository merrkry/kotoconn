use std::net::IpAddr;

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct TunAddress {
    #[ts(type = "IpAddr")]
    pub address: IpAddr,
    #[ts(type = "Byte")]
    pub prefix: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct TunInboundConfig {
    pub name: String,
    #[ts(type = "Mtu")]
    pub mtu: u16,
    pub addresses: Vec<TunAddress>,
}

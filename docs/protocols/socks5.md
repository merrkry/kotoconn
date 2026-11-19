# SOCKS5

Kotoconn reports the local inbound TCP endpoint in the CONNECT reply's
`BND.ADDR` and `BND.PORT`. RFC 1928 assigns these fields to the outbound
endpoint, which is unknown when Kotoconn admits a connection. [SOCKS5 replies][replies]

[replies]: https://www.rfc-editor.org/rfc/rfc1928.html#section-6

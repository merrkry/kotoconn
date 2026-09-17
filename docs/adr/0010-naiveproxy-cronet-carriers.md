# Use SagerNet Cronet for Naive outbound connections

Naive clients depend on Chromium's TLS and HTTP behavior, including HTTP/2
reset padding. The Linux outbound links the checksummed SagerNet Cronet release
used by sing-box through the published `cronet` bindings. Builds download the
shared library and never compile Chromium. There is no local fork. Other
platforms reject Naive outbound construction until their native packaging is
implemented. The inbound uses rustls and h2 for TLS HTTP/2.

Cronet owns one connection pool per outbound TCP client. Its socket hooks bridge
to that client's configured carrier, including nested proxies. Only the outbound
server address enters the configured resolver; destination authorities remain
unchanged. The bridge exposes socket descriptors because Cronet cannot consume
Kotoconn streams directly. HTTP/2 needs a TCP carrier and QUIC needs a UDP
carrier. Both expose TCP tunnels; UDP-over-TCP is a separate protocol extension
and is not implemented.

The adapter implements Naive's eight-frame padding codec using tokio-util.
Padding is negotiated by the CONNECT request and response headers, so ordinary
HTTP/2 proxies and clients remain interoperable. The `cronet` crate's higher-level
Naive connection unconditionally pads payloads and uses blocking I/O; this
adapter uses its native stream callbacks instead. Native I/O buffers survive
cancellation until a terminal callback, including runtime teardown. Closing a
tunnel cancels only that native stream. Closing the TCP client ends its pool
and carrier relays.

The daemon tracks outbound pools separately from inbound sessions. Shutdown
first drains accepted sessions, then closes and waits for the outbound scope.
Its existing deadline cancels both scopes. HTTP/2 inbounds send GOAWAY when
admission stops so idle persistent connections do not prevent shutdown.

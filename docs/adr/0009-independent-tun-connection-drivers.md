# Drive TUN connections independently

A TUN endpoint validates IP packets and dispatches TCP by source and destination. Each connection owns a smoltcp interface, TCP socket and Tokio driver. This follows Quinn's separation of endpoint I/O and connection progress, avoids scanning all TCP sockets for each packet, and lets one connection wait for capacity without blocking others. Bounded Tokio duplex buffers carry application bytes and preserve half-close semantics. Drivers remain tracked while delivering FIN or RST after application work ends.

smoltcp owns TCP state, checksums, closed-port replies and IPv4 fragmentation. Fragmented IPv4 ingress passes through its interface and raw socket before transport dispatch. smoltcp 0.14 has no IPv6 reassembly on the IP medium, so that path uses its wire types and Assembler with bounded storage, expiry and overlap rejection. Ordinary packets bypass the reassembly buffers.

Stopping a TUN inbound ends UDP associations and stops TCP admission. Existing TCP drivers retain device I/O until they drain or the daemon forces cancellation. TUN binding reports an interface name through the same inbound binding boundary used by socket listeners.

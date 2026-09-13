# Drive TUN queues and connections independently

A TUN endpoint runs one worker per queue, with independent receive/dispatch and transmit futures so a pending writer cannot block reception. A stable flow hash assigns each TCP connection or UDP association to one worker. Packets arriving elsewhere are forwarded to their owner. Queue membership stays fixed until the inbound stops, keeping ownership stable. On Linux, replies teach TUN which queue owns a flow and reduce subsequent forwarding.

Each TCP connection owns its smoltcp interface, socket and asynchronous driver. Protocol progress and cleanup do not depend on application reads or writes. smoltcp retains its normal contiguous working buffers and complete TCP state machine; external chunked queues provide application buffering and backpressure. [TUN buffering](../tun-buffering.md) defines the queue and storage policy.

All fragments of a datagram reach one reassembly worker before transport dispatch. IPv4 ownership hashes source, destination, protocol and ID; IPv6 hashes source, destination and ID. Including the protocol in the IPv6 key would separate conflicting fragments that must poison the same assembly. A completed datagram then reaches its flow owner. Shared fragment accounting prevents independent workers from each spending the full reassembly allowance.

Stopping admission ends UDP associations while established TCP connections drain. Each worker reports completion only after observing stopped admission and removing every TCP driver. The supervisor waits for all reports, then drains transmit queues before releasing the device. Forced cancellation interrupts blocked I/O. A failed worker or aborted endpoint cancels that inbound's connections without cancelling other inbounds.

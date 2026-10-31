# Route UDP once per destination-specific session

Within an inbound association, each original destination is its own UDP session. The first datagram triggers routing, and later datagrams use the selected client until the session ends. Different associations never share sessions. Association identity and wire state belong to the adapter.

The inbound's UDP idle timeout expires individual sessions, including one still waiting for policy or outbound setup. Forwarding in either direction refreshes it; waiting for capacity does not. A datagram after expiry is routed again. Expiry does not close shared clients or discard protocol security state.

Datagrams keep their message boundaries and address metadata, including empty payloads. Queues are bounded by bytes. Shared ingress paths drop new packets on overflow instead of blocking, so one session cannot stall the others. Packet loss never closes a session or triggers retransmission in the core.

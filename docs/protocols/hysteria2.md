# Hysteria 2

Kotoconn uses BBR for both client and server. On a saturated link, TCP senders
normally reduce their sending rate when they detect congestion. Hysteria 2's
fixed-bandwidth mode (Brutal) keeps aiming for its configured rate, which can
leave less bandwidth for those connections.

Kotoconn omits Brutal. It can sustain more throughput on some lossy links,
but we judge that improvement in connection quality too limited to justify
the cost to other traffic. We expect no benefit for censorship resistance.
[Hysteria 2 bandwidth settings][bandwidth]

[bandwidth]: https://v2.hysteria.network/docs/advanced/Full-Client-Config/#bandwidth

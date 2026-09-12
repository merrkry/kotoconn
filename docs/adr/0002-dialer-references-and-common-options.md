# Compose dialers by ID and centralize common options

`InboundConfig` and `OutboundConfig` hold common options alongside `InboundImpl` and `OutboundImpl` enums containing protocol-specific options. `DialerConfig` pairs an outbound with an optional carrier ID. This keeps common fields in one place and distinguishes configuration from runtime protocol objects without access-only traits.

UDP idle timeout is shared inbound configuration because direct listeners and TUN both need client UDP session expiry; carrier connection lifetimes remain separate. Direct retains its own configuration type even before it has protocol-specific fields.

Typed IDs allow resources to share carriers without embedding recursive values. IDs transparently wrap `NonZeroU64`, preserving the compact representation of `Option<DialerId>`; reference existence and carrier cycles still require validation when loading configuration.

`Config` stores inbound and dialer definitions in maps keyed by ID, and handler IDs in sets because the host owns the JS functions. Collection iteration order has no policy meaning.

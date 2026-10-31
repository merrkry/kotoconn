# Use prebuilt Cronet for Naive outbounds

A Naive client is only useful if it matches Chromium's TLS and HTTP behavior, so the outbound uses Chromium's network stack through SagerNet's Cronet build, the one sing-box uses. Builds download the checksummed shared library and never compile Chromium. Platforms without a packaged library reject Naive outbounds. The inbound has no fingerprint requirement and uses rustls and h2.

Cronet's socket hooks connect through the outbound's carrier, so Naive works in carrier chains like any other adapter.

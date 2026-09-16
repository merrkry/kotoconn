# Hysteria TLS test fixtures

`cert.pem` and `key.pem` are a self-signed certificate and its test-only private
key. They authenticate `hysteria.test` inside isolated Compose networks. They
were generated with rcgen 0.14.10 using
`generate_simple_self_signed(vec!["hysteria.test".into()])`, whose validity spans
1975 to 4096. Both files must be replaced together when regenerating them.

The official Hysteria process and Kotoconn trust this certificate explicitly.
The fixtures do not disable TLS verification. E2E configurations disable the
official program's update check.

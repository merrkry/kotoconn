# Hysteria TLS test fixtures

`cert.pem` and `key.pem` authenticate `hysteria.test` inside isolated
Compose networks. Both peers trust the self-signed certificate explicitly
so the tests exercise TLS verification. Replace the certificate and its
test-only key together when regenerating them.

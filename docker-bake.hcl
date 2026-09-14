// Nx runs cargo-zigbuild first. Bake shares its artifacts between both exports.
variable "KOTOCONN_RUST_TARGET" {
  default = "x86_64-unknown-linux-gnu.2.36"
}

group "default" {
  targets = ["proxy", "binaries", "tun-runtime"]
}

group "release" {
  targets = ["release-binaries", "tun-runtime"]
}

target "package" {
  context = "."
  dockerfile = "e2e/Dockerfile"
  contexts = {
    artifacts = "./target/${split(".", KOTOCONN_RUST_TARGET)[0]}/debug"
  }
}

target "proxy" {
  inherits = ["package"]
  target = "runtime"
  tags = ["kotoconn-e2e:local"]
  output = ["type=docker"]
}

target "binaries" {
  inherits = ["package"]
  target = "binaries"
  output = ["type=local,dest=target/tun/debug"]
}

target "release-binaries" {
  inherits = ["binaries"]
  contexts = {
    artifacts = "./target/${split(".", KOTOCONN_RUST_TARGET)[0]}/release"
  }
  output = ["type=local,dest=target/tun/release"]
}

target "tun-runtime" {
  context = "."
  dockerfile = "e2e/tun.Dockerfile"
  tags = ["kotoconn-tun:local"]
  output = ["type=docker"]
}

module kotoconn-anytls-interop

go 1.25.0

require (
	anytls v0.0.0
	github.com/sagernet/sing v0.9.6
)

require (
	github.com/chen3feng/stl4go v0.1.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace anytls => github.com/anytls/anytls-go v0.0.14-0.20260803131823-fd6167acd6d7

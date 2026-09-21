// Independent interoperability peer using the unmodified official Go library.
package main

import (
	"anytls/proxy/padding"
	"anytls/proxy/session"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/uot"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	switch os.Args[1] {
	case "server":
		certificate, err := tls.LoadX509KeyPair(os.Args[2], os.Args[3])
		must(err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}})
		must(err)
		// A different scheme tests updates received by the Rust client.
		padding.UpdatePaddingScheme([]byte("stop=3\n0=40-40\n1=256-256\n2=512-512,c,512-512"))
		fmt.Println(listener.Addr())
		for ordinal := 0; ; ordinal++ {
			conn, err := listener.Accept()
			must(err)
			expectedPadding := uint16(40)
			if ordinal == 0 {
				expectedPadding = 30
			}
			go serve(conn, expectedPadding)
		}
	case "client":
		testClient(os.Args[2], os.Args[3])
		fmt.Println("ok")
	default:
		panic("expected server or client")
	}
}

func serve(conn net.Conn, expectedPadding uint16) {
	defer conn.Close()
	must(conn.SetDeadline(time.Now().Add(20 * time.Second)))
	hash := sha256.Sum256([]byte("secret"))
	var received [32]byte
	_, err := io.ReadFull(conn, received[:])
	if err != nil || received != hash {
		return
	}
	var length uint16
	if binary.Read(conn, binary.BigEndian, &length) != nil {
		return
	}
	if length != expectedPadding {
		panic("client did not retain padding update across sessions")
	}
	if _, err = io.CopyN(io.Discard, conn, int64(length)); err != nil {
		return
	}
	peer := session.NewServerSession(conn, func(stream *session.Stream) {
		defer stream.Close()
		destination, err := M.SocksaddrSerializer.ReadAddrPort(stream)
		if err != nil {
			return
		}
		if destination.Port == 1 {
			_ = stream.HandshakeFailure(fmt.Errorf("destination rejected"))
			return
		}
		if destination.Fqdn == uot.MagicAddress {
			request, err := uot.ReadRequest(stream)
			if err != nil {
				return
			}
			if stream.HandshakeSuccess() != nil {
				return
			}
			packets := uot.NewConn(stream, *request)
			for {
				packet := buf.NewSize(65535)
				address, err := packets.ReadPacket(packet)
				if err != nil {
					packet.Release()
					return
				}
				if packets.WritePacket(packet, address) != nil {
					return
				}
			}
		}
		if stream.HandshakeSuccess() != nil {
			return
		}
		if _, err := stream.Write([]byte("ready")); err != nil {
			return
		}
		_, _ = io.Copy(stream, stream)
	}, &padding.DefaultPaddingFactory)
	peer.Run()
	_ = peer.Close()
}

func testClient(server, certificate string) {
	pem, err := os.ReadFile(certificate)
	must(err)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		panic("invalid trust anchor")
	}
	dial := func(ctx context.Context) (net.Conn, error) {
		conn, err := (&tls.Dialer{Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}).DialContext(ctx, "tcp", server)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256([]byte("secret"))
		length := padding.DefaultPaddingFactory.Load().GenerateRecordPayloadSizes(0)[0]
		auth := append(hash[:], byte(length>>8), byte(length))
		auth = append(auth, make([]byte, length)...)
		if _, err = conn.Write(auth); err != nil {
			conn.Close()
			return nil, err
		}
		return conn, nil
	}
	client := session.NewClient(context.Background(), dial, &padding.DefaultPaddingFactory, 30*time.Second, 60*time.Second, 0, false)
	defer client.Close()
	destination := M.ParseSocksaddr("unresolved.invalid:80")
	for i := 0; i < 3; i++ {
		stream, err := client.CreateStream(context.Background())
		must(err)
		must(stream.SetDeadline(time.Now().Add(10 * time.Second)))
		must(M.SocksaddrSerializer.WriteAddrPort(stream, destination))
		greeting := make([]byte, 5)
		_, err = io.ReadFull(stream, greeting)
		must(err)
		if string(greeting) != "ready" {
			panic("wrong greeting")
		}
		payload := bytes.Repeat([]byte{0, 1, 127, 255}, 32768)
		// The reference Stream.Write API emits one frame per call; keep each
		// call below the protocol's uint16 payload limit, like its relay does.
		for offset := 0; offset < len(payload); offset += 32768 {
			_, err = stream.Write(payload[offset : offset+32768])
			must(err)
		}
		reply := make([]byte, len(payload))
		_, err = io.ReadFull(stream, reply)
		must(err)
		if !bytes.Equal(reply, payload) {
			panic("TCP corruption")
		}
		must(stream.Close())
	}
	for _, connected := range []bool{true, false} {
		stream, err := client.CreateStream(context.Background())
		must(err)
		must(stream.SetDeadline(time.Now().Add(10 * time.Second)))
		must(M.SocksaddrSerializer.WriteAddrPort(stream, uot.RequestDestination(2)))
		request := uot.Request{IsConnect: connected, Destination: M.ParseSocksaddr("127.0.0.1:53")}
		must(uot.WriteRequest(stream, request))
		packets := uot.NewConn(stream, request)
		for _, name := range []string{"127.0.0.1:53", "[::1]:53", "dns.invalid:53"} {
			target := M.ParseSocksaddr(name)
			if connected {
				target = request.Destination
			}
			for _, payload := range [][]byte{{}, {42}, bytes.Repeat([]byte{7}, 65507)} {
				must(packets.WritePacket(buf.As(append([]byte(nil), payload...)), target))
				reply := buf.NewSize(65535)
				received, err := packets.ReadPacket(reply)
				must(err)
				if received != target || !bytes.Equal(reply.Bytes(), payload) {
					panic("UDP corruption")
				}
				reply.Release()
			}
		}
		must(stream.Close())
	}
}

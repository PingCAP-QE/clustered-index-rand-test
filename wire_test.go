package main

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// wire is what a server sees of one connection attempt: what arrived before
// TLS, how the TLS handshake went, and what was read inside TLS.
type wire struct {
	plain     []byte
	tlsErr    error
	encrypted string
}

// mysqlGreeting is the first packet of the MySQL protocol, offering TLS when
// asked to.
func mysqlGreeting(withTLS bool) []byte {
	caps := uint32(0x00088201) // LONG_PASSWORD | PROTOCOL_41 | SECURE_CONNECTION | PLUGIN_AUTH
	if withTLS {
		caps |= 0x800
	}
	packet := append([]byte{10}, []byte("5.7.25\x00")...)
	packet = append(packet, 1, 0, 0, 0)
	packet = append(packet, []byte("12345678\x00")...)
	packet = append(packet, byte(caps), byte(caps>>8), 33, 2, 0, byte(caps>>16), byte(caps>>24), 21)
	packet = append(packet, make([]byte, 10)...)
	packet = append(packet, []byte("123456789012\x00mysql_native_password\x00")...)
	return mysqlPacket(packet, 0)
}

// mysqlPacket frames a payload. sequence is the MySQL packet sequence number,
// which the client checks: a reply carries the number of the packet it answers
// plus one.
func mysqlPacket(payload []byte, sequence byte) []byte {
	return append([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), sequence}, payload...)
}

// readMySQLPacket reads one MySQL protocol packet and its sequence number.
func readMySQLPacket(reader io.Reader) ([]byte, byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, 0, err
	}
	// The length is the first three bytes. Reading it in place keeps the
	// sequence number in the fourth byte intact.
	size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	packet := make([]byte, size)
	if _, err := io.ReadFull(reader, packet); err != nil {
		return nil, 0, err
	}
	return packet, header[3], nil
}

// readUntilQuiet returns what arrives on conn before it falls silent or closes.
func readUntilQuiet(conn net.Conn) []byte {
	conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var received []byte
	buffer := make([]byte, 4096)
	for {
		n, err := conn.Read(buffer)
		received = append(received, buffer[:n]...)
		if err != nil {
			return received
		}
	}
}

// observeConnection answers one connection like a MySQL server whose
// certificate is signed by signer (no TLS when signer is nil), while the
// client trusts anchor. It records what the client sends, then refuses the
// login, and returns what was on the wire.
func observeConnection(t *testing.T, anchor, signer *testCA) wire {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	observed := make(chan wire, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			observed <- wire{tlsErr: err}
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(10 * time.Second))
		seen := wire{}
		if _, err := conn.Write(mysqlGreeting(signer != nil)); err != nil {
			observed <- wire{tlsErr: err}
			return
		}
		if signer == nil {
			// Without TLS on offer, anything the client sends is plaintext.
			seen.plain = readUntilQuiet(conn)
			observed <- seen
			return
		}
		// The client must ask for TLS before anything else.
		request, _, err := readMySQLPacket(conn)
		seen.plain = request
		if err != nil {
			seen.tlsErr = err
			observed <- seen
			return
		}
		secure := tls.Server(conn, &tls.Config{
			Certificates: []tls.Certificate{signer.serverCertificate(t)},
			MinVersion:   tls.VersionTLS12,
		})
		if seen.tlsErr = secure.Handshake(); seen.tlsErr != nil {
			observed <- seen
			return
		}
		// The login is only readable from inside the TLS session.
		response, sequence, err := readMySQLPacket(secure)
		if err == nil {
			seen.encrypted = string(response)
		}
		refused := append([]byte{0xff, 0x15, 0x04, '#', '2', '8', '0', '0', '0'}, "Access denied for the test client"...)
		secure.Write(mysqlPacket(refused, sequence+1))
		observed <- seen
	}()

	targets := []allowedTarget{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, User: "worker"}}
	policy, err := loadTargetPolicy(writePolicy(t, approvedPolicy(t, targets, anchor)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = openTestDatabase(ctx, policy, "worker:synthetic-secret@tcp("+listener.Addr().String()+")/")
	select {
	case seen := <-observed:
		return seen
	case <-time.After(10 * time.Second):
		t.Fatal("the server side of the connection never finished")
		return wire{}
	}
}

func TestConnectionsUseThePinnedTLSConfiguration(t *testing.T) {
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")

	// A server the pinned CA signed: the client completes the TLS handshake
	// and sends the login inside it.
	seen := observeConnection(t, trusted, trusted)
	if seen.tlsErr != nil {
		t.Fatalf("the client did not complete a TLS handshake: %v", seen.tlsErr)
	}
	if !strings.Contains(seen.encrypted, "worker") {
		t.Fatalf("no login was read inside the TLS session; wire = plain %q, encrypted %q", seen.plain, seen.encrypted)
	}
	if strings.Contains(string(seen.plain), "worker") || strings.Contains(string(seen.plain), "synthetic-secret") {
		t.Fatalf("credentials were sent before TLS: %q", seen.plain)
	}

	// A server signed by another CA: the handshake fails and no login is sent.
	seen = observeConnection(t, trusted, rogue)
	if seen.tlsErr == nil {
		t.Fatal("the client completed a TLS handshake with a server outside the pinned trust anchor")
	}
	if seen.encrypted != "" {
		t.Fatalf("a login was sent to a server outside the pinned trust anchor: %q", seen.encrypted)
	}

	// A server without TLS: the client sends nothing at all.
	seen = observeConnection(t, trusted, nil)
	if len(seen.plain) != 0 {
		t.Fatalf("the client sent %q to a server that offers no TLS", seen.plain)
	}
}

// acceptingServer answers every connection like a MySQL server that offers
// TLS with a certificate signed by signer and accepts every login and every
// statement. It is what a database handle can actually be opened against.
func acceptingServer(t *testing.T, signer *testCA) net.Addr {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	certificate := signer.serverCertificate(t)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(10 * time.Second))
				if _, err := conn.Write(mysqlGreeting(true)); err != nil {
					return
				}
				if _, _, err := readMySQLPacket(conn); err != nil {
					return
				}
				secure := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
				if err := secure.Handshake(); err != nil {
					return
				}
				for {
					payload, sequence, err := readMySQLPacket(secure)
					if err != nil {
						return
					}
					var reply []byte
					if len(payload) > 0 && payload[0] == 0x03 && strings.Contains(string(payload[1:]), "@@max_allowed_packet") {
						// After the login the driver asks for the largest
						// packet, which is answered with a result set.
						reply = singleValueReply(sequence+1, "4194304")
					} else {
						// A login or a statement: both are answered with OK.
						reply = mysqlPacket([]byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}, sequence+1)
					}
					if _, err := secure.Write(reply); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return listener.Addr()
}

// singleValueReply is the answer to a query that selects one value: one
// column, its definition, the row, and an EOF packet around each.
func singleValueReply(sequence byte, value string) []byte {
	column := lenencString("def")
	column = append(column, 0x00, 0x00, 0x00) // schema, table, org_table
	column = append(column, lenencString("value")...)
	column = append(column, 0x00)                         // org_name
	column = append(column, 0x0c, 0x21, 0x00)             // fixed fields follow, charset 33
	column = append(column, 0x0b, 0x00, 0x00, 0x00, 0x08) // column length, type LONGLONG
	column = append(column, 0x00, 0x00, 0x00, 0x00, 0x00) // flags, decimals, filler
	eof := []byte{0xfe, 0x00, 0x00, 0x02, 0x00}
	var reply []byte
	for index, packet := range [][]byte{{0x01}, column, eof, lenencString(value), eof} {
		reply = append(reply, mysqlPacket(packet, sequence+byte(index))...)
	}
	return reply
}

func lenencString(value string) []byte {
	return append([]byte{byte(len(value))}, []byte(value)...)
}

// driverKnowsTLS asks the driver itself whether it can still build a
// connection for a TLS configuration name.
func driverKnowsTLS(name string) bool {
	configuration := mysql.NewConfig()
	configuration.TLSConfig = name
	_, err := mysql.NewConnector(configuration)
	return err == nil
}

// The driver refers to the pinned TLS policy by name, so the registration has
// to be there while connections are opened: from before the first one until
// the database is closed.
func TestTLSRegistrationOutlivesTheConnections(t *testing.T) {
	trusted := newTestCA(t, "disposable-test-ca")
	address := acceptingServer(t, trusted)
	targets := []allowedTarget{{Host: "127.0.0.1", Port: address.(*net.TCPAddr).Port, User: "worker"}}
	policy, err := loadTargetPolicy(writePolicy(t, approvedPolicy(t, targets, trusted)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	database, err := openTestDatabase(ctx, policy, "worker:synthetic-secret@tcp("+address.String()+")/")
	if err != nil {
		t.Fatalf("could not open a database over TLS: %v", err)
	}
	if !driverKnowsTLS(database.name) {
		t.Fatal("the pinned TLS policy is not registered while the database is open")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if driverKnowsTLS(database.name) {
		t.Fatal("the pinned TLS policy is still registered after the database was closed")
	}
}

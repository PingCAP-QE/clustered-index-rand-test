package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func writeTargetPolicy(t *testing.T, target allowedTarget) string {
	t.Helper()
	return writePolicy(t, targetPolicy{Targets: []allowedTarget{target}})
}

func writePolicy(t *testing.T, policy targetPolicy) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "targets.json")
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// testCA signs server certificates for the loopback address.
type testCA struct {
	key *ecdsa.PrivateKey
	der []byte
	pem []byte
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{key: key, der: der, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// serverCertificate returns a leaf certificate for 127.0.0.1 signed by the CA.
func (ca *testCA) serverCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := x509.ParseCertificate(ca.der)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, parent, key.Public(), ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// writeCAFile writes content to ca.pem with exactly the requested mode; the
// process umask would otherwise take bits away from os.WriteFile.
func writeCAFile(t *testing.T, content []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

// handshakeTo dials a TLS server on the loopback address whose certificate is
// signed by ca, and returns the handshake error.
func handshakeTo(t *testing.T, ca *testCA, config *tls.Config) error {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{ca.serverCertificate(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		if conn, err := listener.Accept(); err == nil {
			_ = conn.(*tls.Conn).Handshake()
			conn.Close()
		}
	}()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", listener.Addr().String(), config)
	if conn != nil {
		conn.Close()
	}
	<-served
	return err
}

func TestExactTargetApprovalAndSafeDSNRequired(t *testing.T) {
	path := writeTargetPolicy(t, allowedTarget{Host: "test.example", Port: 4000, User: "worker"})
	policy, err := loadTargetPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := policy.authorize("worker:synthetic-secret@tcp(test.example:4000)/"); err != nil {
		t.Fatal(err)
	}
	for _, dsn := range []string{
		"worker:synthetic-secret@tcp(prod.example:4000)/",
		"worker:synthetic-secret@tcp(test.example:3306)/",
		"root:synthetic-secret@tcp(test.example:4000)/",
		"worker:synthetic-secret@tcp(test.example:4000)/production",
		"worker:synthetic-secret@tcp(test.example:4000)/?tls=false",
		"worker:synthetic-secret@tcp(test.example:4000)/?tls=skip-verify",
		"worker:synthetic-secret@tcp(test.example:4000)/?tls=preferred",
		"worker:synthetic-secret@tcp(test.example:4000)/?multiStatements=true",
		"worker:synthetic-secret@tcp(test.example:4000)/?tidb_general_log=1",
		"worker:synthetic-secret@tcp(test.example:4000)/?allowAllFiles=true",
		"worker:synthetic-secret@tcp(test.example:4000)/?allowCleartextPasswords=true",
		"worker:@tcp(test.example:4000)/",
	} {
		if _, err := policy.authorize(dsn); err == nil || strings.Contains(err.Error(), "synthetic-secret") {
			t.Fatalf("unsafe DSN authorization")
		}
	}
	if _, err := loadTargetPolicy("targets.json"); err == nil {
		t.Fatal("accepted relative policy")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTargetPolicy(path); err == nil {
		t.Fatal("accepted shared policy")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTargetPolicy(link); err == nil {
		t.Fatal("accepted symlink policy")
	}
}

func TestBadSecondTargetPreventsFirstConnection(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	policyPath := writeTargetPolicy(t, allowedTarget{Host: "127.0.0.1", Port: port, User: "worker"})
	cmd := rootCmd()
	cmd.SetArgs([]string{"abtest", "--target-policy", policyPath, "--dsn1", "worker:synthetic-secret@tcp(" + listener.Addr().String() + ")/", "--dsn2", "worker:synthetic-secret@tcp(prod.example:4000)/"})
	if err := cmd.ExecuteContext(context.Background()); err == nil {
		t.Fatal("accepted unapproved second target")
	}
	listener.SetDeadline(time.Now().Add(20 * time.Millisecond))
	if conn, err := listener.Accept(); err == nil {
		conn.Close()
		t.Fatal("connected to first target before approving second")
	}
}

func TestFreshSchemaNamesAndExportNeverDropExistingData(t *testing.T) {
	first, err := newTestDatabaseName()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newTestDatabaseName()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !regexp.MustCompile(`^sqlgen_test_[a-f0-9]{32}$`).MatchString(first) {
		t.Fatal("unsafe schema namespace")
	}
	path := filepath.Join(t.TempDir(), "reproduce.sql")
	writer := newFileWriter(path, first)
	if writer.file == nil {
		t.Fatal("missing SQL output")
	}
	writer.file.Close()
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(text)), "drop database") || strings.Contains(string(text), "test_syntax") {
		t.Fatal("export resets existing database")
	}
}

func TestTrustAnchorPinIsOptionalButValidatedWhenSet(t *testing.T) {
	trusted := newTestCA(t, "disposable-test-ca")
	caFile := writeCAFile(t, trusted.pem, 0644)
	digest := sha256Hex(trusted.pem)
	targets := []allowedTarget{{Host: "test.example", Port: 4000, User: "worker"}}
	for name, policy := range map[string]targetPolicy{
		"truncated pin":    {Targets: targets, CAFile: caFile, CASHA256: digest[:63]},
		"non-hex pin":      {Targets: targets, CAFile: caFile, CASHA256: strings.Repeat("g", 64)},
		"pin without CA":   {Targets: targets, CASHA256: digest},
		"relative CA path": {Targets: targets, CAFile: "ca.pem", CASHA256: digest},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadTargetPolicy(writePolicy(t, policy)); err == nil {
				t.Fatal("accepted a policy with an invalid trust anchor pin")
			}
		})
	}
	for name, policy := range map[string]targetPolicy{
		"system roots": {Targets: targets},
		// The pin is optional: the file still has to pass the ownership and
		// permission checks.
		"unpinned CA": {Targets: targets, CAFile: caFile},
		"pinned CA":   {Targets: targets, CAFile: caFile, CASHA256: strings.ToUpper(digest)},
	} {
		t.Run(name, func(t *testing.T) {
			loaded, err := loadTargetPolicy(writePolicy(t, policy))
			if err != nil {
				t.Fatal(err)
			}
			if loaded.CAFile != policy.CAFile || loaded.CASHA256 != strings.ToLower(policy.CASHA256) {
				t.Fatalf("policy = %+v, want the trust anchor and its pin preserved", loaded)
			}
		})
	}
}

func TestTrustAnchorRejectsSubstitutedAndUnprotectedFiles(t *testing.T) {
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	junk := []byte("not a certificate at all")
	// A valid bundle that is only too large: PEM ignores text outside blocks.
	oversized := append(bytes.Repeat([]byte("#"), maxCAFile), trusted.pem...)
	for _, tc := range []struct {
		name    string
		content []byte
		pinned  []byte
		mode    os.FileMode
		reason  string
	}{
		// The rogue-CA case: the replacement is every bit as protected as
		// the pinned file, so only the pin can tell them apart.
		{"rogue CA substituted for the pinned one", rogue.pem, trusted.pem, 0644, "tls_ca_sha256"},
		{"group-writable CA", trusted.pem, trusted.pem, 0664, "not writable by group or others"},
		{"world-writable CA", trusted.pem, trusted.pem, 0666, "not writable by group or others"},
		{"oversized CA", oversized, oversized, 0644, "at most 1 MiB"},
		{"file that is not PEM", junk, junk, 0644, "invalid target CA file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := &targetPolicy{CAFile: writeCAFile(t, tc.content, tc.mode), CASHA256: sha256Hex(tc.pinned)}
			_, err := policy.tlsConfig("test.example")
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("trust anchor = %v, want it rejected with %q", err, tc.reason)
			}
		})
	}
	directory := &targetPolicy{CAFile: t.TempDir(), CASHA256: sha256Hex(nil)}
	if _, err := directory.tlsConfig("test.example"); err == nil {
		t.Fatal("accepted a directory as the trust anchor")
	}
}

func TestSymlinkedTrustAnchorIsCheckedAtTheFileItLeadsTo(t *testing.T) {
	trusted := newTestCA(t, "disposable-test-ca")
	dir := t.TempDir()
	// The layout of a secret volume: the name is a link into a versioned directory.
	versioned := filepath.Join(dir, "..2026_11_25")
	if err := os.Mkdir(versioned, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(versioned, "ca.pem"), trusted.pem, 0644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "ca.pem")
	if err := os.Symlink(filepath.Join("..2026_11_25", "ca.pem"), link); err != nil {
		t.Fatal(err)
	}
	policy := &targetPolicy{CAFile: link, CASHA256: sha256Hex(trusted.pem)}
	config, err := policy.tlsConfig("127.0.0.1")
	if err != nil {
		t.Fatalf("a secret-volume trust anchor was rejected: %v", err)
	}
	if err := handshakeTo(t, trusted, config); err != nil {
		t.Fatalf("rejected a server signed by the pinned CA: %v", err)
	}

	// The link is then repointed at a file other accounts can rewrite.
	if err := os.WriteFile(filepath.Join(versioned, "weak.pem"), trusted.pem, 0666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(versioned, "weak.pem"), 0666); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..2026_11_25", "weak.pem"), link); err != nil {
		t.Fatal(err)
	}
	if _, err := policy.tlsConfig("127.0.0.1"); err == nil {
		t.Fatal("accepted a symlink to a world-writable trust anchor")
	}
}

func TestTrustAnchorDecidesWhichServersTheConnectionTrusts(t *testing.T) {
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	policy := &targetPolicy{CAFile: writeCAFile(t, trusted.pem, 0644), CASHA256: sha256Hex(trusted.pem)}
	config, err := policy.tlsConfig("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := handshakeTo(t, trusted, config); err != nil {
		t.Fatalf("rejected a server signed by the pinned CA: %v", err)
	}
	if err := handshakeTo(t, rogue, config); err == nil {
		t.Fatal("trusted a server signed by a CA outside the pinned trust anchor")
	}
}

func TestSubstitutedTrustAnchorPreventsAnyConnection(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// The target answers nothing, and the driver would wait for its greeting
	// forever: every attempt is recorded and closed at once instead.
	attempts := make(chan struct{}, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			attempts <- struct{}{}
			conn.Close()
		}
	}()
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	caFile := writeCAFile(t, trusted.pem, 0644)
	policy, err := loadTargetPolicy(writePolicy(t, targetPolicy{
		Targets:  []allowedTarget{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, User: "worker"}},
		CAFile:   caFile,
		CASHA256: sha256Hex(trusted.pem),
	}))
	if err != nil {
		t.Fatal(err)
	}

	// The trust anchor is replaced after the target owner approved it. The
	// replacement keeps the same ownership and permissions.
	if err := os.WriteFile(caFile, rogue.pem, 0644); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(caFile); err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("the scenario needs a protected replacement, got %v, %v", info, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dsn := "worker:synthetic-secret@tcp(" + listener.Addr().String() + ")/"
	if _, err := openTestDatabase(ctx, policy, dsn); err == nil {
		t.Fatal("connected through a substituted trust anchor")
	}
	select {
	case <-attempts:
		t.Fatal("reached the target before validating the trust anchor")
	case <-time.After(20 * time.Millisecond):
	}
}

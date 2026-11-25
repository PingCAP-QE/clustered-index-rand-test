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
	"encoding/asn1"
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
	return writePolicy(t, approvedPolicy(t, []allowedTarget{target}, newTestCA(t, "disposable-test-ca")))
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

// testCA signs server certificates and revocation lists for the tests.
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

// leafSerial is the serial number testCA gives every server certificate, so a
// test can revoke it.
const leafSerial = 2

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
		SerialNumber: big.NewInt(leafSerial),
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

// crlPEM builds a certificate revocation list signed by ca that lists the
// given serial numbers. It is assembled by hand: the module predates
// x509.CreateRevocationList.
func crlPEM(t *testing.T, ca *testCA, nextUpdate time.Time, revoked ...*big.Int) []byte {
	t.Helper()
	parent, err := x509.ParseCertificate(ca.der)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Minute).Truncate(time.Second).UTC()
	list := pkix.TBSCertificateList{
		Version:    1,
		Signature:  pkix.AlgorithmIdentifier{Algorithm: asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}}, // ecdsa-with-SHA256
		Issuer:     parent.Subject.ToRDNSequence(),
		ThisUpdate: now,
		NextUpdate: nextUpdate.Truncate(time.Second).UTC(),
	}
	for _, serial := range revoked {
		list.RevokedCertificates = append(list.RevokedCertificates, pkix.RevokedCertificate{SerialNumber: serial, RevocationTime: now})
	}
	der, err := asn1.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(der)
	r, s, err := ecdsa.Sign(rand.Reader, ca.key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	signature, err := asn1.Marshal(struct{ R, S *big.Int }{r, s})
	if err != nil {
		t.Fatal(err)
	}
	crl, err := asn1.Marshal(struct {
		List      asn1.RawValue
		Algorithm pkix.AlgorithmIdentifier
		Signature asn1.BitString
	}{asn1.RawValue{FullBytes: der}, list.Signature, asn1.BitString{Bytes: signature, BitLength: len(signature) * 8}})
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crl})
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// writeTLSFile writes content to name with exactly the requested mode; the
// process umask would otherwise take bits away from os.WriteFile.
func writeTLSFile(t *testing.T, name string, content []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeCAFile(t *testing.T, content []byte, mode os.FileMode) string {
	t.Helper()
	return writeTLSFile(t, "ca.pem", content, mode)
}

// approvedPolicy is a complete policy: the targets, the trust anchor issued
// by ca, its pin, and a current revocation list signed by the same CA.
func approvedPolicy(t *testing.T, targets []allowedTarget, ca *testCA) targetPolicy {
	t.Helper()
	return targetPolicy{
		Targets:  targets,
		CAFile:   writeCAFile(t, ca.pem, 0644),
		CASHA256: sha256Hex(ca.pem),
		CRLFile:  writeTLSFile(t, "ca.crl", crlPEM(t, ca, time.Now().Add(time.Hour)), 0644),
	}
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

// silentTarget is a TCP listener that answers nothing. The MySQL driver would
// wait for its greeting forever, so every attempt is recorded and closed at
// once instead.
type silentTarget struct {
	listener net.Listener
	attempts chan struct{}
}

func newSilentTarget(t *testing.T) *silentTarget {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	target := &silentTarget{listener: listener, attempts: make(chan struct{}, 8)}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			target.attempts <- struct{}{}
			conn.Close()
		}
	}()
	return target
}

func (target *silentTarget) reached() bool {
	select {
	case <-target.attempts:
		return true
	case <-time.After(20 * time.Millisecond):
		return false
	}
}

func TestBadSecondTargetPreventsFirstConnection(t *testing.T) {
	target := newSilentTarget(t)
	port := target.listener.Addr().(*net.TCPAddr).Port
	policyPath := writeTargetPolicy(t, allowedTarget{Host: "127.0.0.1", Port: port, User: "worker"})
	t.Setenv("SQLGEN_DSN1", "worker:synthetic-secret@tcp("+target.listener.Addr().String()+")/")
	t.Setenv("SQLGEN_DSN2", "worker:synthetic-secret@tcp(prod.example:4000)/")
	cmd := rootCmd()
	cmd.SetArgs([]string{"abtest", "--target-policy", policyPath})
	if err := cmd.ExecuteContext(context.Background()); err == nil {
		t.Fatal("accepted unapproved second target")
	}
	if target.reached() {
		t.Fatal("connected to first target before approving second")
	}
}

func TestDSNsComeOnlyFromTheEnvironment(t *testing.T) {
	for _, command := range rootCmd().Commands() {
		for _, name := range []string{"dsn", "dsn1", "dsn2"} {
			if command.Flags().Lookup(name) != nil {
				t.Errorf("%s still accepts --%s: credentials would be visible in the command line", command.Name(), name)
			}
		}
	}
	target := newSilentTarget(t)
	port := target.listener.Addr().(*net.TCPAddr).Port
	policyPath := writeTargetPolicy(t, allowedTarget{Host: "127.0.0.1", Port: port, User: "worker"})
	t.Setenv("SQLGEN_DSN", "")
	t.Setenv("SQLGEN_DSN1", "")
	t.Setenv("SQLGEN_DSN2", "")
	for name, args := range map[string][]string{
		"abtest without DSNs":        {"abtest", "--target-policy", policyPath},
		"check-syntax without a DSN": {"check-syntax", "--target-policy", policyPath},
		"DSN passed as an argument":  {"check-syntax", "--target-policy", policyPath, "--dsn", "worker:synthetic-secret@tcp(" + target.listener.Addr().String() + ")/"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := rootCmd()
			cmd.SetArgs(args)
			if err := cmd.ExecuteContext(context.Background()); err == nil {
				t.Fatal("ran without the DSNs in the environment")
			}
		})
	}
	if target.reached() {
		t.Fatal("reached the target without an approved DSN")
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

func TestTargetPolicyRequiresPinnedTLSMaterial(t *testing.T) {
	trusted := newTestCA(t, "disposable-test-ca")
	digest := sha256Hex(trusted.pem)
	complete := approvedPolicy(t, []allowedTarget{{Host: "test.example", Port: 4000, User: "worker"}}, trusted)
	for name, drop := range map[string]func(*targetPolicy){
		// No trust anchor, and so no fallback to the system roots.
		"no trust anchor": func(policy *targetPolicy) { policy.CAFile = "" },
		"no pin":          func(policy *targetPolicy) { policy.CASHA256 = "" },
		"truncated pin":   func(policy *targetPolicy) { policy.CASHA256 = digest[:63] },
		"non-hex pin":     func(policy *targetPolicy) { policy.CASHA256 = strings.Repeat("g", 64) },
		// No skipping the revocation check either.
		"no revocation lists": func(policy *targetPolicy) { policy.CRLFile = "" },
		"relative CA path":    func(policy *targetPolicy) { policy.CAFile = "ca.pem" },
		"relative CRL path":   func(policy *targetPolicy) { policy.CRLFile = "ca.crl" },
	} {
		t.Run(name, func(t *testing.T) {
			policy := complete
			drop(&policy)
			if _, err := loadTargetPolicy(writePolicy(t, policy)); err == nil {
				t.Fatal("accepted an incomplete policy")
			}
		})
	}
	// The complete policy loads, with the pin normalized.
	complete.CASHA256 = strings.ToUpper(digest)
	loaded, err := loadTargetPolicy(writePolicy(t, complete))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CAFile != complete.CAFile || loaded.CRLFile != complete.CRLFile || loaded.CASHA256 != digest {
		t.Fatalf("policy = %+v, want the trust anchor, its pin and the revocation lists preserved", loaded)
	}
}

func TestTrustAnchorRejectsSubstitutedAndUnprotectedFiles(t *testing.T) {
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	junk := []byte("not a certificate at all")
	// A valid bundle that is only too large: PEM ignores text outside blocks.
	oversized := append(bytes.Repeat([]byte("#"), maxTLSFile), trusted.pem...)
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
			policy := approvedPolicy(t, nil, trusted)
			policy.CAFile, policy.CASHA256 = writeCAFile(t, tc.content, tc.mode), sha256Hex(tc.pinned)
			_, err := policy.tlsConfig("test.example")
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("trust anchor = %v, want it rejected with %q", err, tc.reason)
			}
		})
	}
	policy := approvedPolicy(t, nil, trusted)
	policy.CAFile = t.TempDir()
	if _, err := policy.tlsConfig("test.example"); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("directory as the trust anchor = %v, want it refused", err)
	}
}

func TestRevocationListsAreValidatedLikeTheTrustAnchor(t *testing.T) {
	trusted := newTestCA(t, "disposable-test-ca")
	current := crlPEM(t, trusted, time.Now().Add(time.Hour))
	// PEM blocks may be concatenated, so nothing else may sit in the file.
	withCertificate := append(append([]byte{}, current...), trusted.pem...)
	for _, tc := range []struct {
		name    string
		content []byte
		mode    os.FileMode
		reason  string
	}{
		{"world-writable CRL", current, 0666, "not writable by group or others"},
		{"file that is not a CRL", []byte("not a revocation list at all"), 0644, "invalid target CRL file"},
		{"certificate inside the CRL file", withCertificate, 0644, "only revocation lists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := approvedPolicy(t, nil, trusted)
			policy.CRLFile = writeTLSFile(t, "ca.crl", tc.content, tc.mode)
			_, err := policy.tlsConfig("test.example")
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("revocation lists = %v, want them rejected with %q", err, tc.reason)
			}
		})
	}
	policy := approvedPolicy(t, nil, trusted)
	policy.CRLFile = t.TempDir()
	if _, err := policy.tlsConfig("test.example"); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("directory as the revocation lists = %v, want it refused", err)
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
	policy := approvedPolicy(t, nil, trusted)
	policy.CAFile = link
	config, err := policy.tlsConfig("127.0.0.1")
	if err != nil {
		t.Fatalf("a secret-volume trust anchor was rejected: %v", err)
	}
	if err := handshakeTo(t, trusted, config); err != nil {
		t.Fatalf("rejected a server signed by the pinned CA: %v", err)
	}

	// The link is then repointed at a file other accounts can rewrite.
	weak := filepath.Join(versioned, "weak.pem")
	if err := os.WriteFile(weak, trusted.pem, 0666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(weak, 0666); err != nil {
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
	policy := approvedPolicy(t, nil, trusted)
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

func TestServerCertificateMustPassThePinnedRevocationLists(t *testing.T) {
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	configFor := func(t *testing.T, crl []byte) *tls.Config {
		t.Helper()
		policy := approvedPolicy(t, nil, trusted)
		policy.CRLFile = writeTLSFile(t, "ca.crl", crl, 0644)
		config, err := policy.tlsConfig("127.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		return config
	}
	if err := handshakeTo(t, trusted, configFor(t, crlPEM(t, trusted, time.Now().Add(time.Hour)))); err != nil {
		t.Fatalf("rejected a server covered by a current revocation list: %v", err)
	}
	for name, tc := range map[string]struct {
		crl    []byte
		reason string
	}{
		"revoked server certificate": {crlPEM(t, trusted, time.Now().Add(time.Hour), big.NewInt(leafSerial)), "has been revoked"},
		"expired revocation list":    {crlPEM(t, trusted, time.Now().Add(-time.Minute)), "has expired"},
		"list from another CA":       {crlPEM(t, rogue, time.Now().Add(time.Hour)), "no revocation list covers"},
	} {
		t.Run(name, func(t *testing.T) {
			err := handshakeTo(t, trusted, configFor(t, tc.crl))
			if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("handshake = %v, want it refused with %q", err, tc.reason)
			}
		})
	}
}

func TestSubstitutedTrustAnchorPreventsAnyConnection(t *testing.T) {
	target := newSilentTarget(t)
	trusted, rogue := newTestCA(t, "disposable-test-ca"), newTestCA(t, "rogue-ca")
	policy, err := loadTargetPolicy(writePolicy(t, approvedPolicy(t,
		[]allowedTarget{{Host: "127.0.0.1", Port: target.listener.Addr().(*net.TCPAddr).Port, User: "worker"}}, trusted)))
	if err != nil {
		t.Fatal(err)
	}
	caFile := policy.CAFile

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
	dsn := "worker:synthetic-secret@tcp(" + target.listener.Addr().String() + ")/"
	if _, err := openTestDatabase(ctx, policy, dsn); err == nil {
		t.Fatal("connected through a substituted trust anchor")
	}
	if target.reached() {
		t.Fatal("reached the target before validating the trust anchor")
	}
}

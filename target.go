package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
)

type allowedTarget struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	User string `json:"user"`
}

// targetPolicy is the target owner's approval. Its TLS fields are all
// required, so no connection falls back to system roots or skips revocation.
type targetPolicy struct {
	Targets []allowedTarget `json:"targets"`
	CAFile  string          `json:"tls_ca"`
	// CASHA256 is the hex SHA-256 of the CAFile bytes. The target owner pins
	// it here, so a substituted trust anchor is rejected even when the
	// replacement has safe ownership and permissions.
	CASHA256 string `json:"tls_ca_sha256"`
	// CRLFile holds the PEM certificate revocation lists issued by the CA.
	CRLFile string `json:"tls_crl"`
}

// maxTLSFile bounds the trust anchor and the revocation lists so a hostile or
// misconfigured file cannot exhaust memory.
const maxTLSFile = 1 << 20 // 1 MiB

func loadTargetPolicy(path string) (*targetPolicy, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("--target-policy requires an absolute path to an owner-only approval file")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("cannot open target policy")
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Geteuid()) || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 65536 {
		return nil, fmt.Errorf("target policy must be an owner-only regular file, at most 64 KiB")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 65537))
	decoder.DisallowUnknownFields()
	policy := new(targetPolicy)
	if err := decoder.Decode(policy); err != nil {
		return nil, fmt.Errorf("invalid target policy")
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("invalid target policy")
	}
	if len(policy.Targets) == 0 {
		return nil, fmt.Errorf("target policy has no approved test targets")
	}
	for _, target := range policy.Targets {
		if target.Host == "" || strings.ContainsAny(target.Host, "*/") || target.Port < 1 || target.Port > 65535 || target.User == "" || strings.EqualFold(target.User, "root") {
			return nil, fmt.Errorf("target policy must list exact endpoints and dedicated non-root users")
		}
	}
	for _, field := range []struct{ name, path string }{{"tls_ca", policy.CAFile}, {"tls_crl", policy.CRLFile}} {
		if !filepath.IsAbs(field.path) {
			return nil, fmt.Errorf("target policy must name %s, an absolute path", field.name)
		}
	}
	if !isSHA256(policy.CASHA256) {
		return nil, fmt.Errorf("target policy must pin tls_ca with tls_ca_sha256, the SHA-256 of the PEM file")
	}
	policy.CASHA256 = strings.ToLower(policy.CASHA256)
	return policy, nil
}

func isSHA256(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

// authorize runs before opening a pool or sending any SQL. A caller may not
// select an existing database, weaken TLS, or execute connection-time SQL.
func (policy *targetPolicy) authorize(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid database DSN")
	}
	if cfg.Net != "tcp" || cfg.DBName != "" || cfg.User == "" || strings.EqualFold(cfg.User, "root") || cfg.Passwd == "" || len(cfg.Params) > 0 || cfg.MultiStatements || cfg.AllowAllFiles || cfg.AllowOldPasswords || cfg.AllowCleartextPasswords || (cfg.TLSConfig != "" && cfg.TLSConfig != "true") {
		return nil, fmt.Errorf("DSN must use TCP, a dedicated authenticated user, no selected database or session parameters, and verified TLS")
	}
	host, portText, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("DSN requires an explicit host and port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, fmt.Errorf("invalid target port")
	}
	for _, target := range policy.Targets {
		if strings.EqualFold(strings.TrimSuffix(host, "."), strings.TrimSuffix(target.Host, ".")) && port == target.Port && cfg.User == target.User {
			return cfg, nil
		}
	}
	return nil, fmt.Errorf("database target is not authorized by --target-policy")
}

// tlsConfig is the TLS policy of every connection: the pinned trust anchor,
// chain and hostname verification, and a revocation check of the server
// certificate against the pinned revocation lists.
func (policy *targetPolicy) tlsConfig(host string) (*tls.Config, error) {
	data, err := readTLSFile(policy.CAFile, "CA")
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(digest[:])), []byte(policy.CASHA256)) != 1 {
		return nil, fmt.Errorf("target CA file does not match tls_ca_sha256 pinned by the target policy")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("invalid target CA file")
	}
	revocations, err := readRevocationLists(policy.CRLFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12, ServerName: host, RootCAs: pool,
		// Go verifies the chain first; the server certificate must then
		// also pass the revocation lists the target owner pinned.
		VerifyPeerCertificate: revocations.verifyLeaf,
	}, nil
}

// readTLSFile loads a file every database connection depends on: the trust
// anchor or the revocation lists. It must be a regular file of at most 1 MiB,
// owned by root or the process user, and not writable by group or others, so
// no other account can replace its content. It is read through the descriptor
// that was checked, so the validated bytes are exactly the used ones. Symlinks
// are followed on purpose, as secret volumes mount them: the opened file is
// what is checked, not the path.
func readTLSFile(path, kind string) ([]byte, error) {
	// O_NONBLOCK keeps a FIFO planted at the path from blocking the open.
	descriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("cannot open target %s file", kind)
	}
	file := os.NewFile(uintptr(descriptor), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot inspect target %s file", kind)
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("cannot inspect target %s file", kind)
	}
	if !info.Mode().IsRegular() || info.Size() > maxTLSFile {
		return nil, fmt.Errorf("target %s must be a regular file, at most 1 MiB", kind)
	}
	if (owner.Uid != 0 && owner.Uid != uint32(os.Geteuid())) || info.Mode().Perm()&0022 != 0 {
		return nil, fmt.Errorf("target %s must be owned by root or the process user and not writable by group or others", kind)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxTLSFile+1))
	if err != nil || len(data) > maxTLSFile {
		return nil, fmt.Errorf("cannot read target %s file", kind)
	}
	return data, nil
}

// revocationLists are the certificate revocation lists the target owner pinned.
type revocationLists []*pkix.CertificateList

func readRevocationLists(path string) (revocationLists, error) {
	data, err := readTLSFile(path, "CRL")
	if err != nil {
		return nil, err
	}
	var lists revocationLists
	for rest := data; len(rest) > 0; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "X509 CRL" {
			return nil, fmt.Errorf("target CRL file must contain only revocation lists")
		}
		list, err := x509.ParseDERCRL(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("invalid target CRL file")
		}
		lists = append(lists, list)
	}
	if len(lists) == 0 {
		return nil, fmt.Errorf("invalid target CRL file")
	}
	return lists, nil
}

// verifyLeaf is the VerifyPeerCertificate callback of every connection. It
// fails closed: the server certificate must be covered by a current list
// signed by its issuer, and must not be listed as revoked.
func (lists revocationLists) verifyLeaf(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if len(rawCerts) == 0 || len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
		return errors.New("target presented no verified certificate")
	}
	chain := verifiedChains[0]
	leaf, issuer := chain[0], chain[0]
	if len(chain) > 1 {
		issuer = chain[1]
	}
	covered, now := false, time.Now()
	for _, list := range lists {
		if issuer.CheckCRLSignature(list) != nil {
			continue
		}
		covered = true
		if list.TBSCertList.NextUpdate.Before(now) {
			return errors.New("the revocation list of the target certificate has expired")
		}
		for _, revoked := range list.TBSCertList.RevokedCertificates {
			if revoked.SerialNumber.Cmp(leaf.SerialNumber) == 0 {
				return errors.New("the target certificate has been revoked")
			}
		}
	}
	if !covered {
		return errors.New("no revocation list covers the issuer of the target certificate")
	}
	return nil
}

func newTestDatabaseName() (string, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return "sqlgen_test_" + hex.EncodeToString(token), nil
}

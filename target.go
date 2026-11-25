package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-sql-driver/mysql"
)

type allowedTarget struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	User string `json:"user"`
}

type targetPolicy struct {
	Targets []allowedTarget `json:"targets"`
	CAFile  string          `json:"tls_ca,omitempty"`
	// CASHA256 is the hex SHA-256 of the CAFile bytes. The target owner may pin
	// it here, so a substituted trust anchor is rejected even when the
	// replacement has safe ownership and permissions.
	CASHA256 string `json:"tls_ca_sha256,omitempty"`
}

// maxCAFile bounds the trust anchor so a hostile or misconfigured file
// cannot exhaust memory.
const maxCAFile = 1 << 20 // 1 MiB

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
	if policy.CAFile != "" && !filepath.IsAbs(policy.CAFile) {
		return nil, fmt.Errorf("TLS CA path must be absolute")
	}
	if policy.CASHA256 != "" {
		if policy.CAFile == "" {
			return nil, fmt.Errorf("tls_ca_sha256 is set but tls_ca is not")
		}
		if !isSHA256(policy.CASHA256) {
			return nil, fmt.Errorf("tls_ca_sha256 must be the SHA-256 of the PEM file")
		}
		policy.CASHA256 = strings.ToLower(policy.CASHA256)
	}
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

func (policy *targetPolicy) tlsConfig(host string) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	if policy.CAFile != "" {
		data, err := readTrustAnchor(policy.CAFile, policy.CASHA256)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("invalid target CA file")
		}
		config.RootCAs = pool
	}
	return config, nil
}

// readTrustAnchor loads the CA bundle that every database connection trusts
// with its credentials. The file must be a regular file owned by root or the
// process user and not writable by group or others, so no other account can
// plant a rogue CA. When a digest is pinned in the policy, the bytes must match
// it as well. The file is read through the descriptor that was checked, so
// the validated bytes are exactly the trusted ones. Symlinks are followed on
// purpose, as secret volumes mount them: the opened file is what is checked,
// not the path.
func readTrustAnchor(path, pinnedSHA256 string) ([]byte, error) {
	// O_NONBLOCK keeps a FIFO planted at the path from blocking the open.
	descriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("cannot open target CA file")
	}
	file := os.NewFile(uintptr(descriptor), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot inspect target CA file")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("cannot inspect target CA file")
	}
	if !info.Mode().IsRegular() || info.Size() > maxCAFile {
		return nil, fmt.Errorf("target CA must be a regular file, at most 1 MiB")
	}
	if (owner.Uid != 0 && owner.Uid != uint32(os.Geteuid())) || info.Mode().Perm()&0022 != 0 {
		return nil, fmt.Errorf("target CA must be owned by root or the process user and not writable by group or others")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxCAFile+1))
	if err != nil || len(data) > maxCAFile {
		return nil, fmt.Errorf("cannot read target CA file")
	}
	if pinnedSHA256 == "" {
		return data, nil
	}
	digest := sha256.Sum256(data)
	if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(digest[:])), []byte(pinnedSHA256)) != 1 {
		return nil, fmt.Errorf("target CA file does not match tls_ca_sha256 pinned by the target policy")
	}
	return data, nil
}

func newTestDatabaseName() (string, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return "sqlgen_test_" + hex.EncodeToString(token), nil
}

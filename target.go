package main

import (
	"crypto/rand"
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
}

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
	return policy, nil
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
		data, err := os.ReadFile(policy.CAFile)
		if err != nil {
			return nil, fmt.Errorf("cannot read target CA file")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("invalid target CA file")
		}
		config.RootCAs = pool
	}
	return config, nil
}

func newTestDatabaseName() (string, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return "sqlgen_test_" + hex.EncodeToString(token), nil
}

package main

import (
	"context"
	"encoding/json"
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
	path := filepath.Join(t.TempDir(), "targets.json")
	data, err := json.Marshal(targetPolicy{Targets: []allowedTarget{target}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
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

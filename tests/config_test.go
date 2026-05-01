package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epeers/portfolio/config"
	log "github.com/sirupsen/logrus"
)

func TestConfigLoad_WithEnvVars(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origPort := os.Getenv("PORT")
	origJWT := os.Getenv("JWT_SECRET")
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("JWT_SECRET", origJWT)
		if origPort != "" {
			os.Setenv("PORT", origPort)
		} else {
			os.Unsetenv("PORT")
		}
	}()

	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Setenv("JWT_SECRET", "test-secret-32-chars-minimum!!!!!")
	os.Unsetenv("PORT")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.PGURL != "postgres://test:test@localhost/test" {
		t.Errorf("expected PG_URL to be 'postgres://test:test@localhost/test', got %q", cfg.PGURL)
	}
	if cfg.Port != "8080" {
		t.Errorf("expected default PORT to be '8080', got %q", cfg.Port)
	}
}

func TestConfigLoad_MissingPGURL(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origDir, _ := os.Getwd()
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Chdir(origDir)
	}()

	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	os.Unsetenv("PG_URL")
	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for missing PG_URL, got nil")
	}
}

func TestConfigLoad_MissingJWTSecret(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origJWT := os.Getenv("JWT_SECRET")
	origDir, _ := os.Getwd()
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("JWT_SECRET", origJWT)
		os.Chdir(origDir)
	}()

	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Unsetenv("JWT_SECRET")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for missing JWT_SECRET, got nil")
	}
}

func TestConfigLoad_CustomPort(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origPort := os.Getenv("PORT")
	origJWT := os.Getenv("JWT_SECRET")
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("JWT_SECRET", origJWT)
		if origPort != "" {
			os.Setenv("PORT", origPort)
		} else {
			os.Unsetenv("PORT")
		}
	}()

	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Setenv("JWT_SECRET", "test-secret-32-chars-minimum!!!!!")
	os.Setenv("PORT", "3000")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.Port != "3000" {
		t.Errorf("expected PORT to be '3000', got %q", cfg.Port)
	}
}

// logCaptureHook records log entries for test assertions.
type logCaptureHook struct {
	entries []*log.Entry
}

func (h *logCaptureHook) Levels() []log.Level { return log.AllLevels }
func (h *logCaptureHook) Fire(e *log.Entry) error {
	h.entries = append(h.entries, e)
	return nil
}

func (h *logCaptureHook) hasError(substr string) bool {
	for _, e := range h.entries {
		if e.Level == log.ErrorLevel && strings.Contains(e.Message, substr) {
			return true
		}
	}
	return false
}

func TestConfigLoad_MissingEODHDKey(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origKey := os.Getenv("EODHD_KEY")
	origJWT := os.Getenv("JWT_SECRET")
	origDir, _ := os.Getwd()
	hook := &logCaptureHook{}
	log.AddHook(hook)
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("JWT_SECRET", origJWT)
		if origKey != "" {
			os.Setenv("EODHD_KEY", origKey)
		} else {
			os.Unsetenv("EODHD_KEY")
		}
		os.Chdir(origDir)
	}()

	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Setenv("JWT_SECRET", "test-secret-32-chars-minimum!!!!!")
	os.Unsetenv("EODHD_KEY")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error for missing EODHD_KEY, got %v", err)
	}
	if cfg.EODHDKey != "" {
		t.Errorf("expected EODHDKey to be empty, got %q", cfg.EODHDKey)
	}
	if !hook.hasError("EODHD_KEY") {
		t.Error("expected error-level log mentioning EODHD_KEY")
	}
}

func TestConfigLoad_MissingFREDKey(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origKey := os.Getenv("FRED_KEY")
	origJWT := os.Getenv("JWT_SECRET")
	origDir, _ := os.Getwd()
	hook := &logCaptureHook{}
	log.AddHook(hook)
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("JWT_SECRET", origJWT)
		if origKey != "" {
			os.Setenv("FRED_KEY", origKey)
		} else {
			os.Unsetenv("FRED_KEY")
		}
		os.Chdir(origDir)
	}()

	tmpDir := t.TempDir()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Setenv("JWT_SECRET", "test-secret-32-chars-minimum!!!!!")
	os.Unsetenv("FRED_KEY")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error for missing FRED_KEY, got %v", err)
	}
	if cfg.FREDKey != "" {
		t.Errorf("expected FREDKey to be empty, got %q", cfg.FREDKey)
	}
	if !hook.hasError("FRED_KEY") {
		t.Error("expected error-level log mentioning FRED_KEY")
	}
}

func TestConfigLoad_ShellEnvTakesPrecedence(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origJWT := os.Getenv("JWT_SECRET")
	origDir, _ := os.Getwd()
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("JWT_SECRET", origJWT)
		os.Chdir(origDir)
	}()

	tmpDir := t.TempDir()
	envContent := "PG_URL=postgres://dotenv:dotenv@localhost/dotenv\n"
	envPath := filepath.Join(tmpDir, ".env")
	if err := os.WriteFile(envPath, []byte(envContent), 0644); err != nil {
		t.Fatalf("failed to write .env file: %v", err)
	}

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	os.Setenv("PG_URL", "postgres://shell:shell@localhost/shell")
	os.Setenv("JWT_SECRET", "test-secret-32-chars-minimum!!!!!")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.PGURL != "postgres://shell:shell@localhost/shell" {
		t.Errorf("expected shell PG_URL to take precedence, got %q", cfg.PGURL)
	}
}

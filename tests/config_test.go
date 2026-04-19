package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/epeers/portfolio/config"
)

func TestConfigLoad_WithEnvVars(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origPort := os.Getenv("PORT")
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		if origPort != "" {
			os.Setenv("PORT", origPort)
		} else {
			os.Unsetenv("PORT")
		}
	}()

	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
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

func TestConfigLoad_CustomPort(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origPort := os.Getenv("PORT")
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		if origPort != "" {
			os.Setenv("PORT", origPort)
		} else {
			os.Unsetenv("PORT")
		}
	}()

	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Setenv("PORT", "3000")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.Port != "3000" {
		t.Errorf("expected PORT to be '3000', got %q", cfg.Port)
	}
}

func TestConfigLoad_MissingEODHDKey(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origKey := os.Getenv("EODHD_KEY")
	origDir, _ := os.Getwd()
	defer func() {
		os.Setenv("PG_URL", origPGURL)
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
	os.Unsetenv("EODHD_KEY")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error for missing EODHD_KEY, got %v", err)
	}
	if cfg.EODHDKey != "" {
		t.Errorf("expected EODHDKey to be empty, got %q", cfg.EODHDKey)
	}
}

func TestConfigLoad_MissingFREDKey(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origKey := os.Getenv("FRED_KEY")
	origDir, _ := os.Getwd()
	defer func() {
		os.Setenv("PG_URL", origPGURL)
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
	os.Unsetenv("FRED_KEY")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error for missing FRED_KEY, got %v", err)
	}
	if cfg.FREDKey != "" {
		t.Errorf("expected FREDKey to be empty, got %q", cfg.FREDKey)
	}
}

func TestConfigLoad_ShellEnvTakesPrecedence(t *testing.T) {
	origPGURL := os.Getenv("PG_URL")
	origDir, _ := os.Getwd()
	defer func() {
		os.Setenv("PG_URL", origPGURL)
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

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.PGURL != "postgres://shell:shell@localhost/shell" {
		t.Errorf("expected shell PG_URL to take precedence, got %q", cfg.PGURL)
	}
}

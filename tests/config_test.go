package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/epeers/portfolio/config"
)

func TestConfigLoad_WithEnvVars(t *testing.T) {
	// Save original env vars
	origPGURL := os.Getenv("PG_URL")
	origAVKey := os.Getenv("AV_KEY")
	origPort := os.Getenv("PORT")
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("AV_KEY", origAVKey)
		if origPort != "" {
			os.Setenv("PORT", origPort)
		} else {
			os.Unsetenv("PORT")
		}
	}()

	// Set test values
	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Setenv("AV_KEY", "test-api-key")
	os.Unsetenv("PORT")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.PGURL != "postgres://test:test@localhost/test" {
		t.Errorf("expected PG_URL to be 'postgres://test:test@localhost/test', got %q", cfg.PGURL)
	}
	if cfg.AVKey != "test-api-key" {
		t.Errorf("expected AV_KEY to be 'test-api-key', got %q", cfg.AVKey)
	}
	if cfg.Port != "8080" {
		t.Errorf("expected default PORT to be '8080', got %q", cfg.Port)
	}
}

func TestConfigLoad_MissingKeys(t *testing.T) {
	// Save original env vars
	origPGURL := os.Getenv("PG_URL")
	origAVKey := os.Getenv("AV_KEY")
	origDir, _ := os.Getwd()

	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("AV_KEY", origAVKey)
		os.Chdir(origDir)

	}()

	tmpDir := t.TempDir()
	// Change to temp directory so godotenv.Load() finds no .env file
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	// Unset PG_URL
	os.Unsetenv("PG_URL")
	os.Setenv("AV_KEY", "test-api-key")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for missing PG_URL, got nil")
	}

	// Set PG_URL but unset AV_KEY
	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Unsetenv("AV_KEY")

	_, err = config.Load()
	if err == nil {
		t.Fatal("expected error for missing AV_KEY, got nil")
	}

}

/*
	func TestConfigLoad_MissingAVKey(t *testing.T) {
		// Save original env vars
		origPGURL := os.Getenv("PG_URL")
		origAVKey := os.Getenv("AV_KEY")
		defer func() {
			os.Setenv("PG_URL", origPGURL)
			os.Setenv("AV_KEY", origAVKey)
		}()


		_, err := config.Load()
		if err == nil {
			t.Fatal("expected error for missing AV_KEY, got nil")
		}
	}
*/
func TestConfigLoad_CustomPort(t *testing.T) {
	// Save original env vars
	origPGURL := os.Getenv("PG_URL")
	origAVKey := os.Getenv("AV_KEY")
	origPort := os.Getenv("PORT")
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("AV_KEY", origAVKey)
		if origPort != "" {
			os.Setenv("PORT", origPort)
		} else {
			os.Unsetenv("PORT")
		}
	}()

	// Set test values with custom port
	os.Setenv("PG_URL", "postgres://test:test@localhost/test")
	os.Setenv("AV_KEY", "test-api-key")
	os.Setenv("PORT", "3000")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.Port != "3000" {
		t.Errorf("expected PORT to be '3000', got %q", cfg.Port)
	}
}

func TestConfigLoad_ShellEnvTakesPrecedence(t *testing.T) {
	// Save original env vars and working directory
	origPGURL := os.Getenv("PG_URL")
	origAVKey := os.Getenv("AV_KEY")
	origDir, _ := os.Getwd()
	defer func() {
		os.Setenv("PG_URL", origPGURL)
		os.Setenv("AV_KEY", origAVKey)
		os.Chdir(origDir)
	}()

	// Create a temp directory with a .env file
	tmpDir := t.TempDir()
	envContent := `PG_URL=postgres://dotenv:dotenv@localhost/dotenv
AV_KEY=dotenv-api-key
`
	envPath := filepath.Join(tmpDir, ".env")
	if err := os.WriteFile(envPath, []byte(envContent), 0644); err != nil {
		t.Fatalf("failed to write .env file: %v", err)
	}

	// Change to temp directory so godotenv.Load() finds the .env file
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change directory: %v", err)
	}

	// Set shell env vars that should take precedence
	os.Setenv("PG_URL", "postgres://shell:shell@localhost/shell")
	os.Setenv("AV_KEY", "shell-api-key")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Shell values should take precedence over .env values
	if cfg.PGURL != "postgres://shell:shell@localhost/shell" {
		t.Errorf("expected shell PG_URL to take precedence, got %q", cfg.PGURL)
	}
	if cfg.AVKey != "shell-api-key" {
		t.Errorf("expected shell AV_KEY to take precedence, got %q", cfg.AVKey)
	}
}

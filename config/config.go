package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

// Config holds application configuration loaded from environment variables
type Config struct {
	PGURL    string
	AVKey    string
	Port     string
	LogLevel string
}

// Load reads configuration from environment variables.
// If a .env file exists, it will be loaded first, but shell environment
// variables take precedence over .env values.
func Load() (*Config, error) {
	// Load .env file if it exists (does not override existing env vars)
	_ = godotenv.Load()

	pgURL := os.Getenv("PG_URL")
	if pgURL == "" {
		return nil, fmt.Errorf("PG_URL environment variable is required")
	}

	avKey := os.Getenv("AV_KEY")
	if avKey == "" {
		return nil, fmt.Errorf("AV_KEY environment variable is required")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	LogLevel := os.Getenv("LOGLEVEL")
	if LogLevel == "" {
		LogLevel = "Warning"
	}

	return &Config{
		PGURL:    pgURL,
		AVKey:    avKey,
		Port:     port,
		LogLevel: LogLevel,
	}, nil
}

package config

import (
	"fmt"
	"os"
)

// Config holds application configuration loaded from environment variables
type Config struct {
	PGURL  string
	AVKey  string
	Port   string
}

// Load reads configuration from environment variables
func Load() (*Config, error) {
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

	return &Config{
		PGURL:  pgURL,
		AVKey:  avKey,
		Port:   port,
	}, nil
}

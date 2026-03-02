package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

// Config holds application configuration loaded from environment variables
type Config struct {
	PGURL         string
	AVKey         string
	EODHDKey      string
	FDKey         string
	FREDKey       string
	Port          string
	LogLevel      string
	EnableSwagger bool
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
		log.Warn("AV_KEY is not configured — AlphaVantage features (ETF holdings, security sync, treasury rates) will fail gracefully")
	}

	eohdhdKey := os.Getenv("EODHD_KEY")
	if eohdhdKey == "" {
		log.Warn("EODHD_KEY is not configured — EODHD price fetching will fail gracefully")
	}

	fdKey := os.Getenv("FD_KEY")

	fredKey := os.Getenv("FRED_KEY")
	if fredKey == "" {
		log.Warn("FRED_KEY is not configured — treasury rate fetching will fail gracefully")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	LogLevel := os.Getenv("LOGLEVEL")
	if LogLevel == "" {
		LogLevel = "Warning"
	}

	enableSwagger := os.Getenv("ENABLE_SWAGGER") == "true"

	return &Config{
		PGURL:         pgURL,
		AVKey:         avKey,
		EODHDKey:      eohdhdKey,
		FDKey:         fdKey,
		FREDKey:       fredKey,
		Port:          port,
		LogLevel:      LogLevel,
		EnableSwagger: enableSwagger,
	}, nil
}

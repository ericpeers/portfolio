package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

// Config holds application configuration loaded from environment variables
type Config struct {
	PGURL         string
	EODHDKey      string
	FREDKey       string
	Port          string
	LogLevel      string
	EnableSwagger bool
	Concurrency   int
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

	eohdhdKey := os.Getenv("EODHD_KEY")
	if eohdhdKey == "" {
		log.Warn("EODHD_KEY is not configured — EODHD price fetching will fail gracefully")
	}

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

	concurrency := 10
	if v := os.Getenv("CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			concurrency = n
		} else {
			log.Warnf("CONCURRENCY=%q is not a positive integer, using default %d", v, concurrency)
		}
	}

	return &Config{
		PGURL:         pgURL,
		EODHDKey:      eohdhdKey,
		FREDKey:       fredKey,
		Port:          port,
		LogLevel:      LogLevel,
		EnableSwagger: enableSwagger,
		Concurrency:   concurrency,
	}, nil
}

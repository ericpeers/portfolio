package main

import (
	"context"
	_ "embed"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/epeers/portfolio/config"
	_ "github.com/epeers/portfolio/docs"
	"github.com/epeers/portfolio/internal/apperrors"
	"github.com/epeers/portfolio/internal/database"
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

// @title Portfolio API
// @version 1.0
// @description A CRUD server for analyzing financial portfolios comprising stocks, bonds, and ETFs.

// @host localhost:8080
// @BasePath /

// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization

//go:embed LICENSES.TXT
var licensesText []byte

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		// Force colors to be true to ensure color even if TTY detection is flaky (optional, defaults to true for TTY)
		ForceColors: true,
	})
	switch strings.ToUpper(cfg.LogLevel) {
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	case "WARNING":
		log.SetLevel(log.WarnLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	default:
		log.Fatalf("Did not understand logging level request: %s", cfg.LogLevel)
	}

	// Create context for initialization
	ctx := context.Background()

	// Initialize database connection
	db, err := database.New(ctx, cfg.PGURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize API clients
	eohdClient := eodhd.NewClient(cfg.EODHDKey)
	fredClient := fred.NewClient(cfg.FREDKey)

	// Initialize repositories
	portfolioRepo := repository.NewPortfolioRepository(db.Pool)
	securityRepo := repository.NewSecurityRepository(db.Pool)
	priceRepo := repository.NewPriceRepository(db.Pool)
	exchangeRepo := repository.NewExchangeRepository(db.Pool)
	fundamentalsRepo := repository.NewFundamentalsRepository(db.Pool)

	// Initialize services
	//
	// Concurrency layering — tunable via CONCURRENCY env var (default 10).
	//
	// WithConcurrency: caps simultaneous EODHD/FRED provider connections globally.
	// At 16 req/sec with ~300ms average EODHD latency, Little's Law gives ~5 concurrent
	// connections needed to saturate the rate limiter, so 10 is comfortably right-sized.
	//
	// priceConcurrency (2×): caps concurrent GetDailyPrices calls inside ComputeDailyValues.
	// Higher than WithConcurrency intentionally: on warm-cache runs no provider connection
	// is needed, so 2× parallel DB reads outperform 1× with no downside. On cold-cache runs
	// the inner fetchSem becomes the effective ceiling and the extra goroutines wait cheaply.
	//
	// syncWorkers: concurrent EODHD exchange-symbol-list fetches during security sync.
	// Matches WithConcurrency since both are rate-limited by the same EODHD token bucket.
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{
		Price:    eohdClient,
		Event:    eohdClient,
		Treasury: fredClient,
		Bulk:     eohdClient,
	}).WithConcurrency(cfg.Concurrency)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc)
	priceConcurrency := cfg.Concurrency * 2
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, priceConcurrency)
	comparisonSvc := services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc, securityRepo)
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, fundamentalsRepo, eohdClient, cfg.Concurrency)
	glanceRepo := repository.NewGlanceRepository(db.Pool)
	hintsRepo := repository.NewHintsRepository(db.Pool)
	glanceSvc := services.NewGlanceService(glanceRepo, portfolioSvc, performanceSvc)
	prefetchSvc := services.NewPrefetchService(pricingSvc, securityRepo, adminSvc, hintsRepo)

	// Initialize auth dependencies
	userRepo := repository.NewUserRepository(db.Pool)
	authSvc := services.NewAuthService(userRepo, cfg.JWTSecret)
	authHandler := handlers.NewAuthHandler(authSvc)

	// Initialize handlers
	infoHandler := handlers.NewInfoHandler(licensesText)
	portfolioHandler := handlers.NewPortfolioHandler(portfolioSvc)
	userHandler := handlers.NewUserHandler(portfolioSvc)
	compareHandler := handlers.NewCompareHandler(comparisonSvc)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo, exchangeRepo, priceRepo)
	glanceHandler := handlers.NewGlanceHandler(glanceSvc)

	// Setup Gin router
	router := gin.Default()

	// Apply global middleware
	router.Use(middleware.ErrorCounter())
	router.Use(middleware.ValidateUser([]byte(cfg.JWTSecret)))

	// Public informational endpoints (no auth required)
	router.GET("/licenses", infoHandler.GetLicenses)

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":          "ok",
			"error_count":     apperrors.Count(),
			"uptime_seconds":  apperrors.UptimeSeconds(),
		})
	})

	// Swagger documentation endpoint (disabled by default in production)
	if cfg.EnableSwagger {
		router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
		log.Info("Swagger UI enabled at /swagger/index.html")
	}

	// Auth routes (no auth required)
	authGroup := router.Group("/auth")
	authGroup.POST("/register", authHandler.Register)
	authGroup.POST("/login", authHandler.Login)
	authGroup.GET("/me", middleware.RequireAuth(), authHandler.Me)

	// Portfolio routes
	router.POST("/portfolios", middleware.RequireAuth(), portfolioHandler.Create)
	router.GET("/portfolios/:id", middleware.RequireAuth(), portfolioHandler.Get)
	router.PUT("/portfolios/:id", middleware.RequireAuth(), portfolioHandler.Update)
	router.DELETE("/portfolios/:id", middleware.RequireAuth(), portfolioHandler.Delete)
	router.POST("/portfolios/compare", middleware.WarmingMiddleware(prefetchSvc.WarmingDone()), middleware.RequireAuth(), compareHandler.Compare)

	// User routes
	router.GET("/users/:user_id/portfolios", middleware.RequireAuth(), userHandler.ListPortfolios)

	// Glance routes
	router.POST("/users/:user_id/glance", middleware.RequireAuth(), glanceHandler.Add)
	router.DELETE("/users/:user_id/glance/:portfolio_id", middleware.RequireAuth(), glanceHandler.Remove)
	router.GET("/users/:user_id/glance", middleware.WarmingMiddleware(prefetchSvc.WarmingDone()), middleware.RequireAuth(), glanceHandler.List)

	// Admin routes — all endpoints require authentication and ADMIN role
	admin := router.Group("/admin")
	admin.Use(middleware.RequireAuth(), middleware.RequireAdmin())
	{
		admin.GET("/get_daily_prices", adminHandler.GetDailyPrices)
		admin.GET("/get_etf_holdings", adminHandler.GetETFHoldings)
		admin.GET("/bulk-fetch-eodhd-prices", adminHandler.BulkFetchEODHDPrices)
		admin.POST("/load_etf_holdings", adminHandler.LoadETFHoldings)
		admin.GET("/export-prices", adminHandler.ExportPrices)
		admin.POST("/import-prices", adminHandler.ImportPrices)

		securities := admin.Group("/securities")
		{
			securities.POST("/sync-from-provider", adminHandler.SyncSecuritiesFromProvider)
			securities.POST("/load_csv", adminHandler.LoadSecurities)
			securities.POST("/load_ipo_csv", adminHandler.LoadSecuritiesIPO)

			securities.POST("/backfill_fundamentals", adminHandler.BackfillFundamentals)

			fundamentals := securities.Group("/fundamentals")
			{
				fundamentals.POST("/import_json", adminHandler.ImportFundamentalsJSON)
			}
		}

		admin.GET("/users/pending", authHandler.ListPending)
		admin.PATCH("/users/:id/approve", authHandler.Approve)
	}

	// Start background price prefetch goroutine.
	// prefetchCtx is cancelled on shutdown so the goroutine exits cleanly.
	prefetchCtx, prefetchCancel := context.WithCancel(context.Background())
	prefetchSvc.Start(prefetchCtx)

	// Create HTTP server
	srv := &http.Server{
		Addr:           ":" + cfg.Port,
		Handler:        router,
		ReadHeaderTimeout: 5 * time.Second, // mitigate Slowloris attacks
	}

	// Start server in goroutine
	go func() {
		log.Infof("Starting server on port %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("Shutting down server...")
	prefetchCancel() // stop background prefetch goroutines

	// Give outstanding requests 5 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Info("Server exited")
}

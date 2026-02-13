package main

import (
	"context"
	"fmt"
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
	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/database"
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

// @title Portfolio API
// @version 1.0
// @description A CRUD server for analyzing financial portfolios comprising stocks, bonds, and ETFs.

// @host localhost:8080
// @BasePath /

// @securityDefinitions.apikey UserID
// @in header
// @name X-User-ID

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

	// Initialize AlphaVantage client
	avClient := alphavantage.NewClient(cfg.AVKey)

	// Initialize repositories
	portfolioRepo := repository.NewPortfolioRepository(db.Pool)
	securityRepo := repository.NewSecurityRepository(db.Pool)
	priceCacheRepo := repository.NewPriceCacheRepository(db.Pool)
	exchangeRepo := repository.NewExchangeRepository(db.Pool)
	// Initialize services
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo)
	comparisonSvc := services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc)
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, avClient)

	// Initialize handlers
	portfolioHandler := handlers.NewPortfolioHandler(portfolioSvc)
	userHandler := handlers.NewUserHandler(portfolioSvc)
	compareHandler := handlers.NewCompareHandler(comparisonSvc)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo)

	// Setup Gin router
	router := gin.Default()

	// Apply global middleware
	router.Use(middleware.ValidateUser())

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Swagger documentation endpoint (disabled by default in production)
	if cfg.EnableSwagger {
		router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
		log.Info("Swagger UI enabled at /swagger/index.html")
	}

	// Portfolio routes
	router.POST("/portfolios", portfolioHandler.Create)
	router.GET("/portfolios/:id", portfolioHandler.Get)
	router.PUT("/portfolios/:id", portfolioHandler.Update)
	router.DELETE("/portfolios/:id", portfolioHandler.Delete)
	router.POST("/portfolios/compare", compareHandler.Compare)

	// User routes
	router.GET("/users/:user_id/portfolios", userHandler.ListPortfolios)

	// Admin routes
	admin := router.Group("/admin")
	{
		admin.POST("/sync-securities", adminHandler.SyncSecurities)
		admin.GET("/get_daily_prices", adminHandler.GetDailyPrices)
		admin.GET("/get_etf_holdings", adminHandler.GetETFHoldings)
	}

	// Create HTTP server
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	// Start server in goroutine
	go func() {
		log.Printf("Starting server on port %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Give outstanding requests 5 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	fmt.Println("Server exited")
}

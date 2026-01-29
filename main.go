package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/epeers/portfolio/config"
	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/cache"
	"github.com/epeers/portfolio/internal/database"
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
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

	// Initialize caches
	memCache := cache.NewMemoryCache(5 * time.Minute)

	// Initialize repositories
	portfolioRepo := repository.NewPortfolioRepository(db.Pool)
	securityRepo := repository.NewSecurityRepository(db.Pool)
	priceCacheRepo := repository.NewPriceCacheRepository(db.Pool)

	// Initialize services
	pricingSvc := services.NewPricingService(memCache, priceCacheRepo, securityRepo, avClient)
	portfolioSvc := services.NewPortfolioService(portfolioRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo)
	comparisonSvc := services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc)

	// Initialize handlers
	portfolioHandler := handlers.NewPortfolioHandler(portfolioSvc)
	userHandler := handlers.NewUserHandler(portfolioSvc)
	compareHandler := handlers.NewCompareHandler(comparisonSvc)

	// Setup Gin router
	router := gin.Default()

	// Apply global middleware
	router.Use(middleware.ValidateUser())

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Portfolio routes
	router.POST("/portfolios", portfolioHandler.Create)
	router.GET("/portfolios/:id", portfolioHandler.Get)
	router.PUT("/portfolios/:id", portfolioHandler.Update)
	router.DELETE("/portfolios/:id", portfolioHandler.Delete)
	router.POST("/portfolios/compare", compareHandler.Compare)

	// User routes
	router.GET("/users/:user_id/portfolios", userHandler.ListPortfolios)

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

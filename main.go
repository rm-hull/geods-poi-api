package main

import (
	"database/sql"
	"fmt"
	"geods-poi-api/internal"
	"log"
	"os"

	"github.com/aurowora/compress"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"

	healthcheck "github.com/tavsec/gin-healthcheck"
	"github.com/tavsec/gin-healthcheck/checks"
	hc_config "github.com/tavsec/gin-healthcheck/config"
	cachecontrol "go.eigsys.de/gin-cachecontrol/v2"
)

func main() {
	var err error
	var dbPath string
	var port int

	rootCmd := &cobra.Command{
		Use:   "http",
		Short: "GeoDS-POI API server",
		Run: func(cmd *cobra.Command, args []string) {
			server(dbPath, port)
		},
	}

	rootCmd.Flags().StringVar(&dbPath, "db", "./data/poi_uk.gpkg", "Path to GeoPackage SQLite database")
	rootCmd.Flags().IntVar(&port, "port", 8080, "Port to run HTTP server on")

	if err = rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func server(dbPath string, port int) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("database file does not exist: %s", dbPath)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("error closing database: %v", err)
		}
	}()

	if err = db.Ping(); err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	log.Printf("connected to database: %s\n", dbPath)

	r := gin.New()
	r.Use(
		gin.LoggerWithWriter(gin.DefaultWriter, "/healthz"),
		gin.Recovery(),
		compress.Compress(),
		cachecontrol.New(cachecontrol.CacheAssetsForeverPreset),
		cors.Default(),
	)

	err = healthcheck.New(r, hc_config.DefaultConfig(), []checks.Check{
		checks.SqlCheck{Sql: db},
	})
	if err != nil {
		log.Fatalf("failed to initialize healthcheck: %v", err)
	}

	r.GET("/v1/geods-poi/ref-data", internal.RefData(db))
	r.GET("/v1/geods-poi/search", internal.Search(db))
	r.GET("/v1/geods-poi/marker/shadow", internal.Shadow)
	r.GET("/v1/geods-poi/marker/:category", internal.Marker)

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting HTTP API Server on port %d...", port)
	err = r.Run(addr)
	log.Fatalf("HTTP API Server failed to start on port %d: %v", port, err)
}

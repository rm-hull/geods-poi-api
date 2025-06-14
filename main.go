package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"poi-uk-api/internal"

	"github.com/aurowora/compress"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"

	healthcheck "github.com/tavsec/gin-healthcheck"
	"github.com/tavsec/gin-healthcheck/checks"
	hc_config "github.com/tavsec/gin-healthcheck/config"
)

func main() {
	var err error
	var dbPath string
	var port int

	rootCmd := &cobra.Command{
		Use:   "http",
		Short: "POI UK API server",
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
		cors.Default(),
	)

	err = healthcheck.New(r, hc_config.DefaultConfig(), []checks.Check{
		checks.SqlCheck{Sql: db},
	})
	if err != nil {
		log.Fatalf("failed to initialize healthcheck: %v", err)
	}

	r.GET("/v1/poi/search", internal.Search(db))

	addr := fmt.Sprintf(":%d", port)
	if err := r.Run(addr); err != nil {
		panic(fmt.Sprintf("failed to start server: %v", err))
	}
}

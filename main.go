package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aurowora/compress"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"

	healthcheck "github.com/tavsec/gin-healthcheck"
	"github.com/tavsec/gin-healthcheck/checks"
	hc_config "github.com/tavsec/gin-healthcheck/config"
)

type Response struct {
	Results []POI `json:"results"`
}

type POI struct {
	Fid            int      `json:"fid"`
	Geom           string   `json:"geom"`
	Id             string   `json:"id"`
	PrimaryName    *string  `json:"primary_name,omitempty"`
	Categories     []string `json:"categories,omitempty"`
	Address        *string  `json:"address,omitempty"`
	Locality       *string  `json:"locality,omitempty"`
	Postcode       *string  `json:"postcode,omitempty"`
	Region         *string  `json:"region,omitempty"`
	Country        *string  `json:"country,omitempty"`
	Source         string   `json:"source"`
	SourceRecordId string   `json:"source_record_id"`
	Lat            float64  `json:"lat"`
	Long           float64  `json:"long"`
	H3_15          string   `json:"h3_15"`
	Easting        float64  `json:"easting"`
	Northing       float64  `json:"northing"`
	LSOA21CD       string   `json:"lsoa21cd"`
}

const (
	LEFT = iota
	BOTTOM
	RIGHT
	TOP
)

var db *sql.DB

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
	var err error
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("database file does not exist: %s", dbPath)
	}

	db, err = sql.Open("sqlite3", dbPath)
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
	log.Printf("Connected to database: %s\n", dbPath)

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

	r.GET("/v1/poi/search", search)

	addr := fmt.Sprintf(":%d", port)
	if err := r.Run(addr); err != nil {
		panic(fmt.Sprintf("failed to start server: %v", err))
	}
}

func search(c *gin.Context) {
	bboxStr := c.Query("bbox")
	bbox, err := parseBBox(bboxStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// In bbox: [LEFT, BOTTOM, RIGHT, TOP]
	// So: bbox[LEFT]=min long, bbox[BOTTOM]=min lat, bbox[RIGHT]=max long, bbox[TOP]=max lat
	rows, err := db.Query(`
		SELECT
		  fid, geom, id, primary_name, main_category, alternate_category,
		  address, locality, postcode, region, country, source, source_record_id,
		  lat, long, h3_15, easting, northing, lsoa21cd
		FROM poi_uk
		WHERE lat BETWEEN ? AND ?
		AND long BETWEEN ? AND ?
		`,
		bbox[BOTTOM], bbox[TOP], bbox[LEFT], bbox[RIGHT],
	)
	if err != nil {
		log.Printf("error querying database: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "An internal server error occurred"})
		return
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("error closing rows: %v", err)
		}
	}()

	var results []POI
	var poi POI
	var mainCategory sql.NullString
	var alternateCategory sql.NullString

	for rows.Next() {
		var geomBytes []byte
		if err := rows.Scan(&poi.Fid, &geomBytes, &poi.Id, &poi.PrimaryName, &mainCategory, &alternateCategory,
			&poi.Address, &poi.Locality, &poi.Postcode, &poi.Region, &poi.Country, &poi.Source, &poi.SourceRecordId,
			&poi.Lat, &poi.Long, &poi.H3_15, &poi.Easting, &poi.Northing, &poi.LSOA21CD); err != nil {

			log.Printf("error scanning row: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "An internal server error occurred"})
			return
		}

		poi.Geom, err = wkbPointToWKT(geomBytes)
		if err != nil {
			log.Printf("error converting WKB to WKT: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "An internal server error occurred"})
			return
		}

		poi.Categories = make([]string, 0)
		if mainCategory.Valid {
			poi.Categories = append(poi.Categories, mainCategory.String)
		}
		if alternateCategory.Valid {
			for cat := range strings.SplitSeq(alternateCategory.String, "|") {
				poi.Categories = append(poi.Categories, strings.TrimSpace(cat))
			}
		}

		results = append(results, poi)
	}
	if err = rows.Err(); err != nil {
		log.Printf("error during rows iteration: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "An internal server error occurred"})
		return
	}

	c.JSON(http.StatusOK, Response{Results: results})
}

func parseBBox(bboxStr string) ([]float64, error) {
	bboxParts := strings.Split(bboxStr, ",")
	if len(bboxParts) != 4 {
		return nil, fmt.Errorf("bbox must have 4 comma-separated values")
	}

	bbox := make([]float64, 4)
	for i, part := range bboxParts {
		val, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid bbox value '%s': not a valid float", part)
		}
		bbox[i] = val
	}

	return bbox, nil
}

func wkbPointToWKT(geomBytes []byte) (string, error) {
	if len(geomBytes) < 8 {
		return "", fmt.Errorf("input byte slice is too short to contain a GeoPackage header and WKB data")
	}

	wkbData := geomBytes[8:]

	g, err := wkb.Unmarshal(wkbData)
	if err != nil {
		return "", fmt.Errorf("error unmarshaling WKB: %w", err)
	}

	point, ok := g.(*geom.Point)
	if !ok {
		return "", fmt.Errorf("decoded geometry is not a Point, but a %T", g)
	}

	wktString, err := wkt.Marshal(point)
	if err != nil {
		return "", fmt.Errorf("error marshaling to WKT: %w", err)
	}

	return wktString, nil
}

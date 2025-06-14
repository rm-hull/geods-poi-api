package internal

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type RefDataResponse struct {
	Count       int            `json:"count"`
	LastUpdated string         `json:"last_updated"`
	Categories  map[string]int `json:"categories"`
	Attribution []string       `json:"attribution"`
}

func RefData(db *sql.DB) func(c *gin.Context) {
	categories, count, err := precomputeCategories(db)
	if err != nil {
		log.Fatalf("error pre-computing categories: %v", err)
	}

	lastUpdated, err := retrieveLastUpdated(db)
	if err != nil {
		log.Fatalf("error retrieving last updated timestamp: %v", err)
	}

	return func(c *gin.Context) {
		c.JSON(http.StatusOK, RefDataResponse{
			Count:       count,
			LastUpdated: lastUpdated,
			Categories:  categories,
			Attribution: ATTRIBUTION,
		})
	}
}

func retrieveLastUpdated(db *sql.DB) (string, error) {
	var timestamp string
	err := db.QueryRow(`SELECT last_change FROM gpkg_contents`).Scan(&timestamp)
	if err != nil {
		return "", fmt.Errorf("error retrieving timestamp: %w", err)
	}

	if timestamp == "" {
		return "unknown", nil
	}

	log.Printf("Last updated timestamp in db: %s", timestamp)
	return timestamp, nil
}

func precomputeCategories(db *sql.DB) (map[string]int, int, error) {
	log.Println("Pre-computing POI categories...")
	rows, err := db.Query(`SELECT main_category, alternate_category FROM poi_uk`)
	if err != nil {
		return nil, 0, fmt.Errorf("error querying database: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("error closing rows: %v", err)
		}
	}()

	categories := make(map[string]int, 0)
	count := 0
	incr := func(key string) {
		if _, exists := categories[key]; !exists {
			categories[key] = 0
		}
		categories[key]++
	}

	var mainCategory sql.NullString
	var alternateCategory sql.NullString

	for rows.Next() {
		if err := rows.Scan(&mainCategory, &alternateCategory); err != nil {
			return nil, 0, fmt.Errorf("error scanning row: %w", err)
		}

		if mainCategory.Valid {
			incr(mainCategory.String)
		}

		if alternateCategory.Valid {
			for cat := range strings.SplitSeq(alternateCategory.String, "|") {
				incr(strings.TrimSpace(cat))
			}
		}

		count++
	}

	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error during rows iteration: %w", err)
	}

	log.Printf("Discovered %d distinct categories from %d points of interest", len(categories), count)

	return categories, count, nil
}

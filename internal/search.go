package internal

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
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

func Search(db *sql.DB) func(c *gin.Context) {
	return func(c *gin.Context) {
		bbox, err := parseBBox(c.Query("bbox"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		categories, err := parseCategories(c.Query("categories"))
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

			if len(categories) == 0 || hasCategoryMatch(poi.Categories, categories) {
				results = append(results, poi)
			}
		}
		if err = rows.Err(); err != nil {
			log.Printf("error during rows iteration: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "An internal server error occurred"})
			return
		}

		c.JSON(http.StatusOK, Response{Results: results})
	}
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

func parseCategories(categoriesStr string) (map[string]struct{}, error) {
	if categoriesStr == "" {
		return nil, nil // No categories specified, return nil
	}

	categories := make(map[string]struct{})
	for cat := range strings.SplitSeq(categoriesStr, ",") {
		cat = strings.TrimSpace(cat)
		if cat == "" {
			return nil, fmt.Errorf("category cannot be an empty string")
		}
		categories[strings.ToLower(cat)] = struct{}{}
	}

	return categories, nil
}

func hasCategoryMatch(items []string, categories map[string]struct{}) bool {
	for _, item := range items {
		if _, exists := categories[item]; exists {
			return true
		}
	}
	return false
}

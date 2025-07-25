package internal

import (
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
	"github.com/kofalt/go-memoize"
)

type Photo struct {
	ID             string     `json:"id"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	URLs           URLs       `json:"urls"`
	AltDescription *string    `json:"alt_description,omitempty"`
	BlurHash       *string    `json:"blur_hash,omitempty"`
	Color          *string    `json:"color,omitempty"`
	Description    *string    `json:"description,omitempty"`
	Height         int        `json:"height"`
	Likes          int        `json:"likes"`
	Links          PhotoLinks `json:"links"`
	PromotedAt     *time.Time `json:"promoted_at,omitempty"`
	Width          int        `json:"width"`
	User           User       `json:"user"`
}

type URLs struct {
	Full    string `json:"full"`
	Raw     string `json:"raw"`
	Regular string `json:"regular"`
	Small   string `json:"small"`
	Thumb   string `json:"thumb"`
}

type PhotoLinks struct {
	Self             string `json:"self"`
	HTML             string `json:"html"`
	Download         string `json:"download"`
	DownloadLocation string `json:"download_location"`
}

type User struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Links UserLinks `json:"links"`
}

type UserLinks struct {
	Self      string `json:"self"`
	HTML      string `json:"html"`
	Photos    string `json:"photos"`
	Likes     string `json:"likes"`
	Portfolio string `json:"portfolio"`
}

type Response struct {
	Results    []Photo `json:"results"`
	Total      int     `json:"total"`
	TotalPages int     `json:"total_pages"`
}

const UNSPLASH_API_URL = "https://api.unsplash.com/search/photos"

var httpClient = &http.Client{}

func Image(cache *memoize.Memoizer) func(c *gin.Context) {
	return func(c *gin.Context) {
		category := c.Param("category")
		if category == "" {
			c.JSON(400, gin.H{"error": "category is required"})
			return
		}

		if _, exists := icons[category]; !exists {
			c.JSON(404, gin.H{"error": "category not found"})
			return
		}

		resp, err, _ := memoize.Call(cache, fmt.Sprintf("image/%s", category), func() (*Response, error) {
			log.Printf("Fetching image for category: %s", category)
			return fetch(c.Request.Context(), category)
		})

		if err != nil {
			log.Printf("Error fetching image: %v", err)
			c.JSON(500, gin.H{"error": "failed to fetch image"})
			return
		}
		if len(resp.Results) == 0 {
			c.JSON(404, gin.H{"error": "no image found for category"})
			return
		}

		c.JSON(200, gin.H{
			"src": resp.Results[0].URLs.Small,
			"alt": resp.Results[0].AltDescription,
			"attribution": gin.H{
				"name": resp.Results[0].User.Name,
				"link": resp.Results[0].User.Links.HTML,
			},
		})
	}
}

func fetch(ctx context.Context, category string) (*Response, error) {

	req, err := http.NewRequestWithContext(ctx, "GET", UNSPLASH_API_URL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	q := req.URL.Query()
	q.Add("query", category)
	q.Add("per_page", "1")
	q.Add("orientation", "landscape")
	q.Add("order_by", "relevant")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Authorization", "Client-ID "+os.Getenv("UNSPLASH_ACCESS_KEY"))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip,br,deflate")
	req.Header.Set("User-Agent", "https://github.com/rm-hull/geods-poi-api")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}()

	var reader io.Reader = resp.Body
	switch resp.Header.Get("Content-Encoding") {
	case "br":
		reader = brotli.NewReader(resp.Body)
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error creating gzip reader: %w", err)
		}
		defer func() {
			if err := reader.(*gzip.Reader).Close(); err != nil {
				log.Printf("Error closing gzip reader: %v", err)
			}
		}()
	case "deflate":
		reader = flate.NewReader(resp.Body)
		defer func() {
			if err := reader.(io.ReadCloser).Close(); err != nil {
				log.Printf("Error closing deflate reader: %v", err)
			}
		}()
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("bad response (%s): %s", resp.Status, body)
	}

	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON: %w", err)
	}

	return &response, nil
}

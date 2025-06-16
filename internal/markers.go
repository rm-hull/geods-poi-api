package internal

import (
	"github.com/gin-gonic/gin"

	_ "embed"
	"encoding/json"
	"log"
)

//go:embed _mappings.gemini2.5_pro.json
var mappingsFileContents []byte
var icons map[string]string

func init() {
	err := json.Unmarshal(mappingsFileContents, &icons)
	if err != nil {
		log.Fatalf("failed to unmarshal mappings: %v", err)
	}
}

func Marker(c *gin.Context) {
	category := c.Param("category")
	if category == "" {
		c.JSON(400, gin.H{"error": "category is required"})
		return
	}

	icon, exists := icons[category]
	if icon == "" || !exists {
		c.JSON(404, gin.H{"error": "category not found"})
		return
	}

	c.Header("Content-Type", "image/png")
	c.File("./data/markers/" + icon)
}

func Shadow(c *gin.Context) {
	c.Header("Content-Type", "image/png")
	c.File("./data/markers/_shadow.png")
}

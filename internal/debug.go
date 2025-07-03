package internal

import (
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/earthboundkid/versioninfo/v2"
)

func ShowVersion() {
	log.Printf("Version: %s\n", versioninfo.Short())
}

func EnvironmentVars() {
	log.Println("Environment variables")

	sensitiveRegex := regexp.MustCompile(`(?i)(PASSWORD|API_KEY|ACCESS_KEY|SECRET)`)
	environ := os.Environ()
	sort.Slice(environ, func(i, j int) bool {
		keyI := strings.SplitN(environ[i], "=", 2)[0]
		keyJ := strings.SplitN(environ[j], "=", 2)[0]
		return keyI < keyJ
	})

	for _, entry := range environ {
		kv := strings.SplitN(entry, "=", 2)
		if sensitiveRegex.MatchString(kv[0]) {
			log.Printf("  %s: ********\n", kv[0])
		} else {
			log.Printf("  %s: %s\n", kv[0], kv[1])
		}
	}
}

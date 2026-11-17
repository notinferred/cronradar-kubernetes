package cronradar

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// MaxMonitorKeyLength is the server's limit on a monitor key.
const MaxMonitorKeyLength = 100

// NormalizeKey lowercases a raw identifier, turns separators into single hyphens,
// drops other punctuation and shortens the result to the monitor-key limit,
// matching CronRadarKeyNormalizer in the .NET SDK.
func NormalizeKey(raw string) string {
	raw = strings.Trim(raw, "\"'` \t\r\n")
	var builder strings.Builder
	lastWasSeparator := false
	for _, r := range raw {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			builder.WriteRune(unicode.ToLower(r))
			lastWasSeparator = false
		case r == '.' || r == '/' || r == '_' || r == ' ' || r == '-':
			if !lastWasSeparator && builder.Len() > 0 {
				builder.WriteRune('-')
				lastWasSeparator = true
			}
		}
	}
	return shorten(strings.TrimSuffix(builder.String(), "-"))
}

// shorten keeps the first 91 characters without trailing hyphens, a hyphen, and the
// first 8 hex characters of the SHA-256 of the full key, as every CronRadar SDK does.
func shorten(key string) string {
	const hashLength = 8
	runes := []rune(key)
	if len(runes) <= MaxMonitorKeyLength {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	prefix := strings.TrimRight(string(runes[:MaxMonitorKeyLength-hashLength-1]), "-")
	return prefix + "-" + hex.EncodeToString(sum[:hashLength/2])
}

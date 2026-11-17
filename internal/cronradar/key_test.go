package cronradar

import (
	"strings"
	"testing"
)

func TestNormalizeKey(t *testing.T) {
	cases := map[string]string{
		"prod-nightly-backup": "prod-nightly-backup",
		"Prod_Nightly.Backup": "prod-nightly-backup",
		"  \"quoted-key\"  ":  "quoted-key",
		"job--name":           "job-name",
		"job-name-":           "job-name",
		"job@name#test":       "jobnametest",
		"team/reports.daily":  "team-reports-daily",
		"":                    "",
		"---":                 "",
	}
	for input, expected := range cases {
		if actual := NormalizeKey(input); actual != expected {
			t.Errorf("NormalizeKey(%q) = %q, want %q", input, actual, expected)
		}
	}
}

// Shared vectors from the .NET SDK's CronRadarKeyNormalizerTests; every SDK that derives keys must match them.
func TestNormalizeKeyShortensOverLimitLikeEverySDK(t *testing.T) {
	shortened := NormalizeKey(strings.Repeat("abcdef-", 15) + "end")
	if expected := strings.Repeat("abcdef-", 12) + "abcdef-4f91bd6e"; shortened != expected {
		t.Fatalf("got %q, want %q", shortened, expected)
	}

	shortened = NormalizeKey("nightly-" + strings.Repeat("x", 95))
	if expected := "nightly-" + strings.Repeat("x", 83) + "-89d49067"; shortened != expected {
		t.Fatalf("got %q, want %q", shortened, expected)
	}

	atLimit := strings.Repeat("k", MaxMonitorKeyLength)
	if NormalizeKey(atLimit) != atLimit {
		t.Fatal("a key at the limit must stay unchanged")
	}
}

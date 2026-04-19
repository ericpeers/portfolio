package tests

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestStaticcheck runs staticcheck on the entire codebase.
// Suppression rules are in staticcheck.conf at the project root.
func TestStaticcheck(t *testing.T) {
	t.Parallel()
	root := getRepoRoot(t)

	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("Could not determine home directory: %v", err)
	}
	staticcheckBin := filepath.Join(homeDir, "go", "bin", "staticcheck")

	if _, err := os.Stat(staticcheckBin); err != nil {
		t.Skipf("staticcheck binary not found at %s, skipping", staticcheckBin)
	}

	cmd := exec.Command(staticcheckBin, "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("staticcheck found issues:\n%s", string(output))
	}
}

// TestGoVet runs `go vet ./...` on the entire codebase.
func TestGoVet(t *testing.T) {
	t.Parallel()
	root := getRepoRoot(t)

	cmd := exec.Command("go", "vet", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("go vet found issues:\n%s", string(output))
	}
}

// TestGosec runs gosec on the entire codebase.
// Suppression rules use #nosec comments at the call site.
//
// Primary purpose: regression guard against SQL injection via dynamic query construction.
// All SQL in this codebase uses pgx parameterized queries ($1, $2, ...) which are safe,
// but gosec will catch any future use of fmt.Sprintf or string concatenation in queries
// before it reaches code review (G201, G202).
func TestGosec(t *testing.T) {
	t.Parallel()
	root := getRepoRoot(t)

	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("Could not determine home directory: %v", err)
	}
	gosecBin := filepath.Join(homeDir, "go", "bin", "gosec")

	if _, err := os.Stat(gosecBin); err != nil {
		t.Skipf("gosec binary not found at %s, skipping", gosecBin)
	}

	cmd := exec.Command(gosecBin, "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("gosec found issues:\n%s", string(output))
	}
}

// TestDeadcode runs deadcode analysis on the codebase and fails if any
// unreachable functions are found that are not on the allowlist below.
//
// To allowlist a function, add an entry in the format "relative/path.go:FuncName".
// Use this only for functions that are intentionally kept despite being unreachable
// from main (e.g., testing utilities, future API surface, interface implementations).
//
// we choose not to run `deadcode -test` because that increases smell. Test code should not
// have codepaths in the production codebase (/internal). We should only be testing production code.
// an example of this was in constructors NewClient that had test specific variants.
func TestDeadcode(t *testing.T) {
	t.Parallel()
	root := getRepoRoot(t)

	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("Could not determine home directory: %v", err)
	}
	deadcodeBin := filepath.Join(homeDir, "go", "bin", "deadcode")

	if _, err := os.Stat(deadcodeBin); err != nil {
		t.Skipf("deadcode binary not found at %s, skipping", deadcodeBin)
	}

	// Allowlist of intentionally unreachable functions.
	// Key format: "relative/path/file.go:FuncName"
	// Example: "internal/providers/alphavantage/client.go:NewClientWithBaseURL"
	allowlist := map[string]bool{
		// Reset is intentionally test-only; production code never calls it.
		"internal/apperrors/counter.go:Reset": true,
	}

	cmd := exec.Command(deadcodeBin, ".")
	cmd.Dir = root
	output, _ := cmd.CombinedOutput()

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var violations []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Expected format: "internal/foo/bar.go:12:6: unreachable func: SomeName"
		const marker = ": unreachable func: "
		idx := strings.Index(line, marker)
		if idx == -1 {
			continue
		}

		funcName := line[idx+len(marker):]
		locationPart := line[:idx] // e.g. "internal/foo/bar.go:12:6"

		// Strip ":line:col" to get just the file path
		filePart := locationPart
		for i := 0; i < 2; i++ {
			if lastColon := strings.LastIndex(filePart, ":"); lastColon != -1 {
				filePart = filePart[:lastColon]
			}
		}

		key := fmt.Sprintf("%s:%s", filePart, funcName)
		if !allowlist[key] {
			violations = append(violations, fmt.Sprintf("  %s (key: %q)", line, key))
		}
	}

	if len(violations) > 0 {
		t.Errorf("deadcode found %d unreachable function(s) not in the allowlist.\n"+
			"Either delete them or add their key to the allowlist in TestDeadcode:\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
}

package tests

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// permissiveLicenses is the allowlist of SPDX identifiers acceptable for commercial use.
// Add an entry only after verifying the license terms are compatible with this application.
// Note: MPL-2.0 is weak copyleft — it imposes source-disclosure obligations only on
// modifications to the MPL'd files themselves, not on the application as a whole.
// It is acceptable here as long as we do not modify the upstream MPL'd source files.
var permissiveLicenses = map[string]bool{
	"MIT":          true,
	"Apache-2.0":   true,
	"BSD-2-Clause": true,
	"BSD-3-Clause": true,
	"ISC":          true,
	"MPL-2.0":      true, // file-scoped copyleft; see note above
	"Unlicense":    true,
	"CC0-1.0":      true,
}

// ownModule is the Go module path for this project. Packages under this prefix
// have no LICENSE file by design (private codebase) and are excluded from all
// third-party license checks.
const ownModule = "github.com/epeers/portfolio"

// TestLicenses verifies that every third-party dependency uses a license that
// is permissive for commercial use.
//
// go-licenses emits "contains non-Go code that can't be inspected" warnings for
// packages with assembly (.s) files such as golang.org/x/sys/unix. These are
// informational only: the tool resolves licenses at the module level, so those
// sub-packages are covered by their parent module's LICENSE file and do not
// require a separate whitelist entry.
func TestLicenses(t *testing.T) {
	t.Parallel()

	bin := goLicensesBin(t)
	root := getRepoRoot(t)

	var stdout, stderr bytes.Buffer
	cmd := exec.Command(bin, "report", "./...")
	cmd.Dir = root
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("go-licenses report failed: %v\nstderr:\n%s", err, stderr.String())
	}

	type finding struct {
		module  string
		license string
	}
	var violations []finding

	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// CSV format: module_path,license_url,spdx_identifier
		parts := strings.SplitN(line, ",", 3)
		if len(parts) != 3 {
			continue
		}
		module, license := parts[0], parts[2]

		if module == ownModule || strings.HasPrefix(module, ownModule+"/") {
			continue
		}

		if !permissiveLicenses[license] {
			violations = append(violations, finding{module, license})
		}
	}

	if len(violations) > 0 {
		var sb strings.Builder
		for _, v := range violations {
			fmt.Fprintf(&sb, "  %-60s %s\n", v.module, v.license)
		}
		t.Errorf("%d dependency(ies) with non-permissive or unrecognized license(s).\n"+
			"Verify the license, then either add it to permissiveLicenses or replace the dependency:\n%s",
			len(violations), sb.String())
	}
}

// goLicensesBin returns the path to the go-licenses binary, skipping the test
// if it is not installed.
func goLicensesBin(t *testing.T) string {
	t.Helper()
	if path, err := exec.LookPath("go-licenses"); err == nil {
		return path
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("could not determine home directory: %v", err)
	}
	bin := filepath.Join(homeDir, "go", "bin", "go-licenses")
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("go-licenses not found in PATH or at %s; install with: go install github.com/google/go-licenses@latest", bin)
	}
	return bin
}

package tests

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestSQLFluffLint runs sqlfluff lint on create_tables.sql and verifies no errors
func TestSQLFluffLint(t *testing.T) {
	schemaPath := getSchemaPath(t)

	cmd := exec.Command("sqlfluff", "lint", schemaPath, "--dialect", "postgres")
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	// Check for "All Finished" in output
	if !strings.Contains(outputStr, "All Finished") {
		t.Errorf("sqlfluff did not complete successfully. Expected 'All Finished' in output.\nOutput:\n%s", outputStr)
	}

	// Check for FAIL in output
	if strings.Contains(outputStr, "FAIL") {
		t.Errorf("sqlfluff found linting errors:\n%s", outputStr)
	}

	// If sqlfluff command itself failed (not installed, etc.)
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok && exitErr.ExitCode() != 0 {
			// sqlfluff returns non-zero on lint errors
			if strings.Contains(outputStr, "FAIL") {
				t.Errorf("sqlfluff lint failed with errors:\n%s", outputStr)
			}
		} else if !ok {
			t.Fatalf("Failed to run sqlfluff (is it installed?): %v\nOutput: %s", err, outputStr)
		}
	}

	t.Logf("sqlfluff output:\n%s", outputStr)
}

// TestSQLSchemaCreation creates a temporary database and verifies create_tables.sql runs without errors
func TestSQLSchemaCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database creation test in short mode")
	}

	// Generate unique temp database name
	tempDBName := fmt.Sprintf("test_portfolio_%d", time.Now().UnixNano())
	schemaPath := getSchemaPath(t)

	// Create temporary database
	createCmd := exec.Command("createdb", tempDBName)
	if output, err := createCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create temporary database %s: %v\nOutput: %s", tempDBName, err, output)
	}

	// Ensure cleanup
	defer func() {
		dropCmd := exec.Command("dropdb", "--if-exists", tempDBName)
		dropCmd.Run() // Ignore errors on cleanup
	}()

	t.Logf("Created temporary database: %s", tempDBName)

	// Run schema file against temp database
	psqlCmd := exec.Command("psql", "-d", tempDBName, "-f", schemaPath, "-v", "ON_ERROR_STOP=1")
	output, err := psqlCmd.CombinedOutput()
	outputStr := string(output)

	if err != nil {
		t.Fatalf("Failed to execute create_tables.sql on %s: %v\nOutput:\n%s", tempDBName, err, outputStr)
	}

	// Check for ERROR in output (PostgreSQL error messages)
	if strings.Contains(outputStr, "ERROR:") {
		t.Errorf("create_tables.sql produced errors:\n%s", outputStr)
	}

	t.Logf("Schema creation successful. Output:\n%s", outputStr)

	// Verify key tables exist
	verifyCmd := exec.Command("psql", "-d", tempDBName, "-c",
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
	verifyOutput, err := verifyCmd.CombinedOutput()
	if err != nil {
		t.Errorf("Failed to verify tables: %v", err)
	}

	requiredTables := []string{"portfolio", "portfolio_membership", "dim_security", "dim_user"}
	for _, table := range requiredTables {
		if !strings.Contains(string(verifyOutput), table) {
			t.Errorf("Expected table '%s' not found in database", table)
		}
	}

	t.Logf("Tables verified:\n%s", verifyOutput)
}

// TestRepositorySchemaSync compares repository Create/Update methods against create_tables.sql schema
func TestRepositorySchemaSync(t *testing.T) {
	schemaPath := getSchemaPath(t)
	repoPath := getRepoPath(t)

	// Parse schema to get table definitions
	schemaFields := parseSchemaFields(t, schemaPath)

	// Parse repository to get fields used in Create/Update
	repoFields := parseRepoFields(t, repoPath)

	// Compare each table
	for tableName, repoTableFields := range repoFields {
		schemaTableFields, exists := schemaFields[tableName]
		if !exists {
			t.Errorf("Table '%s' referenced in repository but not found in schema", tableName)
			continue
		}

		// Check for missing fields (schema has but repo doesn't use)
		// Exclude primary key fields from this check
		for field := range schemaTableFields {
			if isPrimaryKeyField(tableName, field) {
				continue
			}
			if !repoTableFields[field] {
				t.Errorf("SYNC ERROR: Table '%s' - field '%s' exists in schema but not used in repository Create/Update. "+
					"Repository file and create_tables.sql may be out of sync.", tableName, field)
			}
		}

		// Check for extra fields (repo uses but schema doesn't have)
		for field := range repoTableFields {
			if !schemaTableFields[field] {
				t.Errorf("SYNC ERROR: Table '%s' - field '%s' used in repository but not found in schema. "+
					"Repository file and create_tables.sql may be out of sync.", tableName, field)
			}
		}
	}
}

// Helper functions

func getSchemaPath(t *testing.T) string {
	t.Helper()
	// Try multiple possible locations
	paths := []string{
		"../create_tables.sql",
		"create_tables.sql",
		filepath.Join(os.Getenv("PWD"), "create_tables.sql"),
		"/home/epeers/portfolio/create_tables.sql",
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			absPath, _ := filepath.Abs(path)
			return absPath
		}
	}

	t.Fatal("create_tables.sql not found")
	return ""
}

func getRepoPath(t *testing.T) string {
	t.Helper()
	paths := []string{
		"../internal/repository/portfolio_repo.go",
		"internal/repository/portfolio_repo.go",
		"/home/epeers/portfolio/internal/repository/portfolio_repo.go",
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			absPath, _ := filepath.Abs(path)
			return absPath
		}
	}

	t.Fatal("portfolio_repo.go not found")
	return ""
}

// parseSchemaFields extracts table column definitions from create_tables.sql
func parseSchemaFields(t *testing.T, schemaPath string) map[string]map[string]bool {
	t.Helper()

	content, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to read schema file: %v", err)
	}

	// Remove SQL comments before parsing
	contentStr := string(content)
	// Remove single-line comments
	singleLineComment := regexp.MustCompile(`--[^\n]*`)
	contentStr = singleLineComment.ReplaceAllString(contentStr, "")
	// Remove multi-line comments
	multiLineComment := regexp.MustCompile(`/\*.*?\*/`)
	contentStr = multiLineComment.ReplaceAllString(contentStr, "")

	tables := make(map[string]map[string]bool)

	// Regex to find CREATE TABLE statements
	tableRegex := regexp.MustCompile(`(?is)create\s+table\s+(\w+)\s*\((.*?)\);`)
	matches := tableRegex.FindAllStringSubmatch(contentStr, -1)

	for _, match := range matches {
		tableName := strings.ToLower(match[1])
		tableBody := match[2]

		fields := make(map[string]bool)

		// Parse column definitions
		scanner := bufio.NewScanner(strings.NewReader(tableBody))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			line = strings.TrimSuffix(line, ",")

			// Skip empty lines
			if line == "" {
				continue
			}

			// Skip constraints, PRIMARY KEY, etc.
			lowerLine := strings.ToLower(line)
			if strings.HasPrefix(lowerLine, "primary key") ||
				strings.HasPrefix(lowerLine, "foreign key") ||
				strings.HasPrefix(lowerLine, "unique") ||
				strings.HasPrefix(lowerLine, "constraint") ||
				strings.HasPrefix(lowerLine, "check") {
				continue
			}

			// Extract column name (first word)
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				colName := strings.ToLower(parts[0])
				// Skip if it looks like a constraint or SQL keyword
				if colName != "primary" && colName != "foreign" && colName != "unique" &&
					colName != "--" && !strings.HasPrefix(colName, "--") {
					fields[colName] = true
				}
			}
		}

		tables[tableName] = fields
	}

	return tables
}

// parseRepoFields extracts fields used in INSERT/UPDATE statements from repository
func parseRepoFields(t *testing.T, repoPath string) map[string]map[string]bool {
	t.Helper()

	content, err := os.ReadFile(repoPath)
	if err != nil {
		t.Fatalf("Failed to read repository file: %v", err)
	}

	tables := make(map[string]map[string]bool)

	// Find INSERT statements
	insertRegex := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)`)
	insertMatches := insertRegex.FindAllStringSubmatch(string(content), -1)

	for _, match := range insertMatches {
		tableName := strings.ToLower(match[1])
		columnsStr := match[2]

		if tables[tableName] == nil {
			tables[tableName] = make(map[string]bool)
		}

		columns := strings.Split(columnsStr, ",")
		for _, col := range columns {
			colName := strings.ToLower(strings.TrimSpace(col))
			tables[tableName][colName] = true
		}
	}

	// Find UPDATE statements
	updateRegex := regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+SET\s+([^W]+)WHERE`)
	updateMatches := updateRegex.FindAllStringSubmatch(string(content), -1)

	for _, match := range updateMatches {
		tableName := strings.ToLower(match[1])
		setClause := match[2]

		if tables[tableName] == nil {
			tables[tableName] = make(map[string]bool)
		}

		// Parse SET clause for column names
		assignments := strings.Split(setClause, ",")
		for _, assignment := range assignments {
			parts := strings.Split(assignment, "=")
			if len(parts) >= 1 {
				colName := strings.ToLower(strings.TrimSpace(parts[0]))
				tables[tableName][colName] = true
			}
		}
	}

	return tables
}

// isPrimaryKeyField checks if a field is a primary key (should be excluded from sync check)
func isPrimaryKeyField(tableName, fieldName string) bool {
	primaryKeys := map[string][]string{
		"portfolio":            {"id"},
		"portfolio_membership": {"portfolio_id", "security_id"},
		"dim_security":         {"id"},
		"dim_user":             {"id"},
		"dim_etf_membership":   {"dim_security_id", "dim_composite_id"},
	}

	if pks, exists := primaryKeys[tableName]; exists {
		for _, pk := range pks {
			if pk == fieldName {
				return true
			}
		}
	}
	return false
}

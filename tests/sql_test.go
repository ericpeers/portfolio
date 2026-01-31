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
	schemaPath := getFilePath(t, "create_tables.sql")

	cmd := exec.Command("sqlfluff", "lint", schemaPath, "--dialect", "postgres")
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	// Check for "All Finished" in output
	if !strings.Contains(outputStr, "All Finished") {
		t.Errorf("sqlfluff did not complete successfully. Expected 'All Finished' in output.\nOutput:\n%s\nErr: %v", outputStr, err)
	}

	// Check for FAIL in output
	if strings.Contains(outputStr, "FAIL") {
		t.Errorf("sqlfluff found linting errors:\n%s\nconsider running sqlfluff fix?", outputStr)
	}
	//sqlfluff fix creaste_tables --dialect postgres

	//t.Logf("sqlfluff output:\n%s", outputStr)
}

// TestSQLSchemaCreation creates a temporary database and verifies create_tables.sql runs without errors
func TestSQLSchemaCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database creation test in short mode")
	}

	// Generate unique temp database name
	tempDBName := fmt.Sprintf("test_portfolio_%d", time.Now().UnixNano())
	schemaPath := getFilePath(t, "create_tables.sql")

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
	schemaPath := getFilePath(t, "create_tables.sql")
	repoPath := getFilePath(t, "internal/repository/portfolio_repo.go")

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

func getRepoRoot(t *testing.T) string {
	t.Helper()

	// Start from current working directory
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	// Walk up looking for .git or go.mod
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("Could not find repository root")
		}
		dir = parent
	}
}

func getFilePath(t *testing.T, relativePath string) string {
	t.Helper()
	root := getRepoRoot(t)
	fullPath := filepath.Join(root, relativePath)

	if _, err := os.Stat(fullPath); err != nil {
		t.Fatalf("File not found: %s", fullPath)
	}
	return fullPath
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

// TestRepositoryTableOwnership verifies each repository only accesses tables it owns
func TestRepositoryTableOwnership(t *testing.T) {
	// Define table ownership: which repository owns which tables
	tableOwnership := map[string]string{
		"dim_security":         "security_repo.go",
		"dim_security_types":   "security_type_repo.go",
		"etf_memberships":      "security_repo.go",
		"dim_exchanges":        "exchange_repo.go",
		"fact_price":           "price_cache_repo.go",
		"fact_price_range":     "price_cache_repo.go",
		"quote_cache":          "price_cache_repo.go",
		"treasury_rates":       "price_cache_repo.go",
		"portfolio":            "portfolio_repo.go",
		"portfolio_membership": "portfolio_repo.go",
	}

	// Define allowed cross-repository JOINs (table -> list of repos allowed to JOIN with it)
	// These are read-only JOINs for lookup purposes, not direct modifications
	allowedJoins := map[string][]string{
		"dim_security_types": {"security_repo.go"}, // IsETFOrMutualFund joins for type lookup
	}

	repoDir := getFilePath(t, "internal/repository")
	entries, err := os.ReadDir(repoDir)
	if err != nil {
		t.Fatalf("Failed to read repository directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_repo.go") {
			continue
		}

		filePath := filepath.Join(repoDir, entry.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Failed to read %s: %v", entry.Name(), err)
		}

		tables := extractTablesFromQueries(string(content))
		for _, table := range tables {
			owner, exists := tableOwnership[table]
			if !exists {
				// Unknown table - not in our ownership map, skip
				continue
			}
			if owner != entry.Name() {
				// Check if this is an allowed JOIN
				if allowed, ok := allowedJoins[table]; ok {
					isAllowed := false
					for _, repo := range allowed {
						if repo == entry.Name() {
							isAllowed = true
							break
						}
					}
					if isAllowed {
						continue
					}
				}
				t.Errorf("Table ownership violation: table '%s' accessed in %s but owned by %s",
					table, entry.Name(), owner)
			}
		}
	}
}

// extractTablesFromQueries extracts table names from SQL queries in Go code
func extractTablesFromQueries(content string) []string {
	tables := make(map[string]bool)

	// Match FROM clause tables
	fromRegex := regexp.MustCompile(`(?i)\bFROM\s+(\w+)`)
	fromMatches := fromRegex.FindAllStringSubmatch(content, -1)
	for _, match := range fromMatches {
		tables[strings.ToLower(match[1])] = true
	}

	// Match JOIN clause tables
	joinRegex := regexp.MustCompile(`(?i)\bJOIN\s+(\w+)`)
	joinMatches := joinRegex.FindAllStringSubmatch(content, -1)
	for _, match := range joinMatches {
		tables[strings.ToLower(match[1])] = true
	}

	// Match INSERT INTO tables
	insertRegex := regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+(\w+)`)
	insertMatches := insertRegex.FindAllStringSubmatch(content, -1)
	for _, match := range insertMatches {
		tables[strings.ToLower(match[1])] = true
	}

	// Match UPDATE tables
	updateRegex := regexp.MustCompile(`(?i)\bUPDATE\s+(\w+)`)
	updateMatches := updateRegex.FindAllStringSubmatch(content, -1)
	for _, match := range updateMatches {
		tables[strings.ToLower(match[1])] = true
	}

	// Match DELETE FROM tables
	deleteRegex := regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+(\w+)`)
	deleteMatches := deleteRegex.FindAllStringSubmatch(content, -1)
	for _, match := range deleteMatches {
		tables[strings.ToLower(match[1])] = true
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result
}

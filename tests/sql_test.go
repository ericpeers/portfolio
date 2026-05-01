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
	t.Parallel()
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
	//-- sqlfluff lint create_tables.sql --dialect postgres
	//-- sqlfluff fix create_tables.sql --dialect postgres

	//t.Logf("sqlfluff output:\n%s", outputStr)
}

// TestSchemaMatchesDatabase verifies that the live database schema matches create_tables.sql
// column-for-column. Catches drift between what was applied to the DB and what the binary
// was built for — including an incomplete pg_restore where tables or columns are missing.
func TestSchemaMatchesDatabase(t *testing.T) {
	t.Parallel()
	schemaPath := getFilePath(t, "create_tables.sql")

	// Expected: tables + columns parsed from create_tables.sql
	expected := parseSchemaFields(t, schemaPath)

	// Actual: tables + columns from the live database
	rows, err := testPool.Query(t.Context(), `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'
		ORDER BY table_name, ordinal_position`)
	if err != nil {
		t.Fatalf("failed to query information_schema.columns: %v", err)
	}
	defer rows.Close()

	actual := make(map[string]map[string]bool)
	for rows.Next() {
		var tbl, col string
		if err := rows.Scan(&tbl, &col); err != nil {
			t.Fatalf("failed to scan column row: %v", err)
		}
		if actual[tbl] == nil {
			actual[tbl] = make(map[string]bool)
		}
		actual[tbl][col] = true
	}

	// Every table in create_tables.sql must exist in the DB with all its columns
	for tbl, cols := range expected {
		if _, ok := actual[tbl]; !ok {
			t.Errorf("table %q is in create_tables.sql but missing from the live DB (incomplete pg_restore?)", tbl)
			continue
		}
		for col := range cols {
			if !actual[tbl][col] {
				t.Errorf("column %q of table %q is in create_tables.sql but missing from the live DB", col, tbl)
			}
		}
	}

	// Tables from in-flight feature branches that exist in the DB but not yet in main's create_tables.sql.
	// Remove entries here once the branch merges.
	futureTables := map[string]bool{
		"fact_fundamentals":      true,
		"fact_financials_history": true,
		"dim_security_listings":  true,
	}

	// Every table in the DB must be in create_tables.sql (catches untracked schema changes)
	for tbl := range actual {
		if futureTables[tbl] {
			continue
		}
		if _, ok := expected[tbl]; !ok {
			t.Errorf("table %q exists in the live DB but is not in create_tables.sql", tbl)
		}
	}
}

// TestSQLSchemaCreation creates a temporary database and verifies create_tables.sql runs without errors
func TestSQLSchemaCreation(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

// TestNoImplicitJoins ensures all SQL queries use explicit JOIN ... ON syntax.
// FROM a, b WHERE a.id = b.id is not a personality trait. It is a crime.
func TestNoImplicitJoins(t *testing.T) {
	t.Parallel()
	root := getRepoRoot(t)
	internalDir := filepath.Join(root, "internal")

	// Matches: FROM <word> <whitespace> <comma> — the hallmark of implicit join syntax
	implicitJoinRegex := regexp.MustCompile(`(?i)\bFROM\s+\w+\s*,`)
	backtickRegex := regexp.MustCompile("`[^`]+`")

	err := filepath.Walk(internalDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		contentStr := string(content)
		for _, loc := range backtickRegex.FindAllStringIndex(contentStr, -1) {
			sqlStr := contentStr[loc[0]:loc[1]]
			if implicitJoinRegex.MatchString(sqlStr) {
				lineNum := strings.Count(contentStr[:loc[0]], "\n") + 1
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s:%d: implicit JOIN detected. "+
					"Congratulations, you've rediscovered SQL syntax from before the fall of the Soviet Union. "+
					"Please use explicit JOIN ... ON syntax.", rel, lineNum)
			}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to walk internal directory: %v", err)
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
	t.Parallel()
	// Define table ownership: which repository owns which tables
	tableOwnership := map[string]string{
		"dim_security":         "security_repo.go",
		"dim_etf_membership":   "security_repo.go",
		"dim_etf_pull_range":   "security_repo.go",
		"dim_exchanges":        "exchange_repo.go",
		"fact_price":           "price_repo.go",
		"fact_price_range":     "price_repo.go",
		"fact_event":           "price_repo.go",
		"portfolio":            "portfolio_repo.go",
		"portfolio_membership": "portfolio_repo.go",
		"portfolio_glance":     "glance_repo.go",
		"app_hints":            "hints_repo.go",
		"dim_user":             "user_repo.go",
	}

	// Define allowed cross-repository JOINs (table -> list of repos allowed to JOIN with it)
	// These are read-only JOINs for lookup purposes, not direct modifications.
	// See CLAUDE.md "Repository Table Ownership" exception.
	allowedJoins := map[string][]string{
		"dim_exchanges":        {"security_repo.go", "price_repo.go"}, // price_repo JOINs for export exchange name
		"dim_security":         {"portfolio_repo.go", "price_repo.go"}, // price_repo JOINs for export ticker
		"portfolio_membership": {"price_repo.go"},     // JOIN for dividends in a portfolio
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
				t.Fatalf("Added a new table? Table: \"%s\". I don't know which repository file should own it", table)
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

// extractTablesFromQueries extracts table names from SQL queries in Go code.
// It only parses backtick-delimited strings to avoid false positives from
// comments, error messages, and other non-SQL text.
func extractTablesFromQueries(content string) []string {
	tables := make(map[string]bool)

	// SQL keywords that should not be treated as table names
	sqlKeywords := map[string]bool{
		"set": true, "where": true, "and": true, "or": true,
		"select": true, "from": true, "join": true, "on": true,
		"insert": true, "into": true, "update": true, "delete": true,
		"values": true, "null": true, "not": true, "in": true,
		"order": true, "by": true, "asc": true, "desc": true,
		"limit": true, "offset": true, "group": true, "having": true,
		"left": true, "right": true, "inner": true, "outer": true, "cross": true, "full": true, "lateral": true,
		"unnest": true, "generate_series": true,
		"excluded": true, "conflict": true, "do": true,
	}

	// Extract only backtick-delimited strings (SQL queries in this codebase)
	backtickRegex := regexp.MustCompile("`[^`]+`")
	sqlStrings := backtickRegex.FindAllString(content, -1)

	// Regex patterns for SQL table references
	fromRegex := regexp.MustCompile(`(?i)\bFROM\s+(\w+)`)
	joinRegex := regexp.MustCompile(`(?i)\bJOIN\s+(\w+)`)
	insertRegex := regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+(\w+)`)
	updateRegex := regexp.MustCompile(`(?i)\bUPDATE\s+(\w+)`)
	deleteRegex := regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+(\w+)`)

	addTable := func(name string) {
		lower := strings.ToLower(name)
		if !sqlKeywords[lower] {
			tables[lower] = true
		}
	}

	for _, sqlStr := range sqlStrings {
		// Apply all patterns to each SQL string
		for _, match := range fromRegex.FindAllStringSubmatch(sqlStr, -1) {
			addTable(match[1])
		}
		for _, match := range joinRegex.FindAllStringSubmatch(sqlStr, -1) {
			addTable(match[1])
		}
		for _, match := range insertRegex.FindAllStringSubmatch(sqlStr, -1) {
			addTable(match[1])
		}
		for _, match := range updateRegex.FindAllStringSubmatch(sqlStr, -1) {
			addTable(match[1])
		}
		for _, match := range deleteRegex.FindAllStringSubmatch(sqlStr, -1) {
			addTable(match[1])
		}
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result
}

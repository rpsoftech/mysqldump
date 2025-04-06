package mysqldump

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func init() {}

type dumpOption struct {
	// 导出表数据
	isData bool

	// 导出指定表, 与 isAllTables 互斥, isAllTables 优先级高
	tables []string
	// 导出全部表
	isAllTable bool
	// 是否删除表
	isDropTable bool

	// writer 默认为 os.Stdout
	writer io.Writer
}

type DumpOption func(*dumpOption)

// 删除表
func WithDropTable() DumpOption {
	return func(option *dumpOption) {
		option.isDropTable = true
	}
}

// 导出表数据
func WithData() DumpOption {
	return func(option *dumpOption) {
		option.isData = true
	}
}

// 导出指定表, 与 WithAllTables 互斥, WithAllTables 优先级高
func WithTables(tables ...string) DumpOption {
	return func(option *dumpOption) {
		option.tables = tables
	}
}

// 导出全部表
func WithAllTable() DumpOption {
	return func(option *dumpOption) {
		option.isAllTable = true
	}
}

// 导出到指定 writer
func WithWriter(writer io.Writer) DumpOption {
	return func(option *dumpOption) {
		option.writer = writer
	}
}

func Dump(db *sql.DB, dbName string, opts ...DumpOption) error {
	// 打印开始
	start := time.Now()
	// 打印结束
	var err error

	var o dumpOption

	for _, opt := range opts {
		opt(&o)
	}

	if len(o.tables) == 0 {
		// 默认包含全部表
		o.isAllTable = true
	}

	if o.writer == nil {
		// 默认输出到 os.Stdout
		o.writer = os.Stdout
	}

	buf := bufio.NewWriter(o.writer)
	defer buf.Flush()

	// 打印 Header
	_, _ = buf.WriteString("-- ----------------------------\n")
	_, _ = buf.WriteString("-- MySQL Database Dump\n")
	_, _ = buf.WriteString("-- Start Time: " + start.Format("2006-01-02 15:04:05") + "\n")
	_, _ = buf.WriteString("-- ----------------------------\n")
	_, _ = buf.WriteString("\n\n")

	_, err = db.Exec(fmt.Sprintf("USE `%s`", dbName))
	if err != nil {
		return err
	}

	// 2. 获取表
	var tables []string
	if o.isAllTable {
		tmp, err := getAllTables(db)
		if err != nil {
			return err
		}
		tables = tmp
	} else {
		tables = o.tables
	}

	// 3. 导出表
	for _, table := range tables {
		// 删除表
		if o.isDropTable {
			_, _ = buf.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table))
		}

		// 导出表结构
		err = writeTableStruct(db, table, buf)
		if err != nil {
			return err
		}

	}

	if o.isData {
		for _, table := range tables {
			err = writeTableData(db, table, buf)
			if err != nil {
				return err
			}
		}
	}

	// 导出每个表的结构和数据

	_, _ = buf.WriteString("-- ----------------------------\n")
	_, _ = buf.WriteString("-- Dumped by mysqldump\n")
	_, _ = buf.WriteString("-- Cost Time: " + time.Since(start).String() + "\n")
	_, _ = buf.WriteString("-- ----------------------------\n")
	buf.Flush()

	return nil
}

func getCreateTableSQL(db *sql.DB, table string) (string, error) {
	var createTableSQL string
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&table, &createTableSQL)
	if err != nil {
		return "", err
	}
	// IF NOT EXISTS
	createTableSQL = strings.Replace(createTableSQL, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	return createTableSQL, nil
}

func getForeignKeyRelationships(db *sql.DB, table string) ([]string, error) {
	var foreignKeys []string
	query := `
		SELECT TABLE_NAME 
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
		WHERE REFERENCED_TABLE_NAME = ?`
	rows, err := db.Query(query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var dependentTable string
		if err := rows.Scan(&dependentTable); err != nil {
			return nil, err
		}
		foreignKeys = append(foreignKeys, dependentTable)
	}
	return foreignKeys, nil
}

// Sort tables based on foreign key dependencies
func sortTablesByDependency(db *sql.DB, tables []string) ([]string, error) {
	var sortedTables []string
	visited := make(map[string]bool)
	var result []string

	// Use depth-first search (DFS) to traverse the tables in dependency order
	var visit func(table string) error
	visit = func(table string) error {
		if visited[table] {
			return nil
		}
		visited[table] = true

		// Find tables that the current table is referenced by
		foreignKeys, err := getForeignKeyRelationships(db, table)
		if err != nil {
			return err
		}

		// Visit tables that the current table depends on
		for _, foreignKey := range foreignKeys {
			if !visited[foreignKey] {
				if err := visit(foreignKey); err != nil {
					return err
				}
			}
		}

		// After visiting all dependencies, add the current table
		result = append(result, table)
		return nil
	}

	// Visit each table to ensure all dependencies are traversed
	for _, table := range tables {
		if !visited[table] {
			if err := visit(table); err != nil {
				return nil, err
			}
		}
	}

	// Reverse the result list to ensure the correct order
	for i := len(result) - 1; i >= 0; i-- {
		sortedTables = append(sortedTables, result[i])
	}

	return sortedTables, nil
}

func getAllTables(db *sql.DB) ([]string, error) {
	var tables []string
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return sortTablesByDependency(db, tables)
}

func writeTableStruct(db *sql.DB, table string, buf *bufio.Writer) error {
	// 导出表结构
	_, _ = buf.WriteString("-- ----------------------------\n")
	_, _ = buf.WriteString(fmt.Sprintf("-- Table structure for %s\n", table))
	_, _ = buf.WriteString("-- ----------------------------\n")

	createTableSQL, err := getCreateTableSQL(db, table)
	if err != nil {
		return err
	}
	_, _ = buf.WriteString(createTableSQL)
	_, _ = buf.WriteString(";")

	_, _ = buf.WriteString("\n\n")
	_, _ = buf.WriteString("\n\n")
	return nil
}

// 禁止 golangci-lint 检查
// nolint: gocyclo
func writeTableData(db *sql.DB, table string, buf *bufio.Writer) error {
	var totalRow uint64
	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table))
	row.Scan(&totalRow)

	// 导出表数据
	_, _ = buf.WriteString("-- ----------------------------\n")
	_, _ = buf.WriteString(fmt.Sprintf("-- Records of %s (%d Rows)\n", table, totalRow))
	_, _ = buf.WriteString("-- ----------------------------\n")

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return err
	}
	defer rows.Close()

	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return err
	}

	// Generate the column names for the INSERT statement
	columnNames := strings.Join(columns, ",")

	// Write the data for each row
	for rows.Next() {
		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		dataStrings := make([]string, len(columns))

		// Prepare the values
		for key, value := range data {
			if value != nil && value.Valid {
				escaped := strings.ReplaceAll(value.String, "'", "''")
				dataStrings[key] = "'" + escaped + "'"
			} else {
				dataStrings[key] = "NULL"
			}
		}
		

		// Insert statement with column names
		buf.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n", table, columnNames, strings.Join(dataStrings, ",")))
	}

	_, _ = buf.WriteString("\n\n")
	return nil
}

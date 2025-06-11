package mysqldump

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"slices"
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

	views []string
	// 导出全部表
	isAllTable bool
	// 是否删除表
	isDropTable     bool
	isDropView      bool
	isAllViews      bool
	withUseDatabase bool
	withTransaction bool
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
func WithDropViews() DumpOption {
	return func(option *dumpOption) {
		option.isDropView = true
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
func WithViews(views ...string) DumpOption {
	return func(option *dumpOption) {
		option.views = views
	}
}

// 导出全部表
func WithAllTable() DumpOption {
	return func(option *dumpOption) {
		option.isAllTable = true
	}
}
func WithUseDatabase() DumpOption {
	return func(option *dumpOption) {
		option.withUseDatabase = true
	}
}
func WithTransaction() DumpOption {
	return func(option *dumpOption) {
		option.withTransaction = true
	}
}

func WithAllViews() DumpOption {
	return func(option *dumpOption) {
		option.isAllViews = true
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

	if len(o.views) == 0 && !o.isAllViews {
		// 默认包含全部表
		o.isAllViews = false
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
	_, _ = buf.WriteString("-- Database Name: " + dbName + "\n")
	_, _ = buf.WriteString("-- ----------------------------\n")
	if o.withTransaction {
		_, _ = buf.WriteString("SET AUTOCOMMIT=0;\n")
		_, _ = buf.WriteString("START TRANSACTION;\n\n")
	}
	if o.withUseDatabase {
		_, _ = buf.WriteString(fmt.Sprintf("USE `%s`;\n\n", dbName))
	}
	_, _ = buf.WriteString("SET FOREIGN_KEY_CHECKS=0;\n\n")
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

	var views []string

	tmp, err := getAllViews(db)
	//Remove views from tables
	for _, view := range tmp {
		index := slices.Index(tables, view)
		if index != -1 {
			// Remove the element at the found index
			tables = slices.Delete(tables, index, index+1)
		}

	}
	if o.isAllViews {
		if err != nil {
			return err
		}
		views = tmp
	} else {
		views = o.views
	}

	allTotalRows := uint64(0)
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
		if o.isData {
			_, _ = buf.WriteString(fmt.Sprintf("LOCK TABLES `%s` WRITE; \n\n", table))
			totalRows, err := writeTableData(db, table, buf)
			_, _ = buf.WriteString("UNLOCK TABLES;\n\n")
			allTotalRows += totalRows
			if err != nil {
				return err
			}
		}
	}
	// Committing transaction so Views Can Be Defined Without Issues
	if o.withTransaction {
		_, _ = buf.WriteString("COMMIT;\n")
		_, _ = buf.WriteString("SET AUTOCOMMIT=1;\n")
	}
	// 4. Views

	for _, view := range views {
		// 删除表
		if o.isDropView {
			_, _ = buf.WriteString(fmt.Sprintf("DROP VIEW IF EXISTS `%s`;\n", view))
		}

		// 导出表结构
		err = writeTableStruct(db, view, buf)
		if err != nil {
			return err
		}
	}

	// Again Starting Transaction For Data Insertion
	if o.withTransaction {
		_, _ = buf.WriteString("SET AUTOCOMMIT=0;\n")
		_, _ = buf.WriteString("START TRANSACTION;\n\n")
	}

	// 导出每个表的结构和数据
	_, _ = buf.WriteString("SET FOREIGN_KEY_CHECKS=1;\n")
	if o.withTransaction {
		_, _ = buf.WriteString("COMMIT;\n")
		_, _ = buf.WriteString("SET AUTOCOMMIT=1;\n")
	}
	_, _ = buf.WriteString("-- ----------------------------\n")
	_, _ = buf.WriteString("-- Dumped by mysqldump\n")
	_, _ = buf.WriteString("-- Maintained by Yusta (https://github.com/NotYusta)\n")
	_, _ = buf.WriteString("-- Cost Time: " + time.Since(start).String() + "\n")
	_, _ = buf.WriteString("-- Complete Time: " + time.Now().Format("2006-01-02 15:04:05") + "\n")
	_, _ = buf.WriteString("-- Table Counts: " + fmt.Sprintf("%d", len(tables)) + "\n")
	_, _ = buf.WriteString("-- Table Rows: " + fmt.Sprintf("%d", allTotalRows) + "\n")
	_, _ = buf.WriteString("-- ----------------------------\n")
	buf.Flush()

	return nil
}

func getCreateTableSQL(db *sql.DB, table string) (string, error) {
	var createTableSQL string

	rows, err := db.Query(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	if err != nil {
		return "", err
	}
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	} else if len(columns) < 2 {
		return "", fmt.Errorf("less then 2 columns found on querying table %s", table)
	}
	extras := make([]any, len(columns))
	extras[1] = &createTableSQL
	extras[0] = &table
	if !rows.Next() {
		return "", fmt.Errorf("table %s not found", table)
	}
	var notNeededData string
	if len(columns) > 2 {
		for i := 2; i < len(columns); i++ {
			extras[i] = &notNeededData
		}
	}
	err = rows.Scan(extras...)
	if err != nil {
		return "", err
	}
	rows.Close()
	// IF NOT EXISTS
	createTableSQL = strings.Replace(createTableSQL, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	return createTableSQL, nil
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

	return tables, nil
}
func getAllViews(db *sql.DB) ([]string, error) {
	var views []string
	rows, err := db.Query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'VIEW'")
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
		views = append(views, table)
	}
	return views, nil
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
	_, _ = buf.WriteString(fmt.Sprintf("%s;\n\n", createTableSQL))
	return nil
}

// 禁止 golangci-lint 检查
// nolint: gocyclo
func writeTableData(db *sql.DB, table string, buf *bufio.Writer) (uint64, error) {
	var totalRow uint64
	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table))
	row.Scan(&totalRow)

	// 导出表数据
	_, _ = buf.WriteString("-- ----------------------------\n")
	_, _ = buf.WriteString(fmt.Sprintf("-- Records of %s (%d Rows)\n", table, totalRow))
	_, _ = buf.WriteString("-- ----------------------------\n")

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return totalRow, err
	}
	defer rows.Close()

	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return totalRow, err
	}

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = "`" + col + "`"
	}

	columnNames := strings.Join(quotedColumns, ",")

	if totalRow > 0 {
		dataValueString := []string{}
		rowNumber := 0
		for rows.Next() {
			data := make([]*sql.NullString, len(columns))
			ptrs := make([]interface{}, len(columns))
			for i := range data {
				ptrs[i] = &data[i]
			}

			// Read data
			if err := rows.Scan(ptrs...); err != nil {
				return totalRow, err
			}

			dataStrings := make([]string, len(columns))
			for key, value := range data {
				if value != nil && value.Valid {
					escaped := strings.ReplaceAll(value.String, "'", "''")
					dataStrings[key] = "'" + escaped + "'"
				} else {
					dataStrings[key] = "NULL"
				}
			}
			dataValueString = append(dataValueString, "("+strings.Join(dataStrings, ",")+")")
			rowNumber += 1
			if rowNumber >= 600 {
				writeDataInsertToBuffer(table, columnNames, dataValueString, buf)
				rowNumber = 0
				dataValueString = []string{}
			}
		}
		if rowNumber > 0 {
			writeDataInsertToBuffer(table, columnNames, dataValueString, buf)
		}
	}

	_, _ = buf.WriteString("\n")
	return totalRow, nil
}

func writeDataInsertToBuffer(table string, columnNames string, dataValueString []string, buf *bufio.Writer) {
	s := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s;\n", table, columnNames, strings.Join(dataValueString, ","))
	s = strings.ReplaceAll(s, "\\'", "\\\\'")
	// s = strings.ReplaceAll(s, "')", "`)")
	// s = strings.ReplaceAll(s, "',", "`,")
	// s = strings.ReplaceAll(s, ",'", ",`")
	buf.WriteString(s)
}

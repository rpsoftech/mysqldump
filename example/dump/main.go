package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/NotYusta/mysqldump"
)

func main() {
	dsn := "root:rootpasswd@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=true&loc=Asia%2FJakarta"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("[error] %v \n", err)
		return
	}

	defer db.Close()
	f, _ := os.Create("dump.sql")

	_ = mysqldump.Dump(
		db,                           // DSN
		mysqldump.WithDropTable(),    // Option: Delete table before create (Default: Not delete table)
		mysqldump.WithData(),         // Option: Dump Data (Default: Only dump table schema)
		mysqldump.WithTables("test"), // Option: Dump Tables (Default: All tables)
		mysqldump.WithWriter(f),      // Option: Writer (Default: os.Stdout)
	)
}

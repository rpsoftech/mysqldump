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

	f, _ := os.Open("dump.sql")

	_ = mysqldump.Source(
		db,
		"test",
		f,
		mysqldump.WithMergeInsert(1000), // Option: Merge insert 1000 (Default: Not merge insert)
		mysqldump.WithDebug(),           // Option: Print execute sql (Default: Not print execute sql)
	)
}

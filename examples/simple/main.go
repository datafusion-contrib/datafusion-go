package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/datafusion-contrib/datafusion-go"
)

func main() {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var one int64
	if err := db.QueryRowContext(context.Background(), "select 1").Scan(&one); err != nil {
		log.Fatal(err)
	}

	fmt.Println(one)
}

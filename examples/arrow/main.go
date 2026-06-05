package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"

	"github.com/datafusion-contrib/datafusion-go"
)

func main() {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	reader, err := datafusion.QueryArrowContext(ctx, conn, "select 1 as value union all select 2")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = reader.Close()
	}()

	fmt.Println(reader.Schema())
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record.NumRows())
		record.Release()
	}
}

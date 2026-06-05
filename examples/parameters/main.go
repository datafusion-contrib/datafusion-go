package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/datafusion-contrib/datafusion-go"
)

func main() {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	day := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	row := db.QueryRowContext(
		ctx,
		"select $1, $2, $3, $4, $5",
		datafusion.DateFromTime(day),
		datafusion.TimeNanos(10*time.Second.Nanoseconds()),
		datafusion.DurationFromTime(2*time.Second),
		datafusion.DecimalString("123.45", 10, 2),
		datafusion.NullOf(datafusion.ParameterInt64),
	)

	var date time.Time
	var clock time.Time
	var durationNanos int64
	var decimal string
	var nullable sql.NullInt64
	if err := row.Scan(&date, &clock, &durationNanos, &decimal, &nullable); err != nil {
		log.Fatal(err)
	}

	fmt.Println(date.Format("2006-01-02"), clock.Format("15:04:05"), durationNanos, decimal, nullable.Valid)
}

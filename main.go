package main

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"os"
	"time"
)

var MESSAGE_RATE = 20 * time.Millisecond
var MESSAGE_STRING = `{"x":1.23456789,"y":1.23456789,"dx":0.12345678,"dy":0.12345678,"dtheta":-0.12345678}`
var NUM_SIGNALS = 60

type message struct {
	SignalId  int64
	Timestamp time.Time
	Content   string
}

func now() time.Time {
	return time.Now().UTC()
}

// This function runs forever, emitting data on the provided channel.
// Must be run in it own goroutine.
func emit(output chan []message) {
	for true {
		var chunk = make([]message, NUM_SIGNALS)
		for i := 0; i < NUM_SIGNALS; i++ {
			chunk[i] = message{
				SignalId:  int64(i),
				Timestamp: now(),
				Content:   MESSAGE_STRING,
			}
		}

		output <- chunk

		time.Sleep(MESSAGE_RATE)
	}
}

// Consumes message from the channel and writes messages to the database.
// Must be run in its own goroutine.
// https://godoc.org/github.com/lib/pq#hdr-Bulk_imports
func record(input chan []message, db *sql.DB) {
	for chunk := range input {
		txn, err := db.Begin()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		stmt, err := txn.Prepare(pq.CopyIn("points", "signal_id", "timestamp", "content"))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		for _, message := range chunk {
			_, err = stmt.Exec(message.SignalId, message.Timestamp, message.Content)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		err = stmt.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		err = txn.Commit()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}

func main() {
	var db *sql.DB
	var err error
	db, err = sql.Open("postgres", "user=metrics_server dbname=metrics_server sslmode=disable")

	db.Exec(`DROP TABLE points`)

	res, err := db.Exec(`CREATE TABLE points (
		signal_id integer,
		timestamp timestamp,
		content   jsonb,
		PRIMARY KEY(signal_id, timestamp)
		)
		`)
	if err != nil {
		fmt.Printf("%#v", res)
		fmt.Println(err)
		os.Exit(1)
	}

	var message_buffer = make(chan []message, 5000)

	go emit(message_buffer)

	go record(message_buffer, db)

	time.Sleep(20 * time.Second)

	var max = time.Duration(0)

	for times := 0; times < 8; times++ { // run for 10 times minutes
		for i := 0; i < NUM_SIGNALS; i++ {
			rows, err := db.Query("SELECT timestamp FROM points WHERE signal_id = $1 ORDER BY timestamp DESC LIMIT 1000 ", i)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			var most_recent time.Time
			if rows.Next() {
				err = rows.Scan(&most_recent)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				var d = now().Sub(most_recent)
				if d > max {
					max = d
				}
				fmt.Printf("\rCurrent delay: %v - Max delay: %v             ", d, max)
			} else {
				fmt.Println("no data")
			}
			rows.Close()

			time.Sleep(10 * time.Second)
		}
	}
	fmt.Println()
	os.Exit(0)
}

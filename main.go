package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/dukky/toydb/db"
	"github.com/dukky/toydb/hashkv"
	logdb "github.com/dukky/toydb/log"
)

func main() {
	dbFile := flag.String("file", "test.bin", "The path to the database file.")
	dbType := flag.String("type", "log", "The type of database to use (log or hash).")
	op := flag.String("op", "write", "The operation to perform (read or write).")
	key := flag.String("key", "", "The key for the operation.")
	value := flag.String("value", "", "The value for the write operation.")

	flag.Parse()

	var d db.DB

	switch *dbType {
	case "log":
		d = logdb.NewLog(*dbFile)
	case "hash":
		d = hashkv.NewHashKV(*dbFile)
	default:
		log.Fatalf("Unknown database type: %s", *dbType)
	}

	switch *op {
	case "write":
		if *key == "" || *value == "" {
			log.Fatal("Both key and value must be specified for a write operation.")
		}
		err := d.Write(*key, *value)
		if err != nil {
			log.Fatalf("Error writing to database: %v", err)
		}
		fmt.Println("Write successful.")
	case "read":
		if *key == "" {
			log.Fatal("Key must be specified for a read operation.")
		}
		val, err := d.Read(*key)
		if err != nil {
			log.Fatalf("Error reading from database: %v", err)
		}
		fmt.Printf("Read value: %s\n", val)
	default:
		log.Fatalf("Unknown operation: %s", *op)
	}
}

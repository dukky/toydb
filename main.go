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
	op := flag.String("op", "write", "The operation to perform (read, write, delete, or compact).")
	key := flag.String("key", "", "The key for the operation.")
	value := flag.String("value", "", "The value for the write operation.")

	flag.Parse()

	var d db.DB

	switch *dbType {
	case "log":
		var err error
		d, err = logdb.NewLog(*dbFile)
		if err != nil {
			log.Fatalf("Error initializing log database: %v", err)
		}
		// Ensure the database lock is released when we're done
		defer func() {
			if logDB, ok := d.(*logdb.Log); ok {
				if err := logDB.Close(); err != nil {
					log.Printf("Warning: error closing database: %v", err)
				}
			}
		}()
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
	case "delete":
		if *key == "" {
			log.Fatal("Key must be specified for a delete operation.")
		}
		err := d.Delete(*key)
		if err != nil {
			log.Fatalf("Error deleting from database: %v", err)
		}
		fmt.Println("Delete successful.")
	case "compact":
		// Compact is only supported for log databases
		if *dbType != "log" {
			log.Fatalf("Compact operation is only supported for log databases.")
		}
		logDB, ok := d.(*logdb.Log)
		if !ok {
			log.Fatal("Failed to get log database instance.")
		}
		err := logDB.Compact()
		if err != nil {
			log.Fatalf("Error compacting database: %v", err)
		}
		fmt.Println("Compact successful.")
	default:
		log.Fatalf("Unknown operation: %s", *op)
	}
}

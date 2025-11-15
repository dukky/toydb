package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/dukky/toydb/db"
	"github.com/dukky/toydb/hashkv"
	logdb "github.com/dukky/toydb/log"
	"github.com/dukky/toydb/sstable"
)

func main() {
	dbFile := flag.String("file", "test.bin", "The path to the database file or directory.")
	dbType := flag.String("type", "log", "The type of database to use (log, hash, or sstable).")
	op := flag.String("op", "write", "The operation to perform (read, write, delete, compact, or flush).")
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
	case "hash":
		d = hashkv.NewHashKV(*dbFile)
	case "sstable":
		var err error
		d, err = sstable.NewSSTableDB(*dbFile)
		if err != nil {
			log.Fatalf("Error initializing SSTable database: %v", err)
		}
		// Close SSTable DB when done to flush memtable
		defer func() {
			if sstDB, ok := d.(*sstable.SSTableDB); ok {
				if err := sstDB.Close(); err != nil {
					log.Printf("Error closing SSTable database: %v", err)
				}
			}
		}()
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
		// Compact is supported for log and sstable databases
		switch *dbType {
		case "log":
			logDB, ok := d.(*logdb.Log)
			if !ok {
				log.Fatal("Failed to get log database instance.")
			}
			err := logDB.Compact()
			if err != nil {
				log.Fatalf("Error compacting database: %v", err)
			}
			fmt.Println("Compact successful.")
		case "sstable":
			sstDB, ok := d.(*sstable.SSTableDB)
			if !ok {
				log.Fatal("Failed to get SSTable database instance.")
			}
			err := sstDB.Compact()
			if err != nil {
				log.Fatalf("Error compacting database: %v", err)
			}
			fmt.Println("Compact successful.")
		default:
			log.Fatalf("Compact operation is not supported for %s databases.", *dbType)
		}
	case "flush":
		// Flush is only supported for SSTable databases
		if *dbType != "sstable" {
			log.Fatalf("Flush operation is only supported for sstable databases.")
		}
		sstDB, ok := d.(*sstable.SSTableDB)
		if !ok {
			log.Fatal("Failed to get SSTable database instance.")
		}
		err := sstDB.Flush()
		if err != nil {
			log.Fatalf("Error flushing database: %v", err)
		}
		fmt.Println("Flush successful.")
	default:
		log.Fatalf("Unknown operation: %s", *op)
	}
}

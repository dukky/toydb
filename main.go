package main

import (
	"fmt"
	"log"

	logdb "github.com/dukky/toydb/log"
)

func main() {
	db := logdb.NewLog("test.bin")

	err := db.Write("Hello", "you")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Write succeded")

	data, err := db.Read("Hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Read: ", data)
}

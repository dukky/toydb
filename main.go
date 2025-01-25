package main

import (
	"fmt"
	"log"

	"github.com/dukky/toydb/hashkv"
)

func main() {
	db := hashkv.NewHashKV("test.bin")

	err := db.Write("Goodbye", "world")
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

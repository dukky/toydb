package main

import (
	"fmt"

	"github.com/dukky/toydb/log"
)

func main() {
	db := log.NewLog("test.log")

	db.Write("Hello", "world\nnewline")
	db.Write("Goodbye", "world")
	db.Write("Hello1", "universe")
	db.Write("Hello2", "everyone")

	value, _ := db.Read("Hello")
	println("Hello:", value)

	value, _ = db.Read("Goodbye")
	println("Goodbye:", value)

	fmt.Printf("%#v\n", db.Index)
}

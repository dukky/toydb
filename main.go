package main

import (
	"github.com/dukky/toydb/db"
	"github.com/dukky/toydb/log"
)

func main() {
	var db db.DB = log.NewLog("test.log")

	db.Write("Hello", "world")
	db.Write("Goodbye", "world")

	value, _ := db.Read("Hello")
	println("Hello:", value)

	value, _ = db.Read("Goodbye")
	println("Goodbye:", value)
}

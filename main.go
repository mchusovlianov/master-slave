package main

import (
	"flag"

	"github.com/satori/go.uuid"
)

var port = flag.String("port", ":8080", "port number")

func main() {
	flag.Parse()

	uuid := uuid.NewV4().String()
	node := Node{UUID: uuid, State: "undefined", CommandPort: *port}
	node.Init()
}

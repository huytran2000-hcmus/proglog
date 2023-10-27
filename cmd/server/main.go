package main

import (
	"log"

	"github.com/huytran2000-hcmus/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}

package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	dir := os.Args[1]
	bind := ":8080"
	fmt.Printf("Serving `%s' on `%s'\n", dir, bind)
	panic(http.ListenAndServe(bind, http.FileServer(http.Dir(dir))))
}

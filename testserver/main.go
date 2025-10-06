package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Microsecond)
	})
	fmt.Println("Starting test server on :8080")
	http.ListenAndServe(":8080", nil)
}

package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10000 * time.Microsecond)

		// write a simple response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello, World! This is a test server.\n  wfwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwefwef\n"))

	})
	fmt.Println("Starting test server on :8080")
	http.ListenAndServe(":8080", nil)
}

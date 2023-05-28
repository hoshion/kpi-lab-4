package main

import (
	"flag"
	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
	"log"
	"net/http"
)

var port = flag.Int("port", 8083, "server port")

func main() {
	h := new(http.ServeMux)

	h.HandleFunc("/db/", func(res http.ResponseWriter, req *http.Request) {
		log.Println("caught request")
		url := req.URL.String()
		key := url[4:]
		log.Printf("key: %s", key)

		switch req.Method {
		case "GET":
			log.Printf("Caught get request")
			res.WriteHeader(http.StatusOK)
		case "POST":
			log.Printf("Cuahgt post request")
			res.WriteHeader(http.StatusCreated)
		default:
			res.WriteHeader(http.StatusBadRequest)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

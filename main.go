package main

import (
	"net/http"
	"fmt"
	"strings"
)

func formatRequest(r *http.Request) string {
	var request []string

	url := fmt.Sprintf("%v %v %v", r.Method, r.URL, r.Proto)
	request = append(request, url)
	request = append(request, fmt.Sprintf("Host: %v", r.Host))
	for name, headers := range r.Header {
		name = strings.ToLower(name)
		for _, h := range headers {
			request = append(request, fmt.Sprintf("%v: %v", name, h))
		}
	}

	if r.Method == "POST" {
		r.ParseForm()
		request = append(request, "\n")
		request = append(request, r.Form.Encode())
	}

	return strings.Join(request, "\n")
}

func ProcessRequest(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(formatRequest(req)))
}


func main() {
	http.HandleFunc("/", ProcessRequest)

	err := http.ListenAndServeTLS(":443", "server.crt", "server.key", nil)
	if err != nil {
		fmt.Println("ListenAndServe: ", err)
	}
}



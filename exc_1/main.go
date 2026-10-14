package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type User struct {
	Name    string `json:"name"`
	Course  string `json:"course"`
	Message string `json:"message"`
}

func getUser(w http.ResponseWriter, r *http.Request) {
	student := User{
		Name:    "Luka Vujičić",
		Course:  "Software Architectures for Big Data",
		Message: "REST API running",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(student)
}

func main() {
	http.HandleFunc("/user", getUser)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

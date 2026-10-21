package main

import "fmt"

type student struct {
	name    string
	course  string
	message string
}

func (s student) printDetails() {
	fmt.Printf("%s\n", s.message)
	fmt.Printf("student: %s | course: %s\n", s.name, s.course)
}

func main() {
	luka := student{
		name:    "luka vujicic",
		course:  "software architectures for big data",
		message: "hello world!",
	}

	luka.printDetails()
}
package main

import "fmt"

type user struct {
	name   string
	course string
}

func (u user) printDetails() {
	fmt.Println("hello, world!")
	fmt.Printf("student: %s\ncourse: %s\n", u.name, u.course)
}

func main() {
	student := user{
		name:   "Luka Vujičić",
		course: "Software Architectures for Big Data",
	}
	student.printDetails()
}

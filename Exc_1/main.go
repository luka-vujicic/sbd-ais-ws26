package main

import "fmt"

type Person struct {
	Name string
	Age  int
}

func (p Person) Print() {
	fmt.Println("yo, I am", p.Name, "and I am", p.Age, "years old")
}

func main() {
	p := Person{Name: "Jasur", Age: 21}
	p.Print()
}

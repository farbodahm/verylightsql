package main

import (
	"fmt"
)

const (
	VERSION = "0.1.0"
)

func main() {
	fmt.Printf("Verylightsql v%s\n", VERSION)

	for {
		fmt.Print("> ")

		var input string
		_, err := fmt.Scanln(&input)
		if err != nil {
			panic(err)
		}

		switch input {
		case ".exit":
			fmt.Print("Bye!\n")
			return
		case ".help":
			fmt.Print("Available commands: help, exit\n")
		default:
			fmt.Printf("Unrecognized command: %s\n", input)
		}

	}

}

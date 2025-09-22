package main

import (
	"bufio"
	"fmt"
	"os"
)

const (
	VERSION = "0.1.0"
)

func execute_meta_command(input string) error {
	switch input {
	case ".exit":
		fmt.Print("Bye!\n")
		// TODO: What is the best way to exit?
		os.Exit(0)
	case ".help":
		fmt.Print("Available commands: help, exit\n")
	default:
		return fmt.Errorf("unrecognized command: %s", input)
	}
	return nil
}

func execute_statement(stmt Statement) error {
	switch stmt.Type {
	case STATEMENT_INSERT:
		fmt.Print("This is where we would do an insert.\n")
		fmt.Printf("We would insert: (%d, %s, %s)\n", stmt.RowToInsert.ID, stmt.RowToInsert.Username, stmt.RowToInsert.Email)
	case STATEMENT_SELECT:
		fmt.Print("This is where we would do a select.\n")
	}
	return nil
}

func main() {
	fmt.Printf("Verylightsql v%s\n", VERSION)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		input, err := reader.ReadString('\n')
		if err != nil {
			panic(err)
		}

		if input[0] == '.' {
			err := execute_meta_command(input)
			if err != nil {
				fmt.Printf("%s\n", err)
			}
			continue
		}

		stmt, err := prepare_statement(input)
		if err != nil {
			fmt.Printf("%s\n", err)
			continue
		}

		err = execute_statement(stmt)
		if err != nil {
			fmt.Printf("%s\n", err)
			continue
		}
	}

}

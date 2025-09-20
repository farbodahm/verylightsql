package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const (
	VERSION = "0.1.0"
)

// StatementType represents the type of SQL statement
type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

// Statement represents a SQL statement
type Statement struct {
	Type StatementType
}

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

func prepare_statement(input string) (Statement, error) {
	var stmt Statement

	action := strings.Split(input, " ")[0]

	switch action {
	case "insert":
		stmt.Type = STATEMENT_INSERT
	case "select":
		stmt.Type = STATEMENT_SELECT
	default:
		return stmt, fmt.Errorf("unrecognized keyword at start of '%s'", input)
	}

	return stmt, nil
}

func execute_statement(stmt Statement) error {
	switch stmt.Type {
	case STATEMENT_INSERT:
		fmt.Print("This is where we would do an insert.\n")
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

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

func executeInsert(stmt Statement, table *Table) error {
	return table.Insert(&stmt.RowToInsert)
}

func executeSelect(stmt Statement, table *Table) error {
	rows := table.SelectAll()
	for _, row := range rows {
		printRow(&row)
	}
	return nil
}

func printRow(row *Row) {
	// Convert byte arrays to strings for display
	username := string(row.Username[:])
	email := string(row.Email[:])

	// Trim null bytes for display
	for i, b := range row.Username {
		if b == 0 {
			username = string(row.Username[:i])
			break
		}
	}
	for i, b := range row.Email {
		if b == 0 {
			email = string(row.Email[:i])
			break
		}
	}

	fmt.Printf("(%d, %s, %s)\n", row.ID, username, email)
}

func execute_statement(stmt Statement, table *Table) error {
	switch stmt.Type {
	case STATEMENT_INSERT:
		return executeInsert(stmt, table)
	case STATEMENT_SELECT:
		return executeSelect(stmt, table)
	}
	return nil
}

func main() {
	fmt.Printf("Verylightsql v%s\n", VERSION)
	table := NewTable()
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		input, err := reader.ReadString('\n')
		input = input[:len(input)-1] // remove newline
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
			fmt.Printf("%s.\n", err)
			continue
		}

		err = execute_statement(stmt, table)
		if err != nil {
			fmt.Printf("%s\n", err)
			continue
		}
		fmt.Println("Executed.")
	}
}

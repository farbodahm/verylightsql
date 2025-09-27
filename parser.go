package main

import (
	"fmt"
	"strings"
)

// StatementType represents the type of SQL statement
type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

// Statement represents a SQL statement
type Statement struct {
	Type        StatementType
	RowToInsert Row // only used by insert statement
}

const (
	ColumnUsernameSize = 32
	ColumnEmailSize    = 255
)

// TODO: this should be a generic implementation
type Row struct {
	ID       int32
	Username [ColumnUsernameSize]byte // TODO: what if we use string? How does DBs manage sparse part?
	Email    [ColumnEmailSize]byte
}

// parse_insert_string_to_row parses a string input into a Row struct
// Expects input in the format: "insert <id> <username> <email>"
func parse_insert_string_to_row(input string) (Row, error) {
	var row Row
	var username, email string
	_, err := fmt.Sscanf(input, "insert %d %s %s", &row.ID, &username, &email)
	if err != nil {
		return row, fmt.Errorf("syntax error: could not parse row: %w", err)
	}

	// TODO: Handle overflow
	for i := 0; i < len(username) && i < ColumnUsernameSize; i++ {
		row.Username[i] = username[i]
	}
	for i := 0; i < len(email) && i < ColumnEmailSize; i++ {
		row.Email[i] = email[i]
	}

	return row, nil
}

func prepare_statement(input string) (Statement, error) {
	var stmt Statement

	action := strings.Split(input, " ")[0]

	switch action {
	case "insert":
		row, err := parse_insert_string_to_row(input)
		if err != nil {
			return stmt, err
		}
		stmt.RowToInsert = row
		stmt.Type = STATEMENT_INSERT

	case "select":
		stmt.Type = STATEMENT_SELECT
	default:
		return stmt, fmt.Errorf("unrecognized keyword at start of '%s'", input)
	}

	return stmt, nil
}

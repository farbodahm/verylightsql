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
	Type StatementType
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

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/kong"
)

const (
	VERSION = "0.1.0"
)

var CLI struct {
	DBPath  string `arg:"" name:"database_file" help:"Path to the database file." default:"vlsql.db"`
	Version bool   `help:"Print version and exit." short:"v"`
}

func execute_meta_command(input string, t *Table) error {
	switch input {
	case ".exit":
		fmt.Print("Bye!\n")
		t.Close()
		os.Exit(0)
	case ".help":
		fmt.Print("Available commands: help, exit\n")
	case ".constants":
		printConstants()
	case ".btree":
		printTree(t.pager, 0, 0)
	default:
		return fmt.Errorf("unrecognized command: %s", input)
	}
	return nil
}

func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", rowSize)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", CommonHeaderSize)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LeafNodeHeaderSize)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNodeCellSize)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LeafNodeSpaceForCells)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNodeMaxCells)
}

func indent(level int) {
	for range level {
		fmt.Print("  ")
	}
}

func printTree(pager *Pager, pageNum uint32, indentationLevel int) {
	page, err := pager.getPage(pageNum)
	if err != nil {
		panic(err)
	}
	var numKeys, child uint32

	switch *nodeType(page) {
	case NodeTypeLeaf:
		numKeys = *leafNodeNumCells(page)
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentationLevel + 1)
			fmt.Printf("- %d\n", *leafNodeKey(page, i))
		}
	case NodeTypeInternal:
		numKeys = *internalNodeNumKeys(page)
		indent(indentationLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child = *internalNodeChild(page, i)
			printTree(pager, child, indentationLevel+1)

			indent(indentationLevel + 1)
			fmt.Printf("- key %d\n", *internalNodeKey(page, i))
		}
		child = *internalNodeRightChild(page)
		printTree(pager, child, indentationLevel+1)
	default:
		panic("Unrecognized node type")
	}
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
	ctx := kong.Parse(&CLI,
		kong.Name("verylightsql"),
		kong.Description("A tiny SQL-like REPL."),
		kong.Vars{"version": VERSION},
	)

	if CLI.Version {
		fmt.Printf("Verylightsql v%s\n", VERSION)
		ctx.Exit(0)
	}

	fmt.Printf("Verylightsql v%s\n", VERSION)
	fmt.Printf("Opening database: %s\n", CLI.DBPath)

	table, err := OpenDatabase(CLI.DBPath)
	if err != nil {
		fmt.Printf("Error opening database file: %s\n", err)
		os.Exit(1)
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		// Trim both \n and potential \r\n on Windows
		input = strings.TrimSuffix(input, "\n")
		input = strings.TrimSuffix(input, "\r")

		// Ignore empty lines safely (prevents out-of-range on input[0])
		if len(strings.TrimSpace(input)) == 0 {
			continue
		}

		if input[0] == '.' {
			if err := execute_meta_command(input, table); err != nil {
				fmt.Printf("%s\n", err)
			}
			continue
		}

		stmt, err := prepare_statement(input)
		if err != nil {
			fmt.Printf("%s.\n", err)
			continue
		}

		if err := execute_statement(stmt, table); err != nil {
			fmt.Printf("Error: %s.\n", err)
			continue
		}
		fmt.Println("Executed.")
	}
}

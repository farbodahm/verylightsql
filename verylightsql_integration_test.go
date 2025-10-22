//go:build integration
// +build integration

package main_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	verylightsqlVersion    = "0.1.0"
	integrationTestTimeout = 3 * time.Second
	verylightsqlBinaryName = "verylightsql"
	verylightsqlDBName     = "vlsql-test.db"
)

var verylightsqlBinary string

func init() {
	// Make the binary path absolute so we can run it from temp dirs.
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	verylightsqlBinary = filepath.Join(cwd, verylightsqlBinaryName)
}

// runScript runs the verylightsql binary in the specified working directory with the provided commands as input.
func runScript(t *testing.T, workdir string, commands []string) (lines []string, all string, code int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), integrationTestTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, verylightsqlBinary, verylightsqlDBName)
	cmd.Dir = workdir

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	go func() {
		w := bufio.NewWriter(stdin)
		for _, c := range commands {
			_, _ = w.WriteString(c + "\n")
		}
		_ = w.Flush()
		_ = stdin.Close()
	}()

	_ = cmd.Wait()

	all = stdout.String() + stderr.String()
	all = strings.ReplaceAll(all, "\r\n", "\n")
	all = strings.TrimRight(all, "\n")

	if all != "" {
		lines = strings.Split(all, "\n")
	}

	if ps := cmd.ProcessState; ps != nil {
		code = ps.ExitCode()
	} else {
		code = -1
	}
	return
}

// assertLinesCmp compares got vs want using go-cmp and includes the full transcript on failure.
func assertLinesCmp(t *testing.T, got, want []string, full string) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s\nfull out:\n%s", diff, full)
	}
}

// mustRunAndAssert runs the script, asserts exit code 0, and compares lines via assertLinesCmp.
func mustRunAndAssert(t *testing.T, dir string, script, want []string) {
	t.Helper()
	out, full, code := runScript(t, dir, script)
	if code != 0 {
		t.Fatalf("%s: unexpected exit code %d; output:\n%s", t.Name(), code, full)
	}
	assertLinesCmp(t, out, want, full)
}

// wantWithHeader prepends the common header to test-specific expected lines.
func wantWithHeader(lines ...string) []string {
	headerLines := []string{
		fmt.Sprintf("Verylightsql v%s", verylightsqlVersion),
		fmt.Sprintf("Opening database: %s", verylightsqlDBName),
	}
	return append(headerLines, lines...)
}

func Test_InsertsAndRetrievesRow(t *testing.T) {
	dir := t.TempDir()

	want := wantWithHeader(
		"> Executed.",
		"> (1, user1, person1@example.com)",
		"Executed.",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, []string{
		"insert 1 user1 person1@example.com",
		"select",
		".exit",
	}, want)
}

func Test_PersistsDataAfterClose(t *testing.T) {
	dir := t.TempDir()

	want1 := wantWithHeader(
		"> Executed.",
		"> Bye!",
	)
	mustRunAndAssert(t, dir, []string{
		"insert 1 user1 person1@example.com",
		".exit",
	}, want1)

	want2 := wantWithHeader(
		"> (1, user1, person1@example.com)",
		"Executed.",
		"> Bye!",
	)
	mustRunAndAssert(t, dir, []string{
		"select",
		".exit",
	}, want2)
}

func Test_InsertMaxLengthStrings(t *testing.T) {
	dir := t.TempDir()

	longUsername := strings.Repeat("a", 32)
	longEmail := strings.Repeat("a", 255)

	script := []string{
		fmt.Sprintf("insert 1 %s %s", longUsername, longEmail),
		"select",
		".exit",
	}

	want := wantWithHeader(
		"> Executed.",
		fmt.Sprintf("> (1, %s, %s)", longUsername, longEmail),
		"Executed.",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, script, want)
}

func Test_TableFullError(t *testing.T) {
	dir := t.TempDir()

	// Table max rows is 4096 (pageSize=4096, rowSize=292, rowsPerPage=14, tableMaxPages=100, tableMaxRows=1400)
	// So we try to insert 1401 rows to trigger the error
	script := make([]string, 0, 1402)
	for i := 1; i <= 1401; i++ {
		script = append(script, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	script = append(script, ".exit")

	out, full, code := runScript(t, dir, script)

	// Expect the process to panic and exit with non-zero code
	if code == 0 {
		t.Fatalf("expected process to panic (non-zero exit code), but got exit code 0; output:\n%s", full)
	}

	// Verify the panic message is in the output
	if len(out) < 2 {
		t.Fatalf("output too short:\n%q", out)
	}
	found := false
	for _, line := range out {
		if strings.Contains(line, "Need to implement updating parent after split") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected panic message 'Need to implement updating parent after split' in output, but did not find it.\nExit code: %d\nOutput:\n%s", code, full)
	}
}

func Test_ErrorOnTooLongStrings(t *testing.T) {
	dir := t.TempDir()

	longUsername := strings.Repeat("a", 33) // 1 over the limit
	longEmail := strings.Repeat("a", 256)   // 1 over the limit

	script := []string{
		fmt.Sprintf("insert 1 %s %s", longUsername, longEmail),
		"select",
		".exit",
	}

	want := wantWithHeader(
		"> string is too long.",
		"> Executed.",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, script, want)
}

func Test_ErrorOnNegativeID(t *testing.T) {
	dir := t.TempDir()

	script := []string{
		"insert -1 cstack foo@bar.com",
		"select",
		".exit",
	}

	want := wantWithHeader(
		"> ID must be positive.",
		"> Executed.",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, script, want)
}

func Test_PrintConstantsMetaCommand(t *testing.T) {
	dir := t.TempDir()

	want := wantWithHeader(
		"> ROW_SIZE: 291",
		"COMMON_NODE_HEADER_SIZE: 6",
		"LEAF_NODE_HEADER_SIZE: 10",
		"LEAF_NODE_CELL_SIZE: 295",
		"LEAF_NODE_SPACE_FOR_CELLS: 4086",
		"LEAF_NODE_MAX_CELLS: 13",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, []string{
		".constants",
		".exit",
	}, want)
}

func Test_PrintBtreeMetaCommandOnEmptyTable(t *testing.T) {
	dir := t.TempDir()

	want := wantWithHeader(
		"> - leaf (size 0)",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, []string{
		".btree",
		".exit",
	}, want)
}

func Test_PrintBtreeMetaCommandWithRows(t *testing.T) {
	dir := t.TempDir()

	want := wantWithHeader(
		"> Executed.",
		"> Executed.",
		"> Executed.",
		"> - leaf (size 3)",
		"  - 1",
		"  - 2",
		"  - 3",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, []string{
		"insert 3 user3 person3@example.com",
		"insert 1 user1 person1@example.com",
		"insert 2 user2 person2@example.com",
		".btree",
		".exit",
	}, want)
}

func Test_ErrorOnDuplicateIDs(t *testing.T) {
	dir := t.TempDir()

	want := wantWithHeader(
		"> Executed.",
		"> Error: duplicate key.",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, []string{
		"insert 1 user1 person1@example.com",
		"insert 1 newuser newemail",
		".exit",
	}, want)
}

func Test_PrintThreeLeafNodeBtree(t *testing.T) {
	dir := t.TempDir()

	script := make([]string, 0, 18)
	for i := 1; i <= 15; i++ {
		script = append(script, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	script = append(script, ".btree")
	script = append(script, ".exit")

	// Build expected output
	want := wantWithHeader()
	// 15 "Executed." lines for inserts
	for range 15 {
		want = append(want, "> Executed.")
	}
	// Btree structure output
	want = append(want,
		"> - internal (size 1)",
		"  - leaf (size 7)",
		"    - 1",
		"    - 2",
		"    - 3",
		"    - 4",
		"    - 5",
		"    - 6",
		"    - 7",
		"  - key 8",
		"  - leaf (size 8)",
		"    - 8",
		"    - 9",
		"    - 10",
		"    - 11",
		"    - 12",
		"    - 13",
		"    - 14",
		"    - 15",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, script, want)
}

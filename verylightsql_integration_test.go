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

	// Table max rows is limited by tableMaxPages=100
	// With internal node splitting working, we can insert many more rows
	// until we run out of pages. Try to insert enough rows to fill all pages.
	script := make([]string, 0, 1402)
	for i := 1; i <= 1401; i++ {
		script = append(script, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	script = append(script, ".exit")

	out, full, code := runScript(t, dir, script)

	// Expect the process to exit (either success or with "page number out of bounds" error)
	if code != 0 {
		t.Fatalf("expected process to return zero exit code, but got exit code %d; output:\n%s", code, full)
	}

	// Verify we got "page number out of bounds" error when we ran out of pages
	if len(out) < 2 {
		t.Fatalf("output too short:\n%q", out)
	}
	found := false
	for _, line := range out {
		if strings.Contains(line, "page number out of bounds") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 'page number out of bounds' error in output when table is full, but did not find it.\nExit code: %d\nOutput:\n%s", code, full)
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
		"LEAF_NODE_HEADER_SIZE: 14",
		"LEAF_NODE_CELL_SIZE: 295",
		"LEAF_NODE_SPACE_FOR_CELLS: 4082",
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
		"  - key 7",
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

func Test_PrintAllRowsInMultiLevelTree(t *testing.T) {
	dir := t.TempDir()

	script := make([]string, 0, 17)
	for i := 1; i <= 15; i++ {
		script = append(script, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	script = append(script, "select")
	script = append(script, ".exit")

	want := wantWithHeader()
	// 15 "Executed." lines for inserts
	for range 15 {
		want = append(want, "> Executed.")
	}

	want = append(want, "> (1, user1, person1@example.com)")
	for i := 2; i <= 15; i++ {
		want = append(want, fmt.Sprintf("(%d, user%d, person%d@example.com)", i, i, i))
	}
	want = append(want,
		"Executed.",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, script, want)
}

func Test_PrintFourLeafNodeBtree(t *testing.T) {
	dir := t.TempDir()

	script := []string{
		"insert 18 user18 person18@example.com",
		"insert 7 user7 person7@example.com",
		"insert 10 user10 person10@example.com",
		"insert 29 user29 person29@example.com",
		"insert 23 user23 person23@example.com",
		"insert 4 user4 person4@example.com",
		"insert 14 user14 person14@example.com",
		"insert 30 user30 person30@example.com",
		"insert 15 user15 person15@example.com",
		"insert 26 user26 person26@example.com",
		"insert 22 user22 person22@example.com",
		"insert 19 user19 person19@example.com",
		"insert 2 user2 person2@example.com",
		"insert 1 user1 person1@example.com",
		"insert 21 user21 person21@example.com",
		"insert 11 user11 person11@example.com",
		"insert 6 user6 person6@example.com",
		"insert 20 user20 person20@example.com",
		"insert 5 user5 person5@example.com",
		"insert 8 user8 person8@example.com",
		"insert 9 user9 person9@example.com",
		"insert 3 user3 person3@example.com",
		"insert 12 user12 person12@example.com",
		"insert 27 user27 person27@example.com",
		"insert 17 user17 person17@example.com",
		"insert 16 user16 person16@example.com",
		"insert 13 user13 person13@example.com",
		"insert 24 user24 person24@example.com",
		"insert 25 user25 person25@example.com",
		"insert 28 user28 person28@example.com",
		".btree",
		".exit",
	}

	want := wantWithHeader()
	// 30 "Executed." lines for inserts
	for range 30 {
		want = append(want, "> Executed.")
	}
	// Btree structure output for 4-leaf-node tree
	want = append(want,
		"> - internal (size 3)",
		"  - leaf (size 7)",
		"    - 1",
		"    - 2",
		"    - 3",
		"    - 4",
		"    - 5",
		"    - 6",
		"    - 7",
		"  - key 7",
		"  - leaf (size 8)",
		"    - 8",
		"    - 9",
		"    - 10",
		"    - 11",
		"    - 12",
		"    - 13",
		"    - 14",
		"    - 15",
		"  - key 15",
		"  - leaf (size 7)",
		"    - 16",
		"    - 17",
		"    - 18",
		"    - 19",
		"    - 20",
		"    - 21",
		"    - 22",
		"  - key 22",
		"  - leaf (size 8)",
		"    - 23",
		"    - 24",
		"    - 25",
		"    - 26",
		"    - 27",
		"    - 28",
		"    - 29",
		"    - 30",
		"> Bye!",
	)

	mustRunAndAssert(t, dir, script, want)
}

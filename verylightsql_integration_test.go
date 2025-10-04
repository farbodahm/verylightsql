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
)

const (
	verylightsqlVersion    = "0.1.0"
	integrationTestTimeout = 3 * time.Second
)

var verylightsqlBinary string

func init() {
	// Make the binary path absolute so we can run it from temp dirs.
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	verylightsqlBinary = filepath.Join(cwd, "verylightsql")
}

func runScript(t *testing.T, workdir string, commands []string) (lines []string, all string, code int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), integrationTestTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, verylightsqlBinary)
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

func Test_InsertsAndRetrievesRow(t *testing.T) {
	dir := t.TempDir()

	out, full, code := runScript(t, dir, []string{
		"insert 1 user1 person1@example.com",
		"select",
		".exit",
	})
	if code != 0 {
		t.Fatalf("unexpected exit code %d; output:\n%s", code, full)
	}

	want := []string{
		fmt.Sprintf("Verylightsql v%s", verylightsqlVersion),
		"Opening database: vlsql.db",
		"> Executed.",
		"> (1, user1, person1@example.com)",
		"Executed.",
		"> Bye!",
	}

	if len(out) != len(want) {
		t.Fatalf("line count mismatch\nout:\n%q\nwant:\n%q", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("line %d mismatch\n got: %q\nwant: %q\nfull out:\n%s", i, out[i], want[i], full)
		}
	}
}

func Test_PersistsDataAfterClose(t *testing.T) {
	dir := t.TempDir()

	out1, full1, code1 := runScript(t, dir, []string{
		"insert 1 user1 person1@example.com",
		".exit",
	})
	if code1 != 0 {
		t.Fatalf("unexpected exit code %d; output:\n%s", code1, full1)
	}

	want1 := []string{
		fmt.Sprintf("Verylightsql v%s", verylightsqlVersion),
		"Opening database: vlsql.db",
		"> Executed.",
		"> Bye!",
	}

	if len(out1) != len(want1) {
		t.Fatalf("line count mismatch after first run\nout:\n%q\nwant:\n%q", out1, want1)
	}
	for i := range want1 {
		if out1[i] != want1[i] {
			t.Fatalf("first run line %d mismatch\n got: %q\nwant: %q\nfull out:\n%s", i, out1[i], want1[i], full1)
		}
	}

	out2, full2, code2 := runScript(t, dir, []string{
		"select",
		".exit",
	})
	if code2 != 0 {
		t.Fatalf("unexpected exit code %d; output:\n%s", code2, full2)
	}

	want2 := []string{
		fmt.Sprintf("Verylightsql v%s", verylightsqlVersion),
		"Opening database: vlsql.db",
		"> (1, user1, person1@example.com)",
		"Executed.",
		"> Bye!",
	}

	if len(out2) != len(want2) {
		t.Fatalf("line count mismatch after reopen\nout:\n%q\nwant:\n%q", out2, want2)
	}
	for i := range want2 {
		if out2[i] != want2[i] {
			t.Fatalf("second run line %d mismatch\n got: %q\nwant: %q\nfull out:\n%s", i, out2[i], want2[i], full2)
		}
	}
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

	out, full, code := runScript(t, dir, script)
	if code != 0 {
		t.Fatalf("unexpected exit code %d; output:\n%s", code, full)
	}

	want := []string{
		fmt.Sprintf("Verylightsql v%s", verylightsqlVersion),
		"Opening database: vlsql.db",
		"> Executed.",
		fmt.Sprintf("> (1, %s, %s)", longUsername, longEmail),
		"Executed.",
		"> Bye!",
	}

	if len(out) != len(want) {
		t.Fatalf("line count mismatch\nout:\n%q\nwant:\n%q", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("line %d mismatch\n got: %q\nwant: %q\nfull out:\n%s", i, out[i], want[i], full)
		}
	}
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
	if code != 0 {
		t.Fatalf("unexpected exit code %d; output:\n%s", code, full)
	}

	// The error should be printed before the last prompt and exit
	if len(out) < 2 {
		t.Fatalf("output too short:\n%q", out)
	}
	found := false
	for _, line := range out {
		if strings.Contains(strings.ToLower(line), "table is full") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected table full error, but did not find it in output:\n%s", full)
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

	out, full, code := runScript(t, dir, script)
	if code != 0 {
		t.Fatalf("unexpected exit code %d; output:\n%s", code, full)
	}

	want := []string{
		fmt.Sprintf("Verylightsql v%s", verylightsqlVersion),
		"Opening database: vlsql.db",
		"> string is too long.",
		"> Executed.",
		"> Bye!",
	}

	if len(out) != len(want) {
		t.Fatalf("line count mismatch\nout:\n%q\nwant:\n%q", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("line %d mismatch\n got: %q\nwant: %q\nfull out:\n%s", i, out[i], want[i], full)
		}
	}
}

func Test_ErrorOnNegativeID(t *testing.T) {
	dir := t.TempDir()

	script := []string{
		"insert -1 cstack foo@bar.com",
		"select",
		".exit",
	}

	out, full, code := runScript(t, dir, script)
	if code != 0 {
		t.Fatalf("unexpected exit code %d; output:\n%s", code, full)
	}

	want := []string{
		fmt.Sprintf("Verylightsql v%s", verylightsqlVersion),
		"Opening database: vlsql.db",
		"> ID must be positive.",
		"> Executed.",
		"> Bye!",
	}

	if len(out) != len(want) {
		t.Fatalf("line count mismatch\nout:\n%q\nwant:\n%q", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("line %d mismatch\n got: %q\nwant: %q\nfull out:\n%s", i, out[i], want[i], full)
		}
	}
}

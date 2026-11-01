package scripts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type execResult struct {
	Session string
	PID     int
	Args    []string
	Input   string
}

func TestIntegrationExecSessionIsolation(t *testing.T) {
	t.Setenv("TESTCONTAINERS_SESSION_ID", "caller-session")
	first := runIntegrationExec(t)
	second := runIntegrationExec(t)
	if first.Session == second.Session || first.Session == "caller-session" {
		t.Fatalf("package sessions must differ: %q, %q", first.Session, second.Session)
	}
	if !strings.HasPrefix(first.Session, "caller-session-") {
		t.Fatalf("caller namespace lost: %q", first.Session)
	}
	t.Setenv("TESTCONTAINERS_SESSION_ID", "")
	if result := runIntegrationExec(t); result.Session == "" {
		t.Fatal("default session is empty")
	}
}

func TestIntegrationExecForwarding(t *testing.T) {
	args := []string{"space value", "", "quote\"value", "$literal", "line\nbreak"}
	cmd := integrationExec(t, args...)
	cmd.Stdin = strings.NewReader("test input\n")
	var output bytes.Buffer
	cmd.Stdout = &output
	var exitErr *exec.ExitError
	if err := cmd.Run(); !errors.As(err, &exitErr) || exitErr.ExitCode() != 37 {
		t.Fatalf("exit status = %v, want 37", err)
	}
	var result execResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.PID != cmd.Process.Pid || result.Input != "test input\n" {
		t.Fatalf("process or input changed: %+v", result)
	}
	if len(result.Args) != len(args) {
		t.Fatalf("args = %q, want %q", result.Args, args)
	}
	for i := range args {
		if result.Args[i] != args[i] {
			t.Fatalf("arg %d = %q, want %q", i, result.Args[i], args[i])
		}
	}
}

func TestIntegrationExecSignals(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Setenv("FABRIQ_INTEGRATION_EXEC_WAIT", "1")
			cmd := integrationExec(t)
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			var result execResult
			if err := json.NewDecoder(output).Decode(&result); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			var exitErr *exec.ExitError
			if err := cmd.Wait(); !errors.As(err, &exitErr) {
				t.Fatalf("signal exit = %v", err)
			}
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			if !ok || status.Signal() != sig {
				t.Fatalf("exit status = %v, want signal %s", exitErr, sig)
			}
		})
	}
}

func integrationExec(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	script, err := filepath.Abs("integration-exec.sh")
	if err != nil {
		t.Fatal(err)
	}
	commandArgs := append([]string{script, os.Args[0], "-test.run=^TestIntegrationExecHelper$", "--"}, args...)
	cmd := exec.CommandContext(ctx, "sh", commandArgs...)
	cmd.Env = append(os.Environ(), "FABRIQ_INTEGRATION_EXEC_HELPER=1")
	cmd.Stderr = os.Stderr
	return cmd
}

func runIntegrationExec(t *testing.T) execResult {
	t.Helper()
	output, err := integrationExec(t).Output()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 37 {
		t.Fatalf("wrapper exit = %v, want 37", err)
	}
	var result execResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestIntegrationExecHelper(t *testing.T) {
	if os.Getenv("FABRIQ_INTEGRATION_EXEC_HELPER") != "1" {
		return
	}
	signal.Reset(os.Interrupt, syscall.SIGTERM)
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(execResult{Session: os.Getenv("TESTCONTAINERS_SESSION_ID"), PID: os.Getpid(), Args: args, Input: string(input)}); err != nil {
		t.Fatal(err)
	}
	for os.Getenv("FABRIQ_INTEGRATION_EXEC_WAIT") == "1" {
		time.Sleep(time.Minute)
	}
	os.Exit(37)
}

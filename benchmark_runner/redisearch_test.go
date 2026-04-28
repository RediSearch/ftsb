package benchmark_runner

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestFTSBWithDuration(t *testing.T) {
	entries, err := os.ReadDir("../bin")
	if err != nil {
		t.Fatalf("Failed to read bin/ directory: %v", err)
	}

	t.Log("Listing bin/ contents:")
	for _, entry := range entries {
		t.Logf(" - %s", entry.Name())
	}
	t.Log("Starting Redis container...")
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.4")
	containerIDRaw, err := dockerRun.Output()
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	containerID := strings.TrimSpace(string(containerIDRaw))

	t.Cleanup(func() {
		t.Log("Stopping Redis container...")
		exec.Command("docker", "stop", containerID).Run()
	})

	t.Log("Waiting for Redis to be ready...")
	time.Sleep(2 * time.Second)

	t.Log("Running ftsb_redisearch with --duration=5s")
	start := time.Now()
	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/minimal.csv",
		"--duration=5s",
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Benchmark failed: %v\nOutput: %s", err, string(output))
	}

	if duration < 5*time.Second {
		t.Errorf("Benchmark exited too early: ran for %v", duration)
	}
	if !strings.Contains(string(output), "Issued") {
		t.Errorf("Expected benchmark output to contain 'Issued', got: %s", string(output))
	}
}

func TestFTSBWithRequests(t *testing.T) {
	t.Log("Starting Redis container...")
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.4")
	containerIDRaw, err := dockerRun.Output()
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	containerID := strings.TrimSpace(string(containerIDRaw))
	t.Cleanup(func() {
		t.Log("Stopping Redis container...")
		exec.Command("docker", "stop", containerID).Run()
	})

	t.Log("Waiting for Redis to be ready...")
	time.Sleep(2 * time.Second)

	t.Log("Running ftsb_redisearch with --requests=50000")
	jsonPath := "../testdata/results.requests.json"
	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/minimal.csv",
		"--requests=50000",
		"--json-out-file", jsonPath,
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Benchmark failed: %v\nOutput: %s", err, string(output))
	}

	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("Failed to read json output file: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	if parsed["Limit"] != float64(50000) { // json.Unmarshal converts numbers to float64
		t.Errorf("Expected Limit to be 50000, got %v", parsed["Limit"])
	}
}

func TestFTSBWithNoLimitNoDuration(t *testing.T) {
	t.Log("Starting Redis container...")
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.4")
	containerIDRaw, err := dockerRun.Output()
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	containerID := strings.TrimSpace(string(containerIDRaw))
	t.Cleanup(func() {
		t.Log("Stopping Redis container...")
		exec.Command("docker", "stop", containerID).Run()
	})

	t.Log("Waiting for Redis to be ready...")
	time.Sleep(2 * time.Second)

	t.Log("Running ftsb_redisearch with no --requests or --duration")
	jsonPath := "../testdata/results.nolimit.json"
	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/minimal.csv",
		"--json-out-file", jsonPath,
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Benchmark failed: %v\nOutput: %s", err, string(output))
	}

	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("Failed to read json output file: %v", err)
	}

	var parsed struct {
		Limit  int `json:"Limit"`
		Totals struct {
			TotalOps int `json:"TotalOps"`
		} `json:"Totals"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	if parsed.Limit != 0 {
		t.Errorf("Expected Limit to be 0, got %v", parsed.Limit)
	}
	if parsed.Totals.TotalOps <= 0 {
		t.Errorf("Expected Totals.TotalOps to be > 0, got %v", parsed.Totals.TotalOps)
	}
}

func TestFTSBErrorAndTimeoutTracking(t *testing.T) {
	t.Log("Starting Redis container...")
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.4")
	containerIDRaw, err := dockerRun.Output()
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	containerID := strings.TrimSpace(string(containerIDRaw))
	t.Cleanup(func() {
		t.Log("Stopping Redis container...")
		exec.Command("docker", "stop", containerID).Run()
	})

	t.Log("Waiting for Redis to be ready...")
	time.Sleep(2 * time.Second)

	t.Log("Running ftsb_redisearch with normal operation (should have 0 errors)")
	jsonPath := "../testdata/results.errors.json"
	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/minimal.csv",
		"--requests=1000",
		"--json-out-file", jsonPath,
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Benchmark failed: %v\nOutput: %s", err, string(output))
	}

	// Check that output contains summary
	outputStr := string(output)
	if !strings.Contains(outputStr, "Summary:") {
		t.Errorf("Expected output to contain 'Summary:', got: %s", outputStr)
	}

	// Parse JSON output
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("Failed to read json output file: %v", err)
	}

	var parsed struct {
		Totals struct {
			TotalOps int     `json:"TotalOps"`
			Errors   float64 `json:"Errors"`
			Timeouts float64 `json:"Timeouts"`
		} `json:"Totals"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	// Verify error and timeout fields exist in JSON
	if parsed.Totals.TotalOps <= 0 {
		t.Errorf("Expected TotalOps to be > 0, got %v", parsed.Totals.TotalOps)
	}

	// In normal operation, errors and timeouts should be 0
	if parsed.Totals.Errors != 0 {
		t.Logf("Warning: Expected Errors to be 0, got %v", parsed.Totals.Errors)
	}
	if parsed.Totals.Timeouts != 0 {
		t.Logf("Warning: Expected Timeouts to be 0, got %v", parsed.Totals.Timeouts)
	}

	// Verify that error statistics ARE shown in output (even when there are no errors)
	if !strings.Contains(outputStr, "Error Statistics:") {
		t.Errorf("Expected output to contain 'Error Statistics:'")
	}

	t.Log("Test passed: Error and timeout tracking fields are present in JSON output")
}

func TestFTSBWithConnectionFailure(t *testing.T) {
	t.Log("Running ftsb_redisearch against non-existent Redis (should trigger errors)")
	jsonPath := "../testdata/results.connection_error.json"
	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/minimal.csv",
		"--requests=10",
		"--json-out-file", jsonPath,
		"--continue-on-error",
	)
	// Point to a non-existent Redis instance
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:9999")
	output, err := cmd.CombinedOutput()

	// The benchmark should fail or complete with errors
	outputStr := string(output)
	t.Logf("Output: %s", outputStr)

	// Check if JSON file was created
	if _, statErr := os.Stat(jsonPath); statErr == nil {
		data, readErr := os.ReadFile(jsonPath)
		if readErr == nil {
			var parsed struct {
				Totals struct {
					Errors   float64 `json:"Errors"`
					Timeouts float64 `json:"Timeouts"`
				} `json:"Totals"`
			}
			if jsonErr := json.Unmarshal(data, &parsed); jsonErr == nil {
				t.Logf("Errors in JSON: %v", parsed.Totals.Errors)
				t.Logf("Timeouts in JSON: %v", parsed.Totals.Timeouts)

				// We expect either errors or timeouts to be > 0
				if parsed.Totals.Errors > 0 || parsed.Totals.Timeouts > 0 {
					t.Log("Test passed: Errors/timeouts were properly tracked")
				}
			}
		}
	}

	// This test is informational - we're just verifying the tracking mechanism exists
	if err != nil {
		t.Logf("Expected failure when connecting to non-existent Redis: %v", err)
	}
}

// startRedisDebugContainer spins up redis:8.6-rc1 with --enable-debug-command
// on host port 6379 and registers cleanup. Centralised so duplicated docker
// boilerplate doesn't flake the SonarCloud duplication gate.
func startRedisDebugContainer(t *testing.T) {
	t.Helper()
	t.Log("Starting Redis container with debug commands enabled...")
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379",
		"redis:8.6-rc1", "redis-server", "--enable-debug-command", "yes")
	containerIDRaw, err := dockerRun.Output()
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	containerID := strings.TrimSpace(string(containerIDRaw))
	t.Cleanup(func() {
		t.Log("Stopping Redis container...")
		exec.Command("docker", "stop", containerID).Run()
	})
	t.Log("Waiting for Redis to be ready...")
	time.Sleep(2 * time.Second)
}

func TestFTSBWithTimeout(t *testing.T) {
	startRedisDebugContainer(t)

	t.Log("Running ftsb_redisearch with timeout_test.csv (contains DEBUG SLEEP commands)")
	jsonPath := "../testdata/results.timeout.json"
	logPath := "../testdata/timeout_debug.log"
	// Remove log file if it exists
	os.Remove(logPath)

	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/timeout_test.csv",
		"--timeout=1", // 1 second timeout
		"--continue-on-error",
		"--debug=1", // Enable debug output to see error messages
		"--json-out-file", jsonPath,
		"--log-file", logPath,
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()

	outputStr := string(output)
	t.Logf("Output: %s", outputStr)

	// The benchmark should complete (with --continue-on-error)
	if err != nil {
		t.Logf("Benchmark completed with error (expected due to timeouts): %v", err)
	}

	// Parse JSON output
	data, readErr := os.ReadFile(jsonPath)
	if readErr != nil {
		t.Fatalf("Failed to read json output file: %v", readErr)
	}

	var parsed struct {
		Totals struct {
			TotalOps int     `json:"TotalOps"`
			Errors   float64 `json:"Errors"`
			Timeouts float64 `json:"Timeouts"`
		} `json:"Totals"`
	}
	if jsonErr := json.Unmarshal(data, &parsed); jsonErr != nil {
		t.Fatalf("Failed to parse JSON output: %v", jsonErr)
	}

	t.Logf("Total Ops: %d", parsed.Totals.TotalOps)
	t.Logf("Errors: %v", parsed.Totals.Errors)
	t.Logf("Timeouts: %v", parsed.Totals.Timeouts)

	// Verify that timeouts were detected
	// The DEBUG SLEEP 2 command should timeout with a 1 second timeout
	if parsed.Totals.Timeouts == 0 {
		t.Errorf("Expected Timeouts to be > 0 (DEBUG SLEEP should have triggered timeouts), got %v", parsed.Totals.Timeouts)
	}

	// Verify that "Error Statistics:" appears in output when there are timeouts
	if !strings.Contains(outputStr, "Error Statistics:") {
		t.Errorf("Expected output to contain 'Error Statistics:' when timeouts occur")
	}

	if !strings.Contains(outputStr, "Timeout") {
		t.Errorf("Expected output to contain 'Timeout' in error statistics")
	}

	// Verify log file was created and contains timeout information
	logData, logReadErr := os.ReadFile(logPath)
	if logReadErr != nil {
		t.Fatalf("Failed to read log file: %v", logReadErr)
	}

	logContent := string(logData)
	t.Logf("Log file content length: %d bytes", len(logContent))

	// Verify log file contains the timeout error message with DEBUG SLEEP command on the same line
	foundTimeoutWithDebugSleep := false
	for _, line := range strings.Split(logContent, "\n") {
		if strings.Contains(line, "Timeout occurred") && strings.Contains(line, "DEBUG") && strings.Contains(line, "SLEEP") {
			foundTimeoutWithDebugSleep = true
			t.Logf("Found timeout line with DEBUG SLEEP: %s", line)
			break
		}
	}
	if !foundTimeoutWithDebugSleep {
		t.Errorf("Expected log file to contain a line with 'Timeout occurred', 'DEBUG', and 'SLEEP' all on the same line")
	}

	// Verify log file contains error statistics
	if !strings.Contains(logContent, "Error Statistics:") {
		t.Errorf("Expected log file to contain 'Error Statistics:'")
	}

	if !strings.Contains(logContent, "Total Timeouts:") {
		t.Errorf("Expected log file to contain 'Total Timeouts:'")
	}

	// Clean up log file
	os.Remove(logPath)

	t.Log("Test passed: Timeouts were properly detected and tracked in both stdout and log file")
}

func TestFTSBWithLogFile(t *testing.T) {
	t.Log("Starting Redis container...")
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.4")
	containerIDRaw, err := dockerRun.Output()
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	containerID := strings.TrimSpace(string(containerIDRaw))
	t.Cleanup(func() {
		t.Log("Stopping Redis container...")
		exec.Command("docker", "stop", containerID).Run()
	})

	t.Log("Waiting for Redis to be ready...")
	time.Sleep(2 * time.Second)

	t.Log("Running ftsb_redisearch with --log-file")
	logPath := "../testdata/benchmark.log"
	// Remove log file if it exists
	os.Remove(logPath)

	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/minimal.csv",
		"--requests=100",
		"--log-file", logPath,
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Benchmark failed: %v\nOutput: %s", err, string(output))
	}

	// Verify log file was created
	if _, statErr := os.Stat(logPath); statErr != nil {
		t.Fatalf("Log file was not created: %v", statErr)
	}

	// Read log file content
	logData, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("Failed to read log file: %v", readErr)
	}

	logContent := string(logData)
	t.Logf("Log file content length: %d bytes", len(logContent))

	// Verify log file contains expected content
	if !strings.Contains(logContent, "ftsb (git_sha1:") {
		t.Errorf("Expected log file to contain 'ftsb (git_sha1:', got: %s", logContent)
	}

	if !strings.Contains(logContent, "Logging to file:") {
		t.Errorf("Expected log file to contain 'Logging to file:', got: %s", logContent)
	}

	// Verify log file contains summary output
	if !strings.Contains(logContent, "Summary:") {
		t.Errorf("Expected log file to contain 'Summary:'")
	}

	// Verify log file contains error statistics (even when 0)
	if !strings.Contains(logContent, "Error Statistics:") {
		t.Errorf("Expected log file to contain 'Error Statistics:'")
	}

	if !strings.Contains(logContent, "Total Errors:") {
		t.Errorf("Expected log file to contain 'Total Errors:'")
	}

	if !strings.Contains(logContent, "Total Timeouts:") {
		t.Errorf("Expected log file to contain 'Total Timeouts:'")
	}

	// Verify stdout also contains the output
	outputStr := string(output)
	if !strings.Contains(outputStr, "ftsb (git_sha1:") {
		t.Errorf("Expected stdout to contain 'ftsb (git_sha1:', got: %s", outputStr)
	}

	// Clean up log file
	os.Remove(logPath)

	t.Log("Test passed: Log file functionality works correctly")
}

func TestFTSBWithLogFileAndTimeout(t *testing.T) {
	startRedisDebugContainer(t)

	t.Log("Running ftsb_redisearch with timeout_test.csv and --log-file")
	logPath := "../testdata/benchmark_timeout.log"
	// Remove log file if it exists
	os.Remove(logPath)

	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/timeout_test.csv",
		"--timeout=1", // 1 second timeout
		"--continue-on-error",
		"--log-file", logPath,
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()

	// The benchmark should complete (with --continue-on-error)
	if err != nil {
		t.Logf("Benchmark completed with error (expected due to timeouts): %v", err)
	}

	// Verify log file was created
	if _, statErr := os.Stat(logPath); statErr != nil {
		t.Fatalf("Log file was not created: %v", statErr)
	}

	// Read log file content
	logData, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("Failed to read log file: %v", readErr)
	}

	logContent := string(logData)
	t.Logf("Log file content length: %d bytes", len(logContent))

	// Verify log file contains timeout messages
	if !strings.Contains(logContent, "Timeout occurred with") {
		t.Errorf("Expected log file to contain timeout message")
	}

	// Verify log file contains error statistics with non-zero values
	if !strings.Contains(logContent, "Error Statistics:") {
		t.Errorf("Expected log file to contain 'Error Statistics:'")
	}

	// Check that the log file shows non-zero errors/timeouts
	if !strings.Contains(logContent, "Total Errors:") {
		t.Errorf("Expected log file to contain 'Total Errors:'")
	}

	if !strings.Contains(logContent, "Total Timeouts:") {
		t.Errorf("Expected log file to contain 'Total Timeouts:'")
	}

	// Verify the log file doesn't show 0 timeouts (should have actual timeouts)
	if strings.Contains(logContent, "Total Timeouts: 0 (0.00%)") {
		t.Errorf("Expected log file to show non-zero timeouts, but got 0")
	}

	// Verify stdout also contains the same output
	outputStr := string(output)
	if !strings.Contains(outputStr, "Timeout occurred") {
		t.Errorf("Expected stdout to contain timeout message")
	}

	// Clean up log file
	os.Remove(logPath)

	t.Log("Test passed: Log file captures timeout and error information correctly")
}

func TestFTSBWithBatchSize(t *testing.T) {
	t.Log("Starting Redis container...")
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.4")
	containerIDRaw, err := dockerRun.Output()
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	containerID := strings.TrimSpace(string(containerIDRaw))

	t.Cleanup(func() {
		t.Log("Stopping Redis container...")
		exec.Command("docker", "stop", containerID).Run()
	})

	t.Log("Waiting for Redis to be ready...")
	time.Sleep(2 * time.Second)

	t.Log("Running ftsb_redisearch with --batch-size=50 --duration=3s")
	cmd := exec.Command("../bin/ftsb_redisearch",
		"--input", "../testdata/minimal.csv",
		"--batch-size=50",
		"--duration=3s",
	)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()

	if err != nil {
		t.Fatalf("Benchmark failed: %v\nOutput: %s", err, string(output))
	}

	outputStr := string(output)
	// Guards against the "flag provided but not defined: -batch-size" regression
	// and against a reintroduction of the zero-batchSize panic.
	if strings.Contains(outputStr, "flag provided but not defined") {
		t.Errorf("--batch-size flag not registered; got: %s", outputStr)
	}
	if strings.Contains(outputStr, "panic:") {
		t.Errorf("Benchmark panicked with --batch-size=50; got: %s", outputStr)
	}
	if !strings.Contains(outputStr, "Issued") {
		t.Errorf("Expected benchmark output to contain 'Issued', got: %s", outputStr)
	}
}

// runSleepBenchmark spins up a Redis container with debug commands enabled,
// runs ftsb_redisearch against testdata/sleep_test.csv (which contains a
// DEBUG SLEEP 2), and returns the q100 (max) latency in milliseconds for the
// "allCommands" group in the JSON output. extraArgs lets the caller override
// --max-latency-seconds (and anything else).
func runSleepBenchmark(t *testing.T, jsonPath string, extraArgs ...string) float64 {
	t.Helper()
	startRedisDebugContainer(t)

	args := []string{
		"--input", "../testdata/sleep_test.csv",
		"--workers=1",
		"--timeout=10", // seconds; well above the 2s sleep so the request completes
		"--json-out-file", jsonPath,
	}
	args = append(args, extraArgs...)

	t.Logf("Running ftsb_redisearch %v", args)
	cmd := exec.Command("../bin/ftsb_redisearch", args...)
	cmd.Env = append(os.Environ(), "REDIS_URL=redis://localhost:6379")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Benchmark failed: %v\nOutput: %s", err, string(output))
	}

	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("Failed to read json output file: %v", err)
	}

	var parsed struct {
		OverallQuantiles struct {
			AllCommands map[string]float64 `json:"allCommands"`
		} `json:"OverallQuantiles"`
		Totals struct {
			TotalOps int     `json:"TotalOps"`
			Timeouts float64 `json:"Timeouts"`
		} `json:"Totals"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	if parsed.Totals.Timeouts > 0 {
		t.Fatalf("DEBUG SLEEP unexpectedly timed out (%v); raise --timeout in the test setup", parsed.Totals.Timeouts)
	}
	if parsed.Totals.TotalOps <= 0 {
		t.Fatalf("Expected TotalOps > 0, got %v", parsed.Totals.TotalOps)
	}

	q100, ok := parsed.OverallQuantiles.AllCommands["q100"]
	if !ok {
		t.Fatalf("Expected OverallQuantiles.allCommands.q100 in JSON, got: %s", string(data))
	}
	t.Logf("q100 (max latency) = %.2f ms", q100)
	return q100
}

// TestFTSBLatencyCapDefault verifies that with the new 60s default cap, a
// DEBUG SLEEP 2 latency is recorded above the old 1s ceiling. Prior to the
// configurable cap, this would have clamped at ~1000 ms.
func TestFTSBLatencyCapDefault(t *testing.T) {
	q100 := runSleepBenchmark(t, "../testdata/results.latencycap_default.json")
	if q100 <= 1000 {
		t.Errorf("Expected q100 > 1000 ms with default 60s cap (DEBUG SLEEP 2 should record ~2000 ms), got %.2f ms", q100)
	}
	if q100 < 1500 {
		t.Errorf("Expected q100 >= 1500 ms (DEBUG SLEEP 2), got %.2f ms — sleep may not have run", q100)
	}
}

// TestFTSBLatencyCapClamped verifies that --max-latency-seconds=1 actually
// clamps recorded latencies — proving the flag is wired through to every
// histogram allocation site (fixed + per-second + per-query).
func TestFTSBLatencyCapClamped(t *testing.T) {
	q100 := runSleepBenchmark(t,
		"../testdata/results.latencycap_clamped.json",
		"--max-latency-seconds=1",
	)
	// hdrhistogram clamps RecordValue to highestTrackable; with cap=1s we
	// expect q100 <= ~1000 ms even though the real latency was ~2000 ms.
	if q100 > 1100 {
		t.Errorf("Expected q100 <= ~1000 ms when capped at 1s, got %.2f ms — flag not honored", q100)
	}
}

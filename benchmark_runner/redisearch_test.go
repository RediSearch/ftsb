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
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.0-M04-bookworm")
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
	dockerRun := exec.Command("docker", "run", "--rm", "-d", "-p", "6379:6379", "redis:8.0-M04-bookworm")
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

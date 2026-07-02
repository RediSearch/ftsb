package main

import (
	"encoding/base64"
	"testing"
)

// A 16-byte little-endian float32 payload whose bytes include 0x0A (newline)
// and 0x2C (comma) — exactly the bytes that can't travel raw through the
// line-oriented CSV input.
var rawBinary = []byte{
	0x0a, 0x2c, 0x00, 0x3f,
	0xcd, 0xcc, 0x4c, 0x3e,
	0x9a, 0x99, 0x99, 0x3e,
	0xcd, 0xcc, 0xcc, 0x3e,
}

func TestDecodeBinaryArgsDecodesMarkedArg(t *testing.T) {
	args := []string{"doc:1", "vec", binaryArgMarker + base64.StdEncoding.EncodeToString(rawBinary)}
	decodeBinaryArgs(args)
	if args[2] != string(rawBinary) {
		t.Fatalf("marked arg not decoded to raw bytes: got %q", args[2])
	}
	if len(args[2]) != len(rawBinary) {
		t.Fatalf("decoded length = %d, want %d", len(args[2]), len(rawBinary))
	}
}

func TestDecodeBinaryArgsLeavesUnmarkedArgsUntouched(t *testing.T) {
	args := []string{"doc:1", "title", "hello __b64 world", "SGVsbG8="}
	want := []string{"doc:1", "title", "hello __b64 world", "SGVsbG8="}
	decodeBinaryArgs(args)
	for i := range args {
		if args[i] != want[i] {
			t.Fatalf("unmarked arg %d modified: got %q, want %q", i, args[i], want[i])
		}
	}
}

func TestPreProcessCmdDecodesMarkedFieldInHSET(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString(rawBinary)
	row := "SETUP,setup-doc-1,1,HSET,doc:1,vec," + binaryArgMarker + encoded
	cmdType, _, _, cmd, key, _, args, _, err := preProcessCmd(row)
	if err != nil {
		t.Fatalf("preProcessCmd returned error: %v", err)
	}
	if cmdType != "SETUP" || cmd != "HSET" || key != "doc:1" {
		t.Fatalf("unexpected parse: cmdType=%q cmd=%q key=%q", cmdType, cmd, key)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d: %q", len(args), args)
	}
	if args[2] != string(rawBinary) {
		t.Fatalf("vector arg not decoded: got %d bytes, want %d raw bytes", len(args[2]), len(rawBinary))
	}
}

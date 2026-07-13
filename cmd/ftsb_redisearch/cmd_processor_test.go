package main

import (
	"encoding/base64"
	"strings"
	"testing"

	radix "github.com/mediocregopher/radix/v3"
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
	shrink, err := decodeBinaryArgs(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args[2] != string(rawBinary) {
		t.Fatalf("marked arg not decoded to raw bytes: got %q", args[2])
	}
	if len(args[2]) != len(rawBinary) {
		t.Fatalf("decoded length = %d, want %d", len(args[2]), len(rawBinary))
	}
	// 16 bytes -> 24 base64 chars + 7-char marker = 31; shrink = 31 - 16 = 15.
	if wantShrink := uint64(len(binaryArgMarker) + 24 - len(rawBinary)); shrink != wantShrink {
		t.Fatalf("shrink = %d, want %d", shrink, wantShrink)
	}
}

func TestDecodeBinaryArgsLeavesUnmarkedArgsUntouched(t *testing.T) {
	// Includes a single-underscore near-miss ("hello __b64 world") and a bare
	// base64-looking token that is NOT marker-prefixed — both pass through.
	args := []string{"doc:1", "title", "hello __b64 world", "SGVsbG8=", "x__b64__notprefix"}
	want := []string{"doc:1", "title", "hello __b64 world", "SGVsbG8=", "x__b64__notprefix"}
	shrink, err := decodeBinaryArgs(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shrink != 0 {
		t.Fatalf("shrink = %d, want 0 (no marked args)", shrink)
	}
	for i := range args {
		if args[i] != want[i] {
			t.Fatalf("unmarked arg %d modified: got %q, want %q", i, args[i], want[i])
		}
	}
}

// A malformed base64 payload must be REPORTED as an error, not fatally abort
// the process — so the caller can honor -continue-on-error.
func TestDecodeBinaryArgsRejectsBadBase64(t *testing.T) {
	args := []string{"doc:1", "vec", binaryArgMarker + "@@not-valid-base64@@"}
	if _, err := decodeBinaryArgs(args); err == nil {
		t.Fatal("expected an error for invalid base64, got nil")
	}
}

// A URL-safe payload is NOT standard base64 and must be rejected (pins the
// documented StdEncoding-only contract).
func TestDecodeBinaryArgsRejectsNonStdEncoding(t *testing.T) {
	urlSafe := base64.URLEncoding.EncodeToString([]byte{0xff, 0xfe, 0xfd}) // "__79" in URL alphabet
	if !strings.ContainsAny(urlSafe, "-_") {
		t.Skipf("payload %q has no URL-safe-specific chars; test is vacuous", urlSafe)
	}
	args := []string{"vec", binaryArgMarker + urlSafe}
	if _, err := decodeBinaryArgs(args); err == nil {
		t.Fatalf("expected StdEncoding to reject URL-safe payload %q", urlSafe)
	}
}

// An empty payload (`__b64__` with nothing after it) is corrupt for a binary
// field — it would ship a zero-length blob and silently fail indexing.
func TestDecodeBinaryArgsRejectsEmptyBlob(t *testing.T) {
	if _, err := decodeBinaryArgs([]string{binaryArgMarker}); err == nil {
		t.Fatal("expected an error for empty base64 payload, got nil")
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

// The byte counter used for reported wire throughput must reflect the decoded
// blob actually sent to Redis, not the larger base64 text in the row.
func TestPreProcessCmdBytelenReflectsDecodedSize(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString(rawBinary) // 16B -> 24 chars
	field := binaryArgMarker + encoded                      // 7 + 24 = 31 chars
	row := "SETUP,setup-doc-1,1,HSET,doc:1,vec," + field
	_, _, _, _, _, _, _, bytelen, err := preProcessCmd(row)
	if err != nil {
		t.Fatalf("preProcessCmd returned error: %v", err)
	}
	shrink := uint64(len(field) - len(rawBinary)) // 31 - 16 = 15
	want := uint64(len(row)) - uint64(len("SETUP")) - shrink
	if bytelen != want {
		t.Fatalf("bytelen = %d, want %d (must reflect decoded 16B, not encoded 31B)", bytelen, want)
	}
	if bytelen >= uint64(len(row)) {
		t.Fatalf("bytelen %d should be < raw row length %d when a __b64__ field is present", bytelen, len(row))
	}
}

// A corrupt marked arg surfaces as a returned error (NOT os.Exit), so the
// worker can skip the row under -continue-on-error.
func TestPreProcessCmdBadBase64ReturnsError(t *testing.T) {
	row := "SETUP,setup-doc-1,1,HSET,doc:1,vec," + binaryArgMarker + "not_base64!!"
	if _, _, _, _, _, _, _, _, err := preProcessCmd(row); err == nil {
		t.Fatal("expected preProcessCmd to return an error for corrupt base64, got nil")
	}
}

// Vector benchmarks issue KNN *queries*, not just ingest. A base64 BLOB param
// on a READ/FT.SEARCH row must be decoded on the same path.
func TestPreProcessCmdDecodesMarkedBlobInQuery(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString(rawBinary)
	row := "READ,knn-1,1,FT.SEARCH,idx,*=>[KNN 10 @vec $BLOB],PARAMS,2,BLOB," + binaryArgMarker + encoded + ",DIALECT,2"
	cmdType, _, _, cmd, key, _, args, _, err := preProcessCmd(row)
	if err != nil {
		t.Fatalf("preProcessCmd returned error: %v", err)
	}
	if cmdType != "READ" || cmd != "FT.SEARCH" || key != "idx" {
		t.Fatalf("unexpected parse: cmdType=%q cmd=%q key=%q", cmdType, cmd, key)
	}
	// The BLOB value is the arg immediately after the literal "BLOB".
	blobIdx := -1
	for i, a := range args {
		if a == "BLOB" && i+1 < len(args) {
			blobIdx = i + 1
			break
		}
	}
	if blobIdx == -1 {
		t.Fatalf("could not locate BLOB param in args: %q", args)
	}
	if args[blobIdx] != string(rawBinary) {
		t.Fatalf("query BLOB not decoded: got %d bytes, want %d raw bytes", len(args[blobIdx]), len(rawBinary))
	}
}

// Every marked arg in a row is decoded, not just the first.
func TestPreProcessCmdDecodesMultipleMarkedArgs(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString(rawBinary)
	row := "SETUP,doc-1,1,HSET,doc:1,vec1," + binaryArgMarker + enc + ",vec2," + binaryArgMarker + enc
	_, _, _, _, _, _, args, _, err := preProcessCmd(row)
	if err != nil {
		t.Fatalf("preProcessCmd returned error: %v", err)
	}
	// args = [doc:1, vec1, <raw>, vec2, <raw>]
	if len(args) != 5 {
		t.Fatalf("expected 5 args, got %d: %q", len(args), args)
	}
	if args[2] != string(rawBinary) || args[4] != string(rawBinary) {
		t.Fatal("expected both marked vector args to be decoded to raw bytes")
	}
}

// Backward-compat regression guard: a plain row with NO marker parses exactly
// as before — values untouched, key correct, bytelen = len(row)-len(cmdType).
func TestPreProcessCmdPlainRowUnchanged(t *testing.T) {
	row := "SETUP,doc-1,1,HSET,doc:1,title,hello world"
	cmdType, _, _, cmd, key, _, args, bytelen, err := preProcessCmd(row)
	if err != nil {
		t.Fatalf("preProcessCmd returned error: %v", err)
	}
	if cmdType != "SETUP" || cmd != "HSET" || key != "doc:1" {
		t.Fatalf("unexpected parse: cmdType=%q cmd=%q key=%q", cmdType, cmd, key)
	}
	if len(args) != 3 || args[0] != "doc:1" || args[1] != "title" || args[2] != "hello world" {
		t.Fatalf("plain args altered: %q", args)
	}
	if want := uint64(len(row)) - uint64(len("SETUP")); bytelen != want {
		t.Fatalf("bytelen = %d, want %d (no shrink expected without a marker)", bytelen, want)
	}
}

// Documents the reserved-token behavior: a value whose literal prefix is
// __b64__ IS interpreted as a marker. Pins the conscious design decision so any
// future change to it is deliberate.
func TestPreProcessCmdMarkerPrefixIsReserved(t *testing.T) {
	// "__b64__SGVsbG8=" -> decodes "SGVsbG8=" -> "Hello".
	row := "SETUP,doc-1,1,HSET,doc:1,title," + binaryArgMarker + "SGVsbG8="
	_, _, _, _, _, _, args, _, err := preProcessCmd(row)
	if err != nil {
		t.Fatalf("preProcessCmd returned error: %v", err)
	}
	if args[2] != "Hello" {
		t.Fatalf("expected reserved marker prefix to decode to %q, got %q", "Hello", args[2])
	}
}

// A too-short row returns an error (previously silently discarded at the call
// site). Pins the len<3 branch.
func TestPreProcessCmdShortRowReturnsError(t *testing.T) {
	if _, _, _, _, _, _, _, _, err := preProcessCmd("a,b"); err == nil {
		t.Fatal("expected an error for a row with fewer than 3 fields, got nil")
	}
}

// A key position derived from an out-of-range pos field must return an error,
// not panic the worker goroutine (which has no recover).
func TestPreProcessCmdKeyPosOutOfRangeReturnsError(t *testing.T) {
	// pos=4 -> keyPos=7, but the row has only 7 fields (indices 0..6).
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("preProcessCmd panicked instead of returning an error: %v", r)
		}
	}()
	if _, _, _, _, _, _, _, _, err := preProcessCmd("SETUP,q,4,HSET,doc:1,f,v"); err == nil {
		t.Fatal("expected an out-of-range key position to return an error, got nil")
	}
}

// Pins keyPos math + cluster-slot computation for a READ row (no prior test
// covered slot computation).
func TestPreProcessCmdReadComputesClusterSlot(t *testing.T) {
	row := "READ,q1,1,FT.SEARCH,idx,*"
	cmdType, _, keyPos, cmd, key, clusterSlot, _, _, err := preProcessCmd(row)
	if err != nil {
		t.Fatalf("preProcessCmd returned error: %v", err)
	}
	if cmdType != "READ" || cmd != "FT.SEARCH" || key != "idx" || keyPos != 4 {
		t.Fatalf("unexpected parse: cmdType=%q cmd=%q key=%q keyPos=%d", cmdType, cmd, key, keyPos)
	}
	if want := int(radix.ClusterSlot([]byte("idx"))); clusterSlot != want {
		t.Fatalf("clusterSlot = %d, want %d", clusterSlot, want)
	}
}

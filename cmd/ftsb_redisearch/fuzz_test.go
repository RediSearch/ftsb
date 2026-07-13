package main

import "testing"

// FuzzPreProcessCmd hammers the CSV input-row parser with arbitrary bytes. The
// input file is untrusted (hand-crafted or generator-produced), so preProcessCmd
// must never panic regardless of the row -- it should return an error instead.
// This is the target that would have caught the historical out-of-range keyPos
// panic (a malformed `pos` column indexing argsStr) before it shipped.
func FuzzPreProcessCmd(f *testing.F) {
	seeds := []string{
		"SETUP,setup-doc-1,1,HSET,doc:1,vec,__b64__zczMPc3MTD6amZk+zczMPg==",
		"WRITE,W1,2,FT.ADD,idx,doc1,1.0,FIELDS,title,hello world",
		"READ,knn-1,1,FT.SEARCH,idx,*=>[KNN 10 @vec $BLOB],PARAMS,2,BLOB,__b64__zczMPg==,DIALECT,2",
		"WRITE,w1,1,HSET,doc:1,title,\"AAAAA ,BBBBBBBBB\"",
		"SETUP,q,4,HSET,doc:1,f,v", // pos=4 -> keyPos out of range
		"a,b",                      // too few fields
		"",                         // empty
		",,,,",                     // all empty fields
		"WRITE,w,-5,HSET,k",        // negative pos
		"WRITE,w,999999999999,HSET,k",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, row string) {
		// Must not panic for any input. A malformed row returns err != nil.
		cmdType, _, keyPos, _, key, _, args, _, err := preProcessCmd(row)
		if err != nil {
			return
		}
		// Light consistency checks on the success path: the returned key, when a
		// key was parsed, must be a real element the parser looked at (it never
		// fabricates), and keyPos must be non-negative when used.
		if key != "" && keyPos < 0 {
			t.Fatalf("negative keyPos %d with non-empty key %q (cmdType=%q, args=%q)", keyPos, key, cmdType, args)
		}
	})
}

// FuzzDecodeBinaryArgs fuzzes the `__b64__` marker decoder. A marked argument
// with arbitrary bytes after the marker must never panic -- invalid or empty
// base64 must be reported as an error, and the reported shrink must never exceed
// the original argument length (it would underflow the caller's byte accounting).
func FuzzDecodeBinaryArgs(f *testing.F) {
	seeds := []string{
		"__b64__zczMPc3MTD6amZk+zczMPg==",
		"__b64__", // empty payload
		"__b64__not-valid-base64!!",
		"__b64__" + "SGVsbG8=",
		"plain value",
		"__b64",       // near-miss marker
		"x__b64__abc", // marker not at prefix
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, arg string) {
		args := []string{arg}
		shrink, err := decodeBinaryArgs(args)
		if err != nil {
			return
		}
		// On success, shrink counts bytes removed by decoding one arg; it can
		// never exceed the original marked arg's length.
		if shrink > uint64(len(arg)) {
			t.Fatalf("shrink %d exceeds original arg length %d for %q", shrink, len(arg), arg)
		}
	})
}

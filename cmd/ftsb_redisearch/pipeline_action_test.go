package main

import (
	"errors"
	"net"
	"testing"

	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp"
)

// fakeConn is a minimal radix.Conn that returns canned per-Decode errors, so we
// can drive pipelineErrs.Run deterministically through the transport-error paths
// that a live Redis makes hard to reproduce (issue #118 timeout-tail coverage).
type fakeConn struct {
	encodeErr  error
	decodeErrs []error // one per Decode call, in order
	decodeAt   int
}

func (f *fakeConn) Encode(resp.Marshaler) error { return f.encodeErr }
func (f *fakeConn) Decode(resp.Unmarshaler) error {
	var e error
	if f.decodeAt < len(f.decodeErrs) {
		e = f.decodeErrs[f.decodeAt]
	}
	f.decodeAt++
	return e
}
func (f *fakeConn) Do(a radix.Action) error { return a.Run(f) }
func (f *fakeConn) Close() error            { return nil }
func (f *fakeConn) NetConn() net.Conn       { return nil }

func newPE(n int) *pipelineErrs {
	cmds := make([]radix.CmdAction, n)
	for i := range cmds {
		cmds[i] = radix.Cmd(nil, "PING")
	}
	return &pipelineErrs{cmds: cmds, errs: make([]error, n)}
}

// A transport error mid-batch fails that command and the ones after it (the
// connection is broken), while already-decoded commands stay successful. This
// is exactly the timeout-tail the WRONGTYPE test can't reach.
func TestPipelineErrsRunRecordsPerCommandTransportError(t *testing.T) {
	timeout := errors.New("read tcp: i/o timeout")
	reset := errors.New("read tcp: connection reset by peer")
	pe := newPE(3)
	fc := &fakeConn{decodeErrs: []error{nil, timeout, reset}} // cmd0 ok, cmd1 times out, cmd2 on broken conn

	err := pe.Run(fc)

	if !pe.ran {
		t.Error("ran = false after Run, want true")
	}
	if err != timeout {
		t.Errorf("Run returned %v, want the first error %v", err, timeout)
	}
	if pe.errs[0] != nil {
		t.Errorf("errs[0] = %v, want nil (command succeeded)", pe.errs[0])
	}
	if pe.errs[1] != timeout || pe.errs[2] != reset {
		t.Errorf("errs = %v, want [nil, timeout, reset] (broken-conn tail must each be recorded, not blamed on cmd0 or dropped)", pe.errs)
	}
	if fc.decodeAt != 3 {
		t.Errorf("decoded %d replies, want 3 (every reply must be decoded, not drained)", fc.decodeAt)
	}
}

// A single RESP error fails only its own command; the rest stay successful.
func TestPipelineErrsRunRecordsSingleRespError(t *testing.T) {
	wrongtype := errors.New("WRONGTYPE Operation against a key ...")
	pe := newPE(4)
	fc := &fakeConn{decodeErrs: []error{nil, wrongtype, nil, nil}}

	pe.Run(fc)

	got := 0
	for _, e := range pe.errs {
		if e != nil {
			got++
		}
	}
	if got != 1 || pe.errs[1] != wrongtype {
		t.Errorf("errs = %v, want exactly errs[1]=WRONGTYPE (one bad reply must not fail its siblings)", pe.errs)
	}
}

// A write failure breaks the connection before any reply, so every command fails.
func TestPipelineErrsRunEncodeFailureMarksAllFailed(t *testing.T) {
	boom := errors.New("write tcp: broken pipe")
	pe := newPE(3)
	fc := &fakeConn{encodeErr: boom}

	if err := pe.Run(fc); err != boom {
		t.Errorf("Run returned %v, want %v", err, boom)
	}
	for i, e := range pe.errs {
		if e != boom {
			t.Errorf("errs[%d] = %v, want %v (a write failure fails the whole batch)", i, e, boom)
		}
	}
	if fc.decodeAt != 0 {
		t.Errorf("decoded %d replies after an encode failure, want 0", fc.decodeAt)
	}
}

// Before Run executes, ran is false and errs is all-nil -- this is the state
// flushPending detects (via !pe.ran) to attribute a pre-Run client.Do failure
// (e.g. connection acquisition during an outage) to the whole window instead of
// silently counting the batch as successful.
func TestPipelineErrsUnrunHasRanFalseAndNilErrs(t *testing.T) {
	pe := newPE(5)
	if pe.ran {
		t.Error("ran = true before Run, want false")
	}
	for i, e := range pe.errs {
		if e != nil {
			t.Errorf("errs[%d] = %v before Run, want nil", i, e)
		}
	}
}

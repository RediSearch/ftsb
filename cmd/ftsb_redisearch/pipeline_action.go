package main

import (
	"io"

	"github.com/mediocregopher/radix/v3"
)

// pipelineErrs runs a batch of commands as a single pipeline (one buffered write
// of every command, one batched read of every reply) but, unlike radix.Pipeline,
// it decodes EVERY reply and records each command's individual error instead of
// stopping at the first one.
//
// radix.Pipeline's Run stops decoding at the first failing command and DRAINS
// (discards) the remaining replies, returning a single error for the whole
// batch. ftsb previously attributed that one error to every command in the
// window, so a single WRONGTYPE/OOM reply inflated the error count by up to
// `pipeline` per occurrence (issue #118). Here errs[i] is populated iff command
// i's reply was an error, so accounting is per-command exact.
//
// The write is still a single flush of all commands (via multiMarshal), so this
// keeps the pipelining benefit; the read cost is identical to radix.Pipeline,
// which also decodes/drains all N replies.
type pipelineErrs struct {
	cmds []radix.CmdAction
	errs []error // len(cmds); errs[i] != nil iff command i failed.
	ran  bool    // set once Run starts; distinguishes "executed, errs is authoritative" from "client.Do failed before Run (e.g. connection acquisition), whole batch failed".
}

// multiMarshal marshals a batch of CmdActions into one RESP write so the whole
// pipeline is sent in a single flush (mirrors radix.Pipeline's own encode path).
type multiMarshal []radix.CmdAction

func (m multiMarshal) MarshalRESP(w io.Writer) error {
	for _, cmd := range m {
		if err := cmd.MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

// Keys returns the union of the batch's keys (radix uses this only for cluster
// routing; ftsb pins a whole batch to one connection so this is informational).
func (p *pipelineErrs) Keys() []string {
	var keys []string
	for _, cmd := range p.cmds {
		keys = append(keys, cmd.Keys()...)
	}
	return keys
}

func (p *pipelineErrs) Run(c radix.Conn) error {
	p.ran = true
	// One buffered write of the whole batch.
	if err := c.Encode(multiMarshal(p.cmds)); err != nil {
		// A write failure breaks the connection, so no reply is coming for any
		// command in the batch: every command failed.
		for i := range p.cmds {
			p.errs[i] = err
		}
		return err
	}
	// One batched read: decode every reply so per-command errors are captured.
	// A RESP error (e.g. WRONGTYPE) fails only its own command and leaves the
	// connection healthy for the rest. A transport error (e.g. i/o timeout)
	// breaks the connection, so that command and every command after it fail;
	// the ones already decoded succeeded. Either way errs reflects reality
	// per-command instead of blaming the whole window.
	var first error
	for i, cmd := range p.cmds {
		if err := c.Decode(cmd); err != nil {
			p.errs[i] = err
			if first == nil {
				first = err
			}
		}
	}
	return first
}

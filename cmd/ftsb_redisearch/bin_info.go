package main

import (
	"strconv"
	"strings"
)

// Vars only for git sha and diff handling
var GitSHA1 string = ""
var GitDirty string = "0"

// internal function to return value of GitSHA1 var, which is filled in link time
func toolGitSHA1() string {
	return GitSHA1
}

// this internal function will check for the number of altered lines that are not yet committed
// and return true in that case
func toolGitDirty() (dirty bool) {
	dirty = false
	dirtyLines, err := strconv.Atoi(strings.TrimSpace(GitDirty))
	if err == nil {
		dirty = (dirtyLines != 0)
	}
	return
}

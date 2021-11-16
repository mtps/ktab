package versioninfo

import (
	"fmt"
	"runtime"
	"strings"
)

var gitBranch string
var gitState string
var gitSummary string
var buildDate string
var gitCommit string

type VersionInfo struct {
	State   string
	Summary string
	Built   string
	Commit  string
	Branch  string
}

var current VersionInfo

func init() {
	current = VersionInfo{
		Branch: gitBranch,
		State: gitState,
		Summary: gitSummary,
		Built: buildDate,
		Commit: gitCommit,
	}
}

func Current() VersionInfo {
	return current
}

func (t VersionInfo) String() string {
	o := &strings.Builder{}
	field := func(name, value string) string {
		return fmt.Sprintf("%-12s : %s", name, value)
	}

	_, _ = fmt.Fprintln(o, "")
	_, _ = fmt.Fprintln(o, field("Go-Version", runtime.Version()))
	_, _ = fmt.Fprintln(o, field("Go-Arch", runtime.GOARCH))
	_, _ = fmt.Fprintln(o, field("Go-Os", runtime.GOOS))
	_, _ = fmt.Fprintln(o, field("BuildDate", t.Built))
	_, _ = fmt.Fprintln(o, field("Git-Commit", t.Commit))
	_, _ = fmt.Fprintln(o, field("Git-Branch", t.Branch))
	_, _ = fmt.Fprintln(o, field("Git-State", t.State))
	_, _ = fmt.Fprintln(o, field("Git-Summary", t.Summary))

	return o.String()
}

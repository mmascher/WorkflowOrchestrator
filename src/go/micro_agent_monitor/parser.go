package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

// HTCondor user log event codes (from Job Event Log Codes)
const (
	ULOG_SUBMIT         = 0
	ULOG_EXECUTE        = 1
	ULOG_JOB_EVICTED    = 4
	ULOG_JOB_TERMINATED = 5
	ULOG_IMAGE_SIZE     = 6
	ULOG_JOB_ABORTED    = 9
	ULOG_JOB_AD         = 28
)

// Event represents a parsed condor user log event
type Event struct {
	EventCode int
	Cluster   int
	Proc      int
	Subproc   int
	Timestamp string
	Message   string
	Extra     map[string]string
}

// CondorLogParser parses HTCondor user log files
type CondorLogParser struct {
	LogPath      string
	LastPosition int64
}

var eventRe = regexp.MustCompile(`^(\d{3})\s+\((\d+)\.(\d+)\.(\d+)\)\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(.*)$`)
var kvRe = regexp.MustCompile(`^(\w+)\s*=\s*(.*)$`)

// ParseEvent parses a condor user log event header line
func ParseEvent(line string) *Event {
	matches := eventRe.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}
	eventCode, _ := strconv.Atoi(matches[1])
	cluster, _ := strconv.Atoi(matches[2])
	proc, _ := strconv.Atoi(matches[3])
	subproc, _ := strconv.Atoi(matches[4])
	return &Event{
		EventCode: eventCode,
		Cluster:   cluster,
		Proc:      proc,
		Subproc:   subproc,
		Timestamp: matches[5],
		Message:   trimSpace(matches[6]),
		Extra:     make(map[string]string),
	}
}

func trimSpace(s string) string {
	return string(bytes.TrimSpace([]byte(s)))
}

func parseKeyValue(line string) (key, val string, ok bool) {
	line = trimSpace(line)
	matches := kvRe.FindStringSubmatch(line)
	if matches == nil {
		return "", "", false
	}
	val = trimSpace(matches[2])
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		val = val[1 : len(val)-1]
	}
	return matches[1], val, true
}

// readKeyValuesUntilStop reads Key=Value lines, stops at next event or empty line.
// Returns the next line to process (event header or first line after empty), or "" if exhausted.
func (p *CondorLogParser) readKeyValuesUntilStop(scanner *bufio.Scanner, extra map[string]string) string {
	for scanner.Scan() {
		line := scanner.Text()
		if ParseEvent(line) != nil {
			return line
		}
		if trimSpace(line) == "" {
			if scanner.Scan() {
				return scanner.Text()
			}
			return ""
		}
		if k, v, ok := parseKeyValue(line); ok {
			extra[k] = v
		}
	}
	return ""
}

// IterLogFile yields events from the log file
func (p *CondorLogParser) IterLogFile(startOffset int64) ([]Event, error) {
	absPath, err := filepath.Abs(p.LogPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(absPath)
	if err != nil || info.IsDir() {
		return nil, nil
	}

	f, err := os.Open(absPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if startOffset > 0 {
		if _, err := f.Seek(startOffset, 0); err != nil {
			return nil, err
		}
	}

	var events []Event
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	line := ""
	for {
		if line == "" {
			if !scanner.Scan() {
				break
			}
			line = scanner.Text()
		}

		ev := ParseEvent(line)
		if ev == nil {
			line = ""
			continue
		}

		line = p.readKeyValuesUntilStop(scanner, ev.Extra)

		// 005 (JOB_TERMINATED) is followed by 028 (Job ad) for same job - absorb into extra
		if line != "" {
			nextEv := ParseEvent(line)
			if nextEv != nil && ev.EventCode == ULOG_JOB_TERMINATED && nextEv.EventCode == ULOG_JOB_AD &&
				ev.Cluster == nextEv.Cluster && ev.Proc == nextEv.Proc {
				line = p.readKeyValuesUntilStop(scanner, ev.Extra)
			}
		}

		events = append(events, *ev)
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return events, err
	}
	if pos, err := f.Seek(0, 1); err == nil {
		p.LastPosition = pos
	}
	return events, nil
}

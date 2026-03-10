package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const fullRereadInterval = 3600 // 1 hour

// Monitor coordinates log parsing and file DB
type Monitor struct {
	CondorLogFile  string
	ResultsDir     string
	RequestPath    string
	DB             *FileDB
	Parser         *CondorLogParser
	KeepOutputSteps map[string]bool
}

// NewMonitor creates a new Monitor
func NewMonitor(logPath, resultsDir, dbPath, requestPath string) (*Monitor, error) {
	absLog, _ := filepath.Abs(logPath)
	absResults, _ := filepath.Abs(resultsDir)
	absRequest, _ := filepath.Abs(requestPath)

	keepSteps, err := LoadKeepOutputSteps(requestPath)
	if err != nil {
		return nil, err
	}

	db, err := OpenFileDB(dbPath)
	if err != nil {
		return nil, err
	}

	return &Monitor{
		CondorLogFile:   absLog,
		ResultsDir:      absResults,
		RequestPath:    absRequest,
		DB:             db,
		Parser:         &CondorLogParser{LogPath: absLog},
		KeepOutputSteps: keepSteps,
	}, nil
}

// Close closes the monitor's database
func (m *Monitor) Close() error {
	return m.DB.Close()
}

func (m *Monitor) handleTerminatedJob(cluster, proc int, extra map[string]string, processedJobs map[string]bool) int {
	key := fmt.Sprintf("%d.%d", cluster, proc)
	if processedJobs != nil {
		if processedJobs[key] {
			return 0
		}
		processedJobs[key] = true
	}

	returnValue := -1
	if rv, ok := extra["ReturnValue"]; ok {
		returnValue, _ = strconv.Atoi(rv)
	}
	glideinCmssite := extra["JOB_GLIDEIN_Site"]
	if glideinCmssite == "" {
		glideinCmssite = extra["JOB_Site"]
	}

	ok, result := m.DB.ProcessTerminatedJob(m.ResultsDir, m.RequestPath, cluster, proc, returnValue,
		glideinCmssite, m.KeepOutputSteps)
	if ok {
		log.Printf("Job %d.%d terminated (exit=%d): stored %v files", cluster, proc, returnValue, result)
		return 1
	}
	log.Printf("Job %d.%d: %v", cluster, proc, result)
	return 0
}

// RunOnce does a single pass over the log file
func (m *Monitor) RunOnce() int {
	if info, err := os.Stat(m.CondorLogFile); err != nil || info.IsDir() {
		log.Printf("Log file not found: %s", m.CondorLogFile)
		return 1
	}

	events, err := m.Parser.IterLogFile(0)
	if err != nil {
		log.Printf("Parse error: %v", err)
		return 1
	}

	processed := 0
	for _, ev := range events {
		if ev.EventCode == ULOG_JOB_TERMINATED {
			processed += m.handleTerminatedJob(ev.Cluster, ev.Proc, ev.Extra, nil)
		}
	}
	log.Printf("Processed %d terminated jobs", processed)
	return 0
}

// RunDaemon polls the log file and processes new events
func (m *Monitor) RunDaemon(pollInterval int) int {
	log.Printf("Daemon mode: watching %s (poll every %ds, full re-read every %ds)",
		m.CondorLogFile, pollInterval, fullRereadInterval)

	lastPosition := int64(0)
	lastFullReread := time.Now()
	processedJobs := make(map[string]bool)

	for {
		info, err := os.Stat(m.CondorLogFile)
		if err != nil || info.IsDir() {
			time.Sleep(time.Duration(pollInterval) * time.Second)
			continue
		}
		size := info.Size()
		if size > lastPosition {
			doFull := time.Since(lastFullReread) >= fullRereadInterval*time.Second
			startOffset := int64(0)
			if !doFull {
				startOffset = lastPosition
			} else {
				lastFullReread = time.Now()
			}

			events, err := m.Parser.IterLogFile(startOffset)
			if err != nil {
				log.Printf("Parse error: %v", err)
			} else {
				for _, ev := range events {
					if ev.EventCode != ULOG_IMAGE_SIZE && ev.EventCode != ULOG_JOB_AD {
						// debug: log event
					}
					if ev.EventCode == ULOG_JOB_TERMINATED {
						m.handleTerminatedJob(ev.Cluster, ev.Proc, ev.Extra, processedJobs)
					}
				}
				lastPosition = m.Parser.LastPosition
			}
		}
		time.Sleep(time.Duration(pollInterval) * time.Second)
	}
}

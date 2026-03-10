// Micro Agent Monitor (MAM) - Go implementation
//
// Monitors HTCondor job log files and records file-level processing information
// in a local SQLite database. Parses condor user log for JOB_TERMINATED events,
// reads framework job_report JSON, and stores output file info.
//
// Usage:
//
//	micro_agent_monitor --log log/run.10372180 --results-dir results --db micro_agent.db --request request.json
//	micro_agent_monitor --log log/run.10372180 --results-dir results --db micro_agent.db --request request.json --once
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	logPath := flag.String("log", "", "Path to condor log file (e.g. log/run.10372180)")
	resultsDir := flag.String("results-dir", "results", "Directory containing job_report.<C>.<P>.<N>.json")
	dbPath := flag.String("db", "micro_agent.db", "SQLite database path")
	requestPath := flag.String("request", "", "Request.json path (required); only store outputs from steps with KeepOutput==True")
	logFile := flag.String("log-file", "", "Write log to file")
	var verbose bool
	flag.BoolVar(&verbose, "verbose", false, "Show verbose output")
	flag.BoolVar(&verbose, "v", false, "alias for -verbose")
	once := flag.Bool("once", false, "Single pass over log, then exit (default: daemon mode)")
	poll := flag.Int("poll", 10, "Poll interval in seconds for daemon mode")
	flag.Parse()

	if *logPath == "" || *requestPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --log and --request are required\n")
		flag.Usage()
		os.Exit(1)
	}

	if _, err := os.Stat(*requestPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: request file not found: %s\n", *requestPath)
		os.Exit(1)
	}

	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not open log file: %v\n", err)
		} else {
			log.SetOutput(f)
			defer f.Close()
		}
	}
	if verbose {
		log.SetFlags(log.LstdFlags)
	}

	monitor, err := NewMonitor(*logPath, *resultsDir, *dbPath, *requestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer monitor.Close()

	if *once {
		os.Exit(monitor.RunOnce())
	}

	// Daemon runs until interrupted
	go monitor.RunDaemon(*poll)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Stopping daemon (interrupted)")
	os.Exit(0)
}

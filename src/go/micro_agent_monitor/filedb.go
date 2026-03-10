package main

import (
	"database/sql"
	"log"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS processed_files (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	condor_job_id TEXT NOT NULL,
	lfn TEXT,
	pfn TEXT,
	step_name TEXT,
	events INTEGER,
	size INTEGER,
	module_label TEXT,
	glidein_cmssite TEXT,
	pnn TEXT,
	job_exit_code INTEGER,
	created_at TEXT DEFAULT CURRENT_TIMESTAMP,
	UNIQUE(condor_job_id, lfn, pfn, step_name)
);
CREATE INDEX IF NOT EXISTS idx_files_lfn ON processed_files(lfn);
CREATE INDEX IF NOT EXISTS idx_files_job ON processed_files(condor_job_id);
CREATE INDEX IF NOT EXISTS idx_files_pnn ON processed_files(pnn);
`

// FileDB is the SQLite database for processed files
type FileDB struct {
	db *sql.DB
}

// OpenFileDB opens or creates the file database
func OpenFileDB(dbPath string) (*FileDB, error) {
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", absPath)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, err
	}
	return &FileDB{db: db}, nil
}

// Close closes the database
func (f *FileDB) Close() error {
	return f.db.Close()
}

// InsertFiles inserts file records, ignoring duplicates
func (f *FileDB) InsertFiles(condorJobID string, files []FileInfo, jobExitCode int, glideinCmssite string) error {
	stmt, err := f.db.Prepare(`INSERT OR IGNORE INTO processed_files
		(condor_job_id, lfn, pfn, step_name, events, size, module_label, glidein_cmssite, pnn, job_exit_code)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, file := range files {
		var events, size interface{}
		if file.Events != nil {
			events = *file.Events
		}
		if file.Size != nil {
			size = *file.Size
		}
		_, err := stmt.Exec(condorJobID, file.LFN, file.PFN, file.StepName, events, size,
			file.ModuleLabel, glideinCmssite, file.PNN, jobExitCode)
		if err != nil {
			log.Printf("Insert file warning: %v", err)
		}
	}
	return nil
}

// ProcessTerminatedJob processes a JOB_TERMINATED event
func (f *FileDB) ProcessTerminatedJob(resultsDir, requestPath string, cluster, proc, returnValue int,
	glideinCmssite string, keepOutputSteps map[string]bool) (bool, interface{}) {

	reportPath := FindJobReport(resultsDir, cluster, proc)
	if reportPath == "" {
		return false, "job_report not found"
	}

	files, err := ExtractFiles(reportPath, keepOutputSteps)
	if err != nil {
		return false, err.Error()
	}

	// Build LFN for files with empty LFN
	for i := range files {
		if files[i].LFN == "" {
			built := BuildLFNForFile(&files[i], requestPath)
			if built != "" {
				files[i].LFN = built
			}
		}
	}

	condorJobID := strconv.Itoa(cluster) + "." + strconv.Itoa(proc)
	if err := f.InsertFiles(condorJobID, files, returnValue, glideinCmssite); err != nil {
		return false, err.Error()
	}
	return true, len(files)
}

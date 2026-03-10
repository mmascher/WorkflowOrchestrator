package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FileInfo holds extracted file information from job report
type FileInfo struct {
	LFN         string
	PFN         string
	PNN         string
	StepName    string
	Events      *int
	Size        *int
	ModuleLabel string
}

// FindJobReport finds the job_report JSON for (cluster, proc)
func FindJobReport(resultsDir string, cluster, proc int) string {
	prefix := "job_report." + strconv.Itoa(cluster) + "." + strconv.Itoa(proc) + "."
	bestPath := ""
	bestN := -1

	entries, err := os.ReadDir(resultsDir)
	if err != nil {
		return ""
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".json") {
			continue
		}
		suffix := name[len(prefix) : len(name)-5]
		n := 0
		if suffix != "" {
			n, err = strconv.Atoi(suffix)
			if err != nil {
				continue
			}
		}
		if n > bestN {
			bestN = n
			bestPath = filepath.Join(resultsDir, name)
		}
	}
	return bestPath
}

// ExtractFiles extracts output file information from job_report.json
func ExtractFiles(reportPath string, keepOutputSteps map[string]bool) ([]FileInfo, error) {
	data, err := os.ReadFile(reportPath)
	if err != nil {
		return nil, err
	}
	var report map[string]interface{}
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, err
	}

	steps, _ := report["steps"].(map[string]interface{})
	if steps == nil {
		return nil, nil
	}

	var files []FileInfo
	for stepName, stepData := range steps {
		if !keepOutputSteps[stepName] {
			continue
		}
		stepMap, ok := stepData.(map[string]interface{})
		if !ok {
			continue
		}
		output, _ := stepMap["output"].(map[string]interface{})
		if output == nil {
			continue
		}

		for modName, fileList := range output {
			var list []interface{}
			switch v := fileList.(type) {
			case []interface{}:
				list = v
			default:
				continue
			}

			for _, fi := range list {
				fim, ok := fi.(map[string]interface{})
				if !ok {
					continue
				}
				lfn := getStrFromMap(fim, "lfn", "LFN", "logicalFileName")
				pfn := getStrFromMap(fim, "pfn", "PFN", "fileName", "physicalFileName")
				if lfn == "" && pfn == "" {
					continue
				}
				pnn := getStrFromMap(fim, "pnn", "PNN")
				events := getIntFromMap(fim, "events", "EventsWritten")
				size := getIntFromMap(fim, "size", "Size", "SizeBytes")

				files = append(files, FileInfo{
					LFN:         lfn,
					PFN:         pfn,
					PNN:         pnn,
					StepName:    stepName,
					Events:      events,
					Size:        size,
					ModuleLabel: modName,
				})
			}
		}
	}
	return files, nil
}

func getStrFromMap(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok {
			return v
		}
	}
	return ""
}

func getIntFromMap(m map[string]interface{}, keys ...string) *int {
	for _, k := range keys {
		switch v := m[k].(type) {
		case float64:
			n := int(v)
			return &n
		case int:
			return &v
		}
	}
	return nil
}

package main

import (
	"strings"
)

// BuildLFN builds LFN for a workflow output file
func BuildLFN(base, era, primary, proc, outModule string) string {
	base = strings.TrimSuffix(base, "/")
	tier := outModule
	if strings.HasSuffix(outModule, "output") {
		tier = strings.TrimSuffix(outModule, "output")
	}
	return base + "/" + era + "/" + primary + "/" + tier + "/" + proc + "-v3/" + outModule + ".root"
}

// BuildLFNForFile builds LFN for a FileInfo from job_report when lfn is empty
func BuildLFNForFile(f *FileInfo, requestPath string) string {
	if f.LFN != "" {
		return f.LFN
	}
	if f.ModuleLabel == "" {
		return ""
	}
	config := LoadStepConfig(requestPath, f.StepName)
	if config == nil {
		return ""
	}
	return BuildLFN(config.Base, config.Era, config.Primary, config.Proc, f.ModuleLabel)
}

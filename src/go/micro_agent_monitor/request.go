package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// LoadKeepOutputSteps loads step names with KeepOutput==True from request.json
func LoadKeepOutputSteps(requestPath string) (map[string]bool, error) {
	data, err := os.ReadFile(requestPath)
	if err != nil {
		return nil, err
	}
	var req map[string]interface{}
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	steps := make(map[string]bool)
	for i := 1; i < 20; i++ {
		key := "Step" + itoa(i)
		stepCfg, ok := req[key].(map[string]interface{})
		if !ok {
			break
		}
		if keep, _ := stepCfg["KeepOutput"].(bool); keep {
			steps["step"+itoa(i)] = true
		}
	}
	return steps, nil
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

// StepConfig holds step configuration for LFN building
type StepConfig struct {
	Base     string
	Era      string
	Primary  string
	Proc     string
}

// LoadStepConfig loads step config from request.json for a given step name
func LoadStepConfig(requestPath, stepName string) *StepConfig {
	if requestPath == "" || stepName == "" {
		return nil
	}
	if !strings.HasPrefix(stepName, "step") || len(stepName) < 5 {
		return nil
	}
	stepNum := 0
	for _, c := range stepName[4:] {
		if c >= '0' && c <= '9' {
			stepNum = stepNum*10 + int(c-'0')
		} else {
			return nil
		}
	}
	if stepNum == 0 {
		return nil
	}

	data, err := os.ReadFile(requestPath)
	if err != nil {
		return nil
	}
	var req map[string]interface{}
	if json.Unmarshal(data, &req) != nil {
		return nil
	}

	key := "Step" + itoa(stepNum)
	step, ok := req[key].(map[string]interface{})
	if !ok {
		return nil
	}

	base := "/store/unmerged"
	if b, ok := req["UnmergedLFNBase"].(string); ok && b != "" {
		base = strings.TrimSuffix(b, "/")
	}

	config := &StepConfig{Base: base}
	if e, ok := step["AcquisitionEra"].(string); ok {
		config.Era = e
	}
	if p, ok := step["PrimaryDataset"].(string); ok {
		config.Primary = p
	}
	if pr, ok := step["ProcessingString"].(string); ok {
		config.Proc = pr
	}
	return config
}

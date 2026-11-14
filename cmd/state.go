package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Seed statuses recorded in the progress file.
const (
	seedStatusPending    = "pending"
	seedStatusInProgress = "in_progress"
	seedStatusDone       = "done"
	seedStatusFailed     = "failed"
)

// seedState is the persisted progress of one seed.
type seedState struct {
	URL        string `json:"url"`
	Status     string `json:"status"`
	NextURL    string `json:"nextUrl,omitempty"`
	NextPage   int    `json:"nextPage,omitempty"`
	FailedPage string `json:"failedPage,omitempty"`
	Error      string `json:"error,omitempty"`
}

// progressState is the durable progress file. It is rewritten atomically
// after every record appended to the output file.
type progressState struct {
	Version   int         `json:"version"`
	OutOffset int64       `json:"outOffset"`
	Pages     int         `json:"pages"`
	Deduped   int         `json:"deduped"`
	Seen      []string    `json:"seen"`
	Seeds     []seedState `json:"seeds"`
}

// loadProgress reads the progress file and validates it against the current
// seed list. It returns nil when there is no usable previous progress.
func loadProgress(path string, seeds []string) *progressState {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var state progressState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil
	}
	if state.Version != 1 || len(state.Seeds) != len(seeds) {
		return nil
	}
	for i, seed := range seeds {
		if state.Seeds[i].URL != seed {
			return nil
		}
	}
	return &state
}

// freshProgress initializes progress for a new run.
func freshProgress(seeds []string) *progressState {
	state := &progressState{Version: 1, Seen: []string{}}
	for _, seed := range seeds {
		state.Seeds = append(state.Seeds, seedState{URL: seed, Status: seedStatusPending})
	}
	return state
}

// saveProgress atomically persists the progress file via write and rename.
func saveProgress(path string, state *progressState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode progress: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("failed to write progress: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("failed to replace progress file: %w", err)
	}
	return nil
}

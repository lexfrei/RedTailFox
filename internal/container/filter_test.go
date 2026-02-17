package container

import (
	"testing"

	apitypes "github.com/moby/moby/api/types/container"
)

func TestFilterByPrefix_MatchesPrefix(t *testing.T) {
	containers := []apitypes.Summary{
		{ID: "abc123", Names: []string{"/fox_worker_1"}},
		{ID: "def456", Names: []string{"/fox_worker_2"}},
		{ID: "ghi789", Names: []string{"/other_container"}},
	}

	result := filterByPrefix(containers, "fox_worker")

	if len(result) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(result))
	}

	if result[0].Name != "fox_worker_1" {
		t.Errorf("expected fox_worker_1, got %s", result[0].Name)
	}

	if result[1].Name != "fox_worker_2" {
		t.Errorf("expected fox_worker_2, got %s", result[1].Name)
	}
}

func TestFilterByPrefix_NoMatch(t *testing.T) {
	containers := []apitypes.Summary{
		{ID: "abc123", Names: []string{"/other_service"}},
	}

	result := filterByPrefix(containers, "fox_worker")

	if len(result) != 0 {
		t.Fatalf("expected 0 containers, got %d", len(result))
	}
}

func TestFilterByPrefix_EmptyInput(t *testing.T) {
	result := filterByPrefix(nil, "fox_worker")

	if len(result) != 0 {
		t.Fatalf("expected 0 containers, got %d", len(result))
	}
}

func TestFilterByPrefix_StripsLeadingSlash(t *testing.T) {
	containers := []apitypes.Summary{
		{ID: "abc123", Names: []string{"/fox_worker_1"}},
	}

	result := filterByPrefix(containers, "fox_worker")

	if len(result) != 1 {
		t.Fatalf("expected 1 container, got %d", len(result))
	}

	if result[0].Name != "fox_worker_1" {
		t.Errorf("expected name without slash, got %s", result[0].Name)
	}
}

package worker_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Dark-F0X/RedTailFox/internal/model"
	"github.com/Dark-F0X/RedTailFox/internal/worker"
)

const testWorkerContainer = "fox_worker_1"

func setupWorker(t *testing.T) (*miniredis.Miniredis, *worker.Worker) {
	t.Helper()

	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	wrk := worker.New(rdb, testWorkerContainer, "CMD:fox_worker_1", testLogger())

	return srv, wrk
}

func TestHandleCommandStart(t *testing.T) {
	srv, wrk := setupWorker(t)
	ctx := context.Background()

	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("failed to marshal task: %v", err)
	}

	wrk.HandleCommand(ctx, raw)

	// Verify report was published.
	reports, err := srv.List("worker_reports")
	if err != nil {
		t.Fatalf("failed to read reports: %v", err)
	}

	if len(reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(reports))
	}

	var report model.WorkerReport

	err = json.Unmarshal([]byte(reports[0]), &report)
	if err != nil {
		t.Fatalf("failed to parse report: %v", err)
	}

	if report.Status != "started" {
		t.Errorf("expected status started, got %s", report.Status)
	}

	if report.SlotID != 1 {
		t.Errorf("expected slot ID 1, got %d", report.SlotID)
	}

	if report.ContainerName != testWorkerContainer {
		t.Errorf("expected container %s, got %s", testWorkerContainer, report.ContainerName)
	}
}

func TestHandleCommandStop(t *testing.T) {
	srv, wrk := setupWorker(t)
	ctx := context.Background()

	// Start a slot first.
	startTask := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	raw, err := json.Marshal(startTask)
	if err != nil {
		t.Fatalf("failed to marshal start task: %v", err)
	}

	wrk.HandleCommand(ctx, raw)

	// Now stop the slot.
	stopTask := model.Task{
		Command: model.CommandStop,
		SlotID:  1,
	}

	raw, err = json.Marshal(stopTask)
	if err != nil {
		t.Fatalf("failed to marshal stop task: %v", err)
	}

	wrk.HandleCommand(ctx, raw)

	reports, err := srv.List("worker_reports")
	if err != nil {
		t.Fatalf("failed to read reports: %v", err)
	}

	// Should have 2 reports: started + stopped.
	if len(reports) != 2 {
		t.Fatalf("expected 2 reports, got %d", len(reports))
	}

	var stopReport model.WorkerReport

	err = json.Unmarshal([]byte(reports[0]), &stopReport)
	if err != nil {
		t.Fatalf("failed to parse stop report: %v", err)
	}

	if stopReport.Status != "stopped" {
		t.Errorf("expected status stopped, got %s", stopReport.Status)
	}

	if stopReport.SlotID != 1 {
		t.Errorf("expected slot ID 1, got %d", stopReport.SlotID)
	}
}

func TestHandleCommandStartDuplicate(t *testing.T) {
	srv, wrk := setupWorker(t)
	ctx := context.Background()

	task := model.Task{
		Command: model.CommandStart,
		SlotID:  1,
		Config:  json.RawMessage(`{}`),
	}

	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("failed to marshal task: %v", err)
	}

	// Start slot twice.
	wrk.HandleCommand(ctx, raw)
	wrk.HandleCommand(ctx, raw)

	// Only one report should be generated (second start is skipped).
	reports, err := srv.List("worker_reports")
	if err != nil {
		t.Fatalf("failed to read reports: %v", err)
	}

	if len(reports) != 1 {
		t.Fatalf("expected 1 report (duplicate ignored), got %d", len(reports))
	}
}

func TestHandleCommandUnknown(t *testing.T) {
	_, wrk := setupWorker(t)
	ctx := context.Background()

	task := model.Task{
		Command: "unknown_command",
		SlotID:  1,
	}

	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("failed to marshal task: %v", err)
	}

	// Should not panic.
	wrk.HandleCommand(ctx, raw)
}

func TestHandleCommandInvalidJSON(t *testing.T) {
	_, wrk := setupWorker(t)
	ctx := context.Background()

	// Should not panic on invalid JSON.
	wrk.HandleCommand(ctx, []byte(`{invalid`))
}

func TestReportContainsContainerName(t *testing.T) {
	srv, wrk := setupWorker(t)
	ctx := context.Background()

	task := model.Task{
		Command: model.CommandStart,
		SlotID:  42,
		Config:  json.RawMessage(`{}`),
	}

	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("failed to marshal task: %v", err)
	}

	wrk.HandleCommand(ctx, raw)

	reports, err := srv.List("worker_reports")
	if err != nil {
		t.Fatalf("failed to read reports: %v", err)
	}

	if len(reports) < 1 {
		t.Fatal("expected at least 1 report")
	}

	var report model.WorkerReport

	err = json.Unmarshal([]byte(reports[0]), &report)
	if err != nil {
		t.Fatalf("failed to parse report: %v", err)
	}

	if report.ContainerName != testWorkerContainer {
		t.Errorf("expected container %s, got %s", testWorkerContainer, report.ContainerName)
	}

	if report.InitiatedBy != "worker" {
		t.Errorf("expected initiatedBy worker, got %s", report.InitiatedBy)
	}
}

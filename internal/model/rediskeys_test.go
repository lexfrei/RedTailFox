package model_test

import (
	"testing"

	"github.com/Dark-F0X/RedTailFox/internal/model"
)

func TestContainerNameRe_AcceptsValid(t *testing.T) {
	valid := []string{
		"fox_worker_1",
		"a",
		"container123",
		"my.container",
		"my-container",
		"my_container",
		"A1.B2-C3_D4",
	}

	for _, name := range valid {
		if !model.ContainerNameRe.MatchString(name) {
			t.Errorf("expected %q to be accepted", name)
		}
	}
}

func TestContainerNameRe_RejectsInvalid(t *testing.T) {
	invalid := []string{
		"",
		".startswithdot",
		"-startswithdash",
		"_startswithunderscore",
		"has space",
		"has\nnewline",
		"has\ttab",
		"слово",
	}

	for _, name := range invalid {
		if model.ContainerNameRe.MatchString(name) {
			t.Errorf("expected %q to be rejected", name)
		}
	}
}

package container

import (
	"strings"

	apitypes "github.com/moby/moby/api/types/container"
)

// filterByPrefix returns only containers whose name matches the given prefix.
// Container engine APIs typically prefix names with "/", so we strip that.
func filterByPrefix(containers []apitypes.Summary, prefix string) []Container {
	result := make([]Container, 0, len(containers))

	for idx := range containers {
		for _, name := range containers[idx].Names {
			cleaned := strings.TrimPrefix(name, "/")
			if strings.HasPrefix(cleaned, prefix) {
				result = append(result, Container{
					ID:   containers[idx].ID,
					Name: cleaned,
				})

				break
			}
		}
	}

	return result
}

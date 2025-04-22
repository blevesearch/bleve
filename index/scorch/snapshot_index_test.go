package scorch

import (
	"testing"

	"github.com/blevesearch/vellum"
)

func TestIndexSnapshot_getLevAutomaton(t *testing.T) {
	// Create a dummy IndexSnapshot (parent doesn't matter for this method)
	is := &IndexSnapshot{}

	tests := []struct {
		name        string
		term        string
		fuzziness   uint8
		expectError bool
		errorMsg    string // Optional: check specific error message
	}{
		{
			name:        "fuzziness 1",
			term:        "test",
			fuzziness:   1,
			expectError: false,
		},
		{
			name:        "fuzziness 2",
			term:        "another",
			fuzziness:   2,
			expectError: false,
		},
		{
			name:        "fuzziness 0",
			term:        "zero",
			fuzziness:   0,
			expectError: true,
			errorMsg:    "fuzziness exceeds the max limit",
		},
		{
			name:        "fuzziness 3",
			term:        "three",
			fuzziness:   3,
			expectError: true,
			errorMsg:    "fuzziness exceeds the max limit",
		},
		{
			name:        "empty term fuzziness 1",
			term:        "",
			fuzziness:   1,
			expectError: false,
		},
		{
			name:        "empty term fuzziness 2",
			term:        "",
			fuzziness:   2,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAutomaton, err := is.getLevAutomaton(tt.term, tt.fuzziness)

			if tt.expectError {
				if err == nil {
					t.Errorf("getLevAutomaton() expected an error but got nil")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("getLevAutomaton() expected error msg %q but got %q", tt.errorMsg, err.Error())
				}
				if gotAutomaton != nil {
					t.Errorf("getLevAutomaton() expected nil automaton on error but got %v", gotAutomaton)
				}
			} else {
				if err != nil {
					t.Errorf("getLevAutomaton() got unexpected error: %v", err)
				}
				if gotAutomaton == nil {
					t.Errorf("getLevAutomaton() expected a valid automaton but got nil")
				}
				// Optional: Check type if needed, though non-nil is usually sufficient
				_, ok := gotAutomaton.(vellum.Automaton)
				if !ok {
					t.Errorf("getLevAutomaton() returned type is not vellum.Automaton")
				}
			}
		})
	}
}

// Add other tests for snapshot_index.go below if needed...

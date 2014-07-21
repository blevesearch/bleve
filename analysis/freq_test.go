package analysis

import (
	"reflect"
	"testing"
)

func TestTokenFrequency(t *testing.T) {
	tokens := TokenStream{
		&Token{
			Term:     []byte("water"),
			Position: 1,
			Start:    0,
			End:      5,
		},
		&Token{
			Term:     []byte("water"),
			Position: 2,
			Start:    6,
			End:      11,
		},
	}
	expectedResult := TokenFrequencies{
		&TokenFreq{
			Term: []byte("water"),
			Locations: []*TokenLocation{
				&TokenLocation{
					Position: 1,
					Start:    0,
					End:      5,
				},
				&TokenLocation{
					Position: 2,
					Start:    6,
					End:      11,
				},
			},
		},
	}
	result := TokenFrequency(tokens)
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("expected %#v, got %#v", expectedResult, result)
	}
}

func TestTokenFrequenciesMergeAll(t *testing.T) {
	tf1 := TokenFrequencies{
		&TokenFreq{
			Term: []byte("water"),
			Locations: []*TokenLocation{
				&TokenLocation{
					Position: 1,
					Start:    0,
					End:      5,
				},
				&TokenLocation{
					Position: 2,
					Start:    6,
					End:      11,
				},
			},
		},
	}
	tf2 := TokenFrequencies{
		&TokenFreq{
			Term: []byte("water"),
			Locations: []*TokenLocation{
				&TokenLocation{
					Position: 1,
					Start:    0,
					End:      5,
				},
				&TokenLocation{
					Position: 2,
					Start:    6,
					End:      11,
				},
			},
		},
	}
	expectedResult := TokenFrequencies{
		&TokenFreq{
			Term: []byte("water"),
			Locations: []*TokenLocation{
				&TokenLocation{
					Position: 1,
					Start:    0,
					End:      5,
				},
				&TokenLocation{
					Position: 2,
					Start:    6,
					End:      11,
				},
				&TokenLocation{
					Field:    "tf2",
					Position: 1,
					Start:    0,
					End:      5,
				},
				&TokenLocation{
					Field:    "tf2",
					Position: 2,
					Start:    6,
					End:      11,
				},
			},
		},
	}
	tf1.MergeAll("tf2", tf2)
	if !reflect.DeepEqual(tf1, expectedResult) {
		t.Errorf("expected %#v, got %#v", expectedResult, tf1)
	}
}

func TestTokenFrequenciesMergeAllLeftEmpty(t *testing.T) {
	tf1 := TokenFrequencies{}
	tf2 := TokenFrequencies{
		&TokenFreq{
			Term: []byte("water"),
			Locations: []*TokenLocation{
				&TokenLocation{
					Position: 1,
					Start:    0,
					End:      5,
				},
				&TokenLocation{
					Position: 2,
					Start:    6,
					End:      11,
				},
			},
		},
	}
	expectedResult := TokenFrequencies{
		&TokenFreq{
			Term: []byte("water"),
			Locations: []*TokenLocation{
				&TokenLocation{
					Field:    "tf2",
					Position: 1,
					Start:    0,
					End:      5,
				},
				&TokenLocation{
					Field:    "tf2",
					Position: 2,
					Start:    6,
					End:      11,
				},
			},
		},
	}
	result := tf1.MergeAll("tf2", tf2)
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("expected %#v, got %#v", expectedResult, result)
		//t.Logf("%#v", tf1[0])
	}
}

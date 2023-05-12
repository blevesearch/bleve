package en

import "testing"

func TestEnglishPluralStemmer(t *testing.T) {
	data := []struct {
		In, Out string
	}{
		{"dresses", "dress"},
		{"dress", "dress"},
		{"axes", "axe"},
		{"ad", "ad"},
		{"ads", "ad"},
		{"gas", "ga"},
		{"sass", "sass"},
		{"berries", "berry"},
		{"dresses", "dress"},
		{"spies", "spy"},
		{"shoes", "shoe"},
		{"headaches", "headache"},
		{"computer", "computer"},
		{"dressing", "dressing"},
		{"clothes", "clothe"},
		{"DRESSES", "dress"},
		{"frog", "frog"},
		{"dress", "dress"},
		{"runs", "run"},
		{"pies", "pie"},
		{"foxes", "fox"},
		{"axes", "axe"},
		{"foes", "fo"},
		{"dishes", "dish"},
		{"snitches", "snitch"},
		{"cliches", "cliche"},
		{"forests", "forest"},
		{"yes", "ye"},
	}

	for _, datum := range data {
		stemmed := stem(datum.In)

		if stemmed != datum.Out {
			t.Errorf("expected %v but got %v", datum.Out, stemmed)
		}
	}
}

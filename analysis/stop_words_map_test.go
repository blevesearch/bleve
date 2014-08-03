package analysis

import (
	"reflect"
	"testing"
)

func TestWordMapLoadFile(t *testing.T) {
	wordMap := make(WordMap, 0)
	wordMap.LoadFile("test_stop_words.txt")

	expectedWords := make(WordMap, 0)
	expectedWords.AddWord("marty")
	expectedWords.AddWord("steve")
	expectedWords.AddWord("dustin")
	expectedWords.AddWord("siri")
	expectedWords.AddWord("multiple")
	expectedWords.AddWord("words")
	expectedWords.AddWord("with")
	expectedWords.AddWord("different")
	expectedWords.AddWord("whitespace")

	if !reflect.DeepEqual(wordMap, expectedWords) {
		t.Errorf("expected %#v, got %#v", expectedWords, wordMap)
	}
}

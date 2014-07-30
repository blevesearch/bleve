package stop_words_filter

import (
	"reflect"
	"testing"
)

func TestStopWordsLoadFile(t *testing.T) {
	stopWordsMap := make(StopWordsMap, 0)
	stopWordsMap.LoadFile("test_stop_words.txt")

	expectedStopWords := make(StopWordsMap, 0)
	expectedStopWords.AddWord("marty")
	expectedStopWords.AddWord("steve")
	expectedStopWords.AddWord("dustin")
	expectedStopWords.AddWord("siri")
	expectedStopWords.AddWord("multiple")
	expectedStopWords.AddWord("words")
	expectedStopWords.AddWord("with")
	expectedStopWords.AddWord("different")
	expectedStopWords.AddWord("whitespace")

	if !reflect.DeepEqual(stopWordsMap, expectedStopWords) {
		t.Errorf("expected %#v, got %#v", expectedStopWords, stopWordsMap)
	}
}

package stop_words_filter

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"strings"
)

type StopWordsMap map[string]bool

func NewStopWordsMap() StopWordsMap {
	return make(StopWordsMap, 0)
}

func (s StopWordsMap) LoadFile(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return s.LoadBytes(data)
}

func (s StopWordsMap) LoadBytes(data []byte) error {
	bytesReader := bytes.NewReader(data)
	bufioReader := bufio.NewReader(bytesReader)
	line, err := bufioReader.ReadString('\n')
	for err == nil {
		s.LoadLine(line)
		line, err = bufioReader.ReadString('\n')
	}
	// if the err was EOF still need to process last value
	if err == io.EOF {
		s.LoadLine(line)
		return nil
	}
	return err
}

func (s StopWordsMap) LoadLine(line string) error {
	// find the start of comment, if any
	startComment := strings.IndexAny(line, "#|")
	if startComment >= 0 {
		line = line[:startComment]
	}

	stopWords := strings.Fields(line)
	for _, stopWord := range stopWords {
		s.AddWord(stopWord)
	}
	return nil
}

func (s StopWordsMap) AddWord(word string) {
	s[word] = true
}

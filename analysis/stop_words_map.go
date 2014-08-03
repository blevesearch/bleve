package analysis

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"strings"
)

type WordMap map[string]bool

func NewWordMap() WordMap {
	return make(WordMap, 0)
}

func (s WordMap) LoadFile(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return s.LoadBytes(data)
}

func (s WordMap) LoadBytes(data []byte) error {
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

func (s WordMap) LoadLine(line string) error {
	// find the start of comment, if any
	startComment := strings.IndexAny(line, "#|")
	if startComment >= 0 {
		line = line[:startComment]
	}

	words := strings.Fields(line)
	for _, word := range words {
		s.AddWord(word)
	}
	return nil
}

func (s WordMap) AddWord(word string) {
	s[word] = true
}

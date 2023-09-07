//  Copyright (c) 2018 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package javadata

import (
	"bytes"
	"io"
	"testing"
)

func TestReadBool(t *testing.T) {

	tests := []struct {
		in  []byte
		out bool
		err error
	}{
		{
			in:  []byte{0},
			out: false,
		},
		{
			in:  []byte{1},
			out: true,
		},
		{
			in:  []byte{27},
			out: true,
		},
		{
			in:  []byte{},
			err: io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(string(test.in), func(t *testing.T) {
			sr := bytes.NewReader(test.in)
			dr := NewReader(sr)
			actual, err := dr.ReadBool()
			if err != test.err {
				t.Error(err)
			}
			if actual != test.out {
				t.Errorf("expected %t, got %t", test.out, actual)
			}
		})
	}
}

func TestReadUint16(t *testing.T) {

	tests := []struct {
		in  []byte
		out uint16
		err error
	}{
		{
			in:  []byte{0, 0},
			out: 0,
		},
		{
			in:  []byte{0, 1},
			out: 1,
		},
		{
			in:  []byte{1, 0},
			out: 256,
		},
		{
			in:  []byte{},
			err: io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(string(test.in), func(t *testing.T) {
			sr := bytes.NewReader(test.in)
			dr := NewReader(sr)
			actual, err := dr.ReadUint16()
			if err != test.err {
				t.Error(err)
			}
			if actual != test.out {
				t.Errorf("expected %d, got %d", test.out, actual)
			}
		})
	}
}

func TestReadInt32(t *testing.T) {

	tests := []struct {
		in  []byte
		out int32
		err error
	}{
		{
			in:  []byte{0, 0, 0, 0},
			out: 0,
		},
		{
			in:  []byte{0, 0, 0, 1},
			out: 1,
		},
		{
			in:  []byte{0, 0, 1, 0},
			out: 256,
		},
		{
			in:  []byte{0, 1, 0, 0},
			out: 65536,
		},
		{
			in:  []byte{},
			err: io.EOF,
		},
	}

	for _, test := range tests {
		t.Run(string(test.in), func(t *testing.T) {
			sr := bytes.NewReader(test.in)
			dr := NewReader(sr)
			actual, err := dr.ReadInt32()
			if err != test.err {
				t.Error(err)
			}
			if actual != test.out {
				t.Errorf("expected %d, got %d", test.out, actual)
			}
		})
	}
}

func TestReadUTF(t *testing.T) {

	tests := []struct {
		in  []byte
		out string
		err error
	}{
		{
			in:  []byte{0, 3, 'c', 'a', 't'},
			out: "cat",
		},
		{
			in:  []byte{0, 2, 0xc2, 0xa3},
			out: "£",
		},
		{
			in:  []byte{0, 3, 0xe3, 0x85, 0x85},
			out: "ㅅ",
		},
		{
			in:  []byte{0, 6, 0xe3, 0x85, 0x85, 'c', 'a', 't'},
			out: "ㅅcat",
		},
		{
			in:  []byte{},
			err: io.EOF,
		},
		{
			in:  []byte{0, 3},
			err: io.EOF,
		},
		{
			in:  []byte{0, 1, 0xc2},
			err: ErrMalformedInput,
		},
		{
			in:  []byte{0, 2, 0xc2, 0xc3},
			err: ErrMalformedInput,
		},
		{
			in:  []byte{0, 2, 0xe3, 0x85},
			err: ErrMalformedInput,
		},
		{
			in:  []byte{0, 3, 0xe3, 0xc5, 0x85},
			err: ErrMalformedInput,
		},
		{
			in:  []byte{0, 1, 0xff},
			err: ErrMalformedInput,
		},
		{
			in:  []byte{0x0, 0x05, 0x44, 0x61, 0x52, 0xc4, 0x87},
			out: "DaRć",
		},
	}

	for _, test := range tests {
		t.Run(string(test.in), func(t *testing.T) {
			sr := bytes.NewReader(test.in)
			dr := NewReader(sr)
			actual, err := dr.ReadUTF()
			if err != test.err {
				t.Error(err)
			}
			if actual != test.out {
				t.Errorf("expected %s, got %s", test.out, actual)
			}
		})
	}

}

// func TestFile(t *testing.T) {
// 	f, err := os.Open("stemmer_20000.tbl")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	r := NewReader(f)
// 	reversed, err := r.ReadBool()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Printf("reversed: %t", reversed)
// 	root, err := r.ReadInt32()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Printf("root: %d", root)
// 	n, err := r.ReadInt32()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	log.Printf("n is %d", n)
// 	// for n > 0 {
// 	// 	utf, err := r.ReadUTF()
// 	// 	if err != nil {
// 	// 		t.Error(err)
// 	// 	}
// 	// 	log.Printf("read: %s", utf)
// 	// 	n--
// 	// }
// }

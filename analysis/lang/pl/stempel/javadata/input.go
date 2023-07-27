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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// ErrMalformedInput returned when malformed input is encountered
var ErrMalformedInput = fmt.Errorf("malformed input")

// Reader knows how to read java serialized data
type Reader struct {
	r *bufio.Reader
}

// NewReader creates a new java data input reader
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadBool attempts to reads a bool from the stream
func (r *Reader) ReadBool() (bool, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

// ReadInt32 attempts to reads a signed 32-bit integer from the stream
func (r *Reader) ReadInt32() (rv int32, err error) {
	err = binary.Read(r.r, binary.BigEndian, &rv)
	return
}

// ReadUint16 attempts to reads a unsigned 16-bit integer from the stream
func (r *Reader) ReadUint16() (rv uint16, err error) {
	err = binary.Read(r.r, binary.BigEndian, &rv)
	return
}

// ReadCharAsRune attempts to read a java two byte char and return it as a rune
func (r *Reader) ReadCharAsRune() (rv rune, err error) {
	var char uint16
	err = binary.Read(r.r, binary.BigEndian, &char)
	rv = rune(char)
	return
}

// ReadUTF attempts to reads a UTF-encoded string from the stream
// this method follows the specific alternate encoding desribed here:
// https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html
func (r *Reader) ReadUTF() (string, error) {
	utfLen, err := r.ReadUint16()
	if err != nil {
		return "", err
	}
	bytes := make([]byte, utfLen)
	runes := make([]rune, utfLen)
	_, err = io.ReadFull(r.r, bytes)
	if err != nil {
		return "", err
	}

	var count uint16
	var runeCount uint16

	// handle simple case of all ascii
	for count < utfLen {
		c := bytes[count]
		if bytes[count] > 127 {
			break
		}
		count++
		runes[runeCount] = rune(c)
		runeCount++
	}

	// handle rest
	for count < utfLen {
		c := bytes[count]
		switch bytes[count] >> 4 {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8:
			/* 0xxxxxxx*/
			count++
			runes[runeCount] = rune(c)
			runeCount++
		case 12, 13:
			/* 110x xxxx   10xx xxxx*/
			count += 2
			if count > utfLen {
				return "", ErrMalformedInput
			}
			char2 := rune(bytes[count-1])
			if (char2 & 0xC0) != 0x80 {
				return "", ErrMalformedInput
			}
			runes[runeCount] = (rune(c)&0x1F)<<6 | char2&0x3F
			runeCount++
		case 14:
			/* 1110 xxxx  10xx xxxx  10xx xxxx */
			count += 3
			if count > utfLen {
				return "", ErrMalformedInput
			}
			char2 := rune(bytes[count-2])
			char3 := rune(bytes[count-1])
			if ((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80) {
				return "", ErrMalformedInput
			}
			runes[runeCount] = ((rune(c)&0x0F)<<12 | (char2&0x3F)<<6 | (char3&0x3F)<<0)
			runeCount++
		default:
			/* 10xx xxxx,  1111 xxxx */
			return "", ErrMalformedInput
		}
	}
	return string(runes[0:runeCount]), nil
}

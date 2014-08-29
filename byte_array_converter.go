//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"encoding/json"
)

type ByteArrayConverter interface {
	Convert([]byte) (interface{}, error)
}

type StringByteArrayConverter struct{}

func NewStringByteArrayConverter() *StringByteArrayConverter {
	return &StringByteArrayConverter{}
}

func (c *StringByteArrayConverter) Convert(in []byte) (interface{}, error) {
	return string(in), nil
}

type JSONByteArrayConverter struct{}

func NewJSONByteArrayConverter() *JSONByteArrayConverter {
	return &JSONByteArrayConverter{}
}

func (c *JSONByteArrayConverter) Convert(in []byte) (interface{}, error) {
	var rv map[string]interface{}
	err := json.Unmarshal(in, &rv)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

type IgnoreByteArrayConverter struct{}

func NewIgnoreByteArrayConverter() *IgnoreByteArrayConverter {
	return &IgnoreByteArrayConverter{}
}

func (c *IgnoreByteArrayConverter) Convert(in []byte) (interface{}, error) {
	return nil, nil
}

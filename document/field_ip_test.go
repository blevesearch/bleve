//  Copyright (c) 2021 Couchbase, Inc.
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

package document

import (
	"bytes"
	"net"
	"testing"
)

func TestIPField(t *testing.T) {
	nf := NewIPField("ip", []uint64{}, net.IPv4(192, 168, 1, 1))
	nf.Analyze()
	if nf.length != 1 {
		t.Errorf("expected 1 token")
	}
	if len(nf.value) != 16 {
		t.Errorf("stored value should be in 16 byte ipv6 format")
	}
	if !bytes.Equal(nf.value, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1}) {
		t.Errorf("wrong value stored, expected 192.168.1.1, got %q", nf.value.String())
	}
	if len(nf.frequencies) != 1 {
		t.Errorf("expected 1 token freqs")
	}
}

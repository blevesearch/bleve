package searcher

import (
	"net"
	"testing"
)

func Test_maskLen(t *testing.T) {
	tests := []struct {
		arg  string
		want int
	}{
		{"1.1.1.1/1", 0},
		{"1.1.1.1/7", 0},
		{"1.1.1.1/8", 1},
		{"1.1.1.1/24", 3},
		{"1.1.1.1/23", 2},
		{"1.1.1.1/25", 3},
		{"1.1.1.1/31", 3},
		{"1.1.1.1/32", 4},
		{"2001:db8::/32", 4},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			_, net, err := net.ParseCIDR(tt.arg)
			if err != nil {
				t.Fatal(err)
			}
			if got := maskLen(net); got != tt.want {
				t.Errorf("maskLen() = %v, want %v", got, tt.want)
			}
		})
	}
}

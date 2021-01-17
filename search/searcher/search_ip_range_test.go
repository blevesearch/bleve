package searcher

import (
	"net"
	"testing"
)

func Test_netLimits(t *testing.T) {
	tests := []struct {
		arg string
		lo  string
		hi  string
	}{
		{"128.0.0.0/1", "128.0.0.0", "255.255.255.255"},
		{"128.0.0.0/7", "128.0.0.0", "129.255.255.255"},
		{"1.1.1.1/8", "1.0.0.0", "1.255.255.255"},
		{"1.2.3.0/24", "1.2.3.0", "1.2.3.255"},
		{"1.2.2.0/23", "1.2.2.0", "1.2.3.255"},
		{"1.2.3.128/25", "1.2.3.128", "1.2.3.255"},
		{"1.2.3.0/25", "1.2.3.0", "1.2.3.127"},
		{"1.2.3.4/31", "1.2.3.4", "1.2.3.5"},
		{"1.2.3.4/32", "1.2.3.4", "1.2.3.4"},
		{"2a00:23c8:7283:ff00:1fa8:0:0:0/80", "2a00:23c8:7283:ff00:1fa8::", "2a00:23c8:7283:ff00:1fa8:ffff:ffff:ffff"},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			_, net, err := net.ParseCIDR(tt.arg)
			if err != nil {
				t.Fatal(err)
			}
			lo, hi := netLimits(net)
			if lo.String() != tt.lo || hi.String() != tt.hi {
				t.Errorf("netLimits(%q) = %s %s, want %s %s", tt.arg, lo, hi, tt.lo, tt.hi)
			}

		})
	}
}

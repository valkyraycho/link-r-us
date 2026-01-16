package privnet

import "net"

var (
	defaultPrivateCIDRs = []string{
		// Loopback
		"127.0.0.0/8",
		"::1/128",
		// Private networks (see RFC1918)
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		// Link-local addresses
		"169.254.0.0/16",
		"fe80::/10",
		// Misc
		"0.0.0.0/8",          // All IP addresses on local machine
		"255.255.255.255/32", // Broadcast address for current network
		"fc00::/7",           // IPv6 unique local addr
	}
)

type Detector struct {
	privBlocks []*net.IPNet
}

func NewDetector() (*Detector, error) {
	return NewDetectorFromCIDRs(defaultPrivateCIDRs...)
}

func NewDetectorFromCIDRs(privateNetworkCIDRs ...string) (*Detector, error) {
	blocks, err := parseCIDRs(privateNetworkCIDRs)
	if err != nil {
		return nil, err
	}
	return &Detector{privBlocks: blocks}, nil
}

func (d *Detector) IsPrivate(address string) (bool, error) {
	ip, err := net.ResolveIPAddr("ip", address)
	if err != nil {
		return false, err
	}

	for _, block := range d.privBlocks {
		if block.Contains(ip.IP) {
			return true, nil
		}
	}

	return false, nil
}

func parseCIDRs(cidrs []string) ([]*net.IPNet, error) {
	var err error
	out := make([]*net.IPNet, len(cidrs))

	for i, cidr := range cidrs {
		if _, out[i], err = net.ParseCIDR(cidr); err != nil {
			return nil, err
		}
	}
	return out, nil
}

//go:build android

package main

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"tailscale.com/net/netmon"
)

var (
	androidNetmonMu     sync.RWMutex
	androidNetmonIfaces []netmon.Interface
)

func init() {
	netmon.RegisterInterfaceGetter(func() ([]netmon.Interface, error) {
		androidNetmonMu.RLock()
		snapshot := cloneNetmonInterfaces(androidNetmonIfaces)
		androidNetmonMu.RUnlock()
		if len(snapshot) > 0 {
			return snapshot, nil
		}
		return fallbackAndroidInterfaces()
	})
}

func applyAndroidNetworkSnapshot(cfg bridgeConfig, logf func(string, ...any)) {
	ifaces := make([]netmon.Interface, 0, len(cfg.AndroidInterfaces))
	for _, raw := range cfg.AndroidInterfaces {
		name := strings.TrimSpace(raw.Name)
		if name == "" {
			continue
		}
		flags := net.Flags(0)
		if raw.Up {
			flags |= net.FlagUp
		}
		if raw.Loopback {
			flags |= net.FlagLoopback
		}
		if raw.Multicast {
			flags |= net.FlagMulticast
		}
		addrs := make([]net.Addr, 0, len(raw.Addrs))
		for _, cidr := range raw.Addrs {
			_, ipNet, err := net.ParseCIDR(strings.TrimSpace(cidr))
			if err != nil {
				if logf != nil {
					logf("tsnetbridge(android): skip malformed snapshot addr %q on %s: %v", cidr, name, err)
				}
				continue
			}
			addrs = append(addrs, ipNet)
		}
		iface := net.Interface{
			Index: raw.Index,
			MTU:   raw.MTU,
			Name:  name,
			Flags: flags,
		}
		ifaces = append(ifaces, netmon.Interface{
			Interface: &iface,
			AltAddrs:  addrs,
		})
	}

	androidNetmonMu.Lock()
	androidNetmonIfaces = cloneNetmonInterfaces(ifaces)
	androidNetmonMu.Unlock()

	defaultIf := strings.TrimSpace(cfg.AndroidDefaultRouteInterface)
	if defaultIf != "" {
		netmon.UpdateLastKnownDefaultRouteInterface(defaultIf)
	}
	if logf != nil {
		logf(
			"tsnetbridge(android): applied interface snapshot ifaces=%d defaultRoute=%q",
			len(ifaces),
			defaultIf,
		)
	}
}

func cloneNetmonInterfaces(input []netmon.Interface) []netmon.Interface {
	if len(input) == 0 {
		return nil
	}
	out := make([]netmon.Interface, 0, len(input))
	for _, iface := range input {
		if iface.Interface == nil {
			continue
		}
		copyInterface := *iface.Interface
		copyAddrs := cloneNetAddrs(iface.AltAddrs)
		out = append(out, netmon.Interface{
			Interface: &copyInterface,
			AltAddrs:  copyAddrs,
			Desc:      iface.Desc,
		})
	}
	return out
}

func cloneNetAddrs(addrs []net.Addr) []net.Addr {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]net.Addr, 0, len(addrs))
	for _, addr := range addrs {
		switch v := addr.(type) {
		case *net.IPNet:
			if v == nil {
				continue
			}
			copyIP := append(net.IP(nil), v.IP...)
			copyMask := append(net.IPMask(nil), v.Mask...)
			out = append(out, &net.IPNet{IP: copyIP, Mask: copyMask})
		case *net.IPAddr:
			if v == nil {
				continue
			}
			copyIP := append(net.IP(nil), v.IP...)
			out = append(out, &net.IPAddr{IP: copyIP, Zone: v.Zone})
		default:
			if addr != nil {
				out = append(out, addr)
			}
		}
	}
	return out
}

func fallbackAndroidInterfaces() ([]netmon.Interface, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("android_netmon_snapshot_missing:%w", err)
	}
	out := make([]netmon.Interface, 0, len(ifs))
	for i := range ifs {
		iface := ifs[i]
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("android_netmon_stdlib_addrs:%w", err)
		}
		out = append(out, netmon.Interface{
			Interface: &iface,
			AltAddrs:  cloneNetAddrs(addrs),
		})
	}
	return out, nil
}

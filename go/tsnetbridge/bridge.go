package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tailscale.com/client/local"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

const (
	defaultStartTimeout   = 45 * time.Second
	defaultStatusTimeout  = 5 * time.Second
	defaultPingTimeout    = 5 * time.Second
	defaultPingSamples    = 3
	maxDatagramPayloadLen = 64 * 1024
)

type proxyKind string

const (
	proxyKindTCP  proxyKind = "tcp"
	proxyKindQUIC proxyKind = "quic"
)

type responseEnvelope struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type bridgeConfig struct {
	ControlURL                   string                   `json:"controlUrl"`
	AuthKey                      string                   `json:"authKey"`
	Hostname                     string                   `json:"hostname"`
	StateDir                     string                   `json:"stateDir"`
	WireGuardPort                int                      `json:"wireguardPort,omitempty"`
	LogLevel                     string                   `json:"logLevel,omitempty"`
	EnableDebug                  bool                     `json:"enableDebug,omitempty"`
	AndroidDefaultRouteInterface string                   `json:"androidDefaultRouteInterface,omitempty"`
	AndroidInterfaces            []bridgeAndroidInterface `json:"androidInterfaces,omitempty"`
}

type bridgeAndroidInterface struct {
	Name      string   `json:"name"`
	Index     int      `json:"index,omitempty"`
	MTU       int      `json:"mtu,omitempty"`
	Up        bool     `json:"up,omitempty"`
	Loopback  bool     `json:"loopback,omitempty"`
	Multicast bool     `json:"multicast,omitempty"`
	Addrs     []string `json:"addrs,omitempty"`
}

type tailnetPeerSummary struct {
	HostName               string   `json:"hostName"`
	DNSName                string   `json:"dnsName"`
	TailscaleIPs           []string `json:"tailscaleIPs"`
	Online                 bool     `json:"online"`
	Active                 bool     `json:"active"`
	CurAddr                string   `json:"curAddr"`
	Addrs                  []string `json:"addrs,omitempty"`
	Relay                  string   `json:"relay,omitempty"`
	PeerRelay              string   `json:"peerRelay,omitempty"`
	InMagicSock            bool     `json:"inMagicSock,omitempty"`
	InEngine               bool     `json:"inEngine,omitempty"`
	LastHandshakeUnixMilli int64    `json:"lastHandshakeUnixMilli,omitempty"`
}

type tailnetStatusResponse struct {
	responseEnvelope
	Started               bool                 `json:"started"`
	BackendState          string               `json:"backendState,omitempty"`
	AuthURL               string               `json:"authUrl,omitempty"`
	Hostname              string               `json:"hostname,omitempty"`
	ControlURL            string               `json:"controlUrl,omitempty"`
	WireGuardPort         int                  `json:"wireguardPort,omitempty"`
	TailnetIPs            []string             `json:"tailnetIPs"`
	TailnetPath           string               `json:"tailnetPath,omitempty"`
	TailnetRelay          string               `json:"tailnetRelay,omitempty"`
	TailnetDerpMapSummary string               `json:"tailnetDerpMapSummary,omitempty"`
	TailnetPeers          []tailnetPeerSummary `json:"tailnetPeers"`
}

type tailnetPingRequest struct {
	PeerIP      string `json:"peerIP"`
	PingType    string `json:"pingType,omitempty"`
	SampleCount int    `json:"sampleCount,omitempty"`
	TimeoutMs   int    `json:"timeoutMs,omitempty"`
}

type tailnetPingSample struct {
	Sequence       int    `json:"sequence"`
	LatencyMs      int64  `json:"latencyMs"`
	Endpoint       string `json:"endpoint,omitempty"`
	PeerRelay      string `json:"peerRelay,omitempty"`
	DERPRegionID   int    `json:"derpRegionId,omitempty"`
	DERPRegionCode string `json:"derpRegionCode,omitempty"`
	Error          string `json:"error,omitempty"`
}

type tailnetPingResponse struct {
	responseEnvelope
	PeerIP       string              `json:"peerIP"`
	PingType     string              `json:"pingType"`
	Samples      []tailnetPingSample `json:"samples"`
	MinLatencyMs int64               `json:"minLatencyMs,omitempty"`
	MaxLatencyMs int64               `json:"maxLatencyMs,omitempty"`
	AvgLatencyMs int64               `json:"avgLatencyMs,omitempty"`
	ElapsedMs    int64               `json:"elapsedMs"`
}

type tailnetDERPNodeSummary struct {
	Name      string `json:"name"`
	RegionID  int    `json:"regionId"`
	HostName  string `json:"hostName"`
	CertName  string `json:"certName,omitempty"`
	IPv4      string `json:"ipv4,omitempty"`
	IPv6      string `json:"ipv6,omitempty"`
	STUNPort  int    `json:"stunPort,omitempty"`
	STUNOnly  bool   `json:"stunOnly,omitempty"`
	DERPPort  int    `json:"derpPort,omitempty"`
	CanPort80 bool   `json:"canPort80,omitempty"`
}

type tailnetDERPRegionSummary struct {
	RegionID        int                      `json:"regionId"`
	RegionCode      string                   `json:"regionCode"`
	RegionName      string                   `json:"regionName"`
	Avoid           bool                     `json:"avoid,omitempty"`
	NoMeasureNoHome bool                     `json:"noMeasureNoHome,omitempty"`
	NodeCount       int                      `json:"nodeCount"`
	Nodes           []tailnetDERPNodeSummary `json:"nodes"`
}

type tailnetDERPMapResponse struct {
	responseEnvelope
	OmitDefaultRegions bool                       `json:"omitDefaultRegions,omitempty"`
	RegionCount        int                        `json:"regionCount"`
	Regions            []tailnetDERPRegionSummary `json:"regions"`
}

type listenerProxyRequest struct {
	Family       string `json:"family"`
	Port         int    `json:"port"`
	LocalAddress string `json:"localAddress"`
}

type listenerProxyResponse struct {
	responseEnvelope
	ListeningAddress string `json:"listeningAddress,omitempty"`
	Port             int    `json:"port,omitempty"`
}

type dialProxyRequest struct {
	Family string `json:"family"`
	IP     string `json:"ip"`
	Port   int    `json:"port"`
}

type dialProxyResponse struct {
	responseEnvelope
	ProxyAddress string `json:"proxyAddress,omitempty"`
}

type resolveRemoteRequest struct {
	RawAddress string `json:"rawAddress"`
}

type resolveRemoteResponse struct {
	responseEnvelope
	RemoteAddress string `json:"remoteAddress,omitempty"`
}

type parsedProxyAddress struct {
	family string
	host   string
	port   int
	kind   proxyKind
}

type bridge struct {
	mu sync.Mutex

	cfg         bridgeConfig
	server      *tsnet.Server
	localClient *local.Client
	started     bool

	tcpListeners map[string]*tcpListenerProxy
	tcpDials     map[string]*tcpDialProxy
	udpListeners map[string]*udpListenerProxy
	udpDials     map[string]*udpDialProxy

	rawRemote map[string]string
}

type tcpListenerProxy struct {
	listener         net.Listener
	family           string
	localTarget      string
	listeningAddress string
	listeningPort    int
}

type tcpDialProxy struct {
	listener     net.Listener
	network      string
	remoteTarget string
	proxyAddress string
}

type udpListenerProxy struct {
	packet           net.PacketConn
	family           string
	localTarget      *net.UDPAddr
	listeningAddress string
	listeningPort    int

	mu     sync.Mutex
	relays map[string]*udpListenerRelay
}

type udpListenerRelay struct {
	conn          *net.UDPConn
	parent        *udpListenerProxy
	remote        net.Addr
	rawAddress    string
	remoteAddress string
}

type udpDialProxy struct {
	packet       net.PacketConn
	localConn    *net.UDPConn
	remote       *net.UDPAddr
	proxyAddress string

	mu         sync.RWMutex
	clientAddr *net.UDPAddr
}

func newBridge(cfg bridgeConfig) *bridge {
	return &bridge{
		cfg:          cfg,
		tcpListeners: map[string]*tcpListenerProxy{},
		tcpDials:     map[string]*tcpDialProxy{},
		udpListeners: map[string]*udpListenerProxy{},
		udpDials:     map[string]*udpDialProxy{},
		rawRemote:    map[string]string{},
	}
}

func (b *bridge) logf(format string, args ...any) {
	if !b.cfg.EnableDebug {
		return
	}
	log.Printf("[tsnetbridge] "+format, args...)
}

func sanitizeTailnetHostname(hostname string) string {
	trimmed := strings.ToLower(strings.TrimSpace(hostname))
	if trimmed == "" {
		return ""
	}
	var builder strings.Builder
	lastDash := false
	for _, r := range trimmed {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func networkFor(family string, kind proxyKind) string {
	switch kind {
	case proxyKindTCP:
		if family == "ip6" {
			return "tcp6"
		}
		return "tcp4"
	case proxyKindQUIC:
		if family == "ip6" {
			return "udp6"
		}
		return "udp4"
	default:
		return ""
	}
}

func loopbackIP(family string) net.IP {
	if family == "ip6" {
		return net.ParseIP("::1")
	}
	return net.IPv4(127, 0, 0, 1)
}

func hostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func buildMultiaddr(family, host string, kind proxyKind, port int, tsnet bool) string {
	switch kind {
	case proxyKindTCP:
		if tsnet {
			return fmt.Sprintf("/%s/%s/tcp/%d/tsnet", family, host, port)
		}
		return fmt.Sprintf("/%s/%s/tcp/%d", family, host, port)
	case proxyKindQUIC:
		if tsnet {
			return fmt.Sprintf("/%s/%s/udp/%d/quic-v1/tsnet", family, host, port)
		}
		return fmt.Sprintf("/%s/%s/udp/%d/quic-v1", family, host, port)
	default:
		return ""
	}
}

func parseProxyMultiaddr(raw string) (parsedProxyAddress, error) {
	parts := strings.Split(strings.Trim(raw, "/"), "/")
	if len(parts) < 4 {
		return parsedProxyAddress{}, fmt.Errorf("invalid_multiaddr:%s", raw)
	}
	if parts[0] != "ip4" && parts[0] != "ip6" {
		return parsedProxyAddress{}, fmt.Errorf("unsupported_family:%s", raw)
	}
	out := parsedProxyAddress{
		family: parts[0],
		host:   parts[1],
	}
	switch {
	case len(parts) == 4 && parts[2] == "tcp":
		out.kind = proxyKindTCP
	case len(parts) >= 5 && parts[2] == "udp" && parts[4] == "quic-v1":
		out.kind = proxyKindQUIC
	default:
		return parsedProxyAddress{}, fmt.Errorf("unsupported_multiaddr:%s", raw)
	}
	port, err := strconv.Atoi(parts[3])
	if err != nil || port < 0 || port > 65535 {
		return parsedProxyAddress{}, fmt.Errorf("invalid_port:%s", raw)
	}
	out.port = port
	return out, nil
}

func netAddrPort(addr net.Addr) (int, error) {
	switch value := addr.(type) {
	case *net.TCPAddr:
		return value.Port, nil
	case *net.UDPAddr:
		return value.Port, nil
	default:
		host, portStr, err := net.SplitHostPort(addr.String())
		if err != nil {
			return 0, fmt.Errorf("split_host_port:%w", err)
		}
		_ = host
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return 0, err
		}
		return port, nil
	}
}

func netAddrToMultiaddr(addr net.Addr, kind proxyKind, tsnet bool) (string, error) {
	var ip net.IP
	var port int
	switch value := addr.(type) {
	case *net.TCPAddr:
		ip = value.IP
		port = value.Port
	case *net.UDPAddr:
		ip = value.IP
		port = value.Port
	default:
		host, portStr, err := net.SplitHostPort(addr.String())
		if err != nil {
			return "", fmt.Errorf("split_host_port:%w", err)
		}
		parsed := net.ParseIP(host)
		if parsed == nil {
			return "", fmt.Errorf("invalid_ip:%s", host)
		}
		ip = parsed
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return "", err
		}
	}
	family := "ip4"
	host := ip.String()
	if ip.To4() == nil {
		family = "ip6"
		host = ip.String()
	}
	return buildMultiaddr(family, host, kind, port, tsnet), nil
}

func tailnetIPForFamily(status *ipnstate.Status, family string) (netip.Addr, error) {
	if status == nil {
		return netip.Addr{}, errors.New("missing_tailnet_status")
	}
	for _, addr := range status.TailscaleIPs {
		switch family {
		case "ip4":
			if addr.Is4() {
				return addr, nil
			}
		case "ip6":
			if addr.Is6() {
				return addr, nil
			}
		}
	}
	return netip.Addr{}, fmt.Errorf("missing_tailnet_%s", family)
}

func normalizePingType(raw string) tailcfg.PingType {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "DISCO":
		return tailcfg.PingDisco
	case "ICMP":
		return tailcfg.PingICMP
	case "PEERAPI":
		return tailcfg.PingPeerAPI
	default:
		return tailcfg.PingTSMP
	}
}

func derpMapSummary(derpMap *tailcfg.DERPMap) string {
	if derpMap == nil || len(derpMap.Regions) == 0 {
		return ""
	}
	regionIDs := make([]int, 0, len(derpMap.Regions))
	for regionID := range derpMap.Regions {
		regionIDs = append(regionIDs, regionID)
	}
	sort.Ints(regionIDs)
	parts := make([]string, 0, len(regionIDs))
	for _, regionID := range regionIDs {
		region := derpMap.Regions[regionID]
		if region == nil {
			continue
		}
		parts = append(parts, fmt.Sprintf("%d/%s", region.RegionID, strings.ToLower(strings.TrimSpace(region.RegionCode))))
	}
	return strings.Join(parts, ",")
}

func currentTailnetPath(status *ipnstate.Status) (string, string) {
	if status == nil {
		return "idle", ""
	}
	if status.Self != nil {
		relay := strings.TrimSpace(status.Self.PeerRelay)
		if relay == "" {
			relay = strings.TrimSpace(status.Self.Relay)
		}
		if strings.TrimSpace(status.Self.CurAddr) != "" {
			return "direct", relay
		}
		if relay != "" {
			return "derp", relay
		}
	}
	for _, peer := range status.Peer {
		if peer == nil || !peer.Active {
			continue
		}
		relay := strings.TrimSpace(peer.PeerRelay)
		if relay == "" {
			relay = strings.TrimSpace(peer.Relay)
		}
		if strings.TrimSpace(peer.CurAddr) != "" {
			return "direct", relay
		}
		if relay != "" {
			return "derp", relay
		}
	}
	return "idle", ""
}

func (b *bridge) ensureStarted() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started && b.server != nil && b.localClient != nil {
		return nil
	}

	hostname := sanitizeTailnetHostname(b.cfg.Hostname)
	if hostname == "" {
		return errors.New("missing_tsnet_hostname")
	}
	stateDir := strings.TrimSpace(b.cfg.StateDir)
	if stateDir == "" {
		stateDir = filepath.Join(os.TempDir(), "nim-libp2p-tsnet", hostname)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("create_state_dir:%w", err)
	}
	authKey := strings.TrimSpace(b.cfg.AuthKey)
	if authKey != "" {
		statePath := filepath.Join(stateDir, "tailscaled.state")
		if info, err := os.Stat(statePath); err == nil && !info.IsDir() {
			b.logf("tsnetbridge: state file %q exists; reusing logged-in state and ignoring auth key", statePath)
			authKey = ""
		}
	}
	if err := os.Setenv("TS_LOGS_DIR", stateDir); err != nil {
		b.logf("tsnetbridge: failed to set TS_LOGS_DIR=%q: %v", stateDir, err)
	}
	applyAndroidNetworkSnapshot(b.cfg, b.logf)

	server := &tsnet.Server{
		Dir:        stateDir,
		Hostname:   hostname,
		AuthKey:    authKey,
		ControlURL: strings.TrimSpace(b.cfg.ControlURL),
		Port:       uint16(max(0, min(65535, b.cfg.WireGuardPort))),
	}
	if b.cfg.EnableDebug {
		server.Logf = b.logf
		server.UserLogf = b.logf
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("tsnet_start:%w", err)
	}
	localClient, err := server.LocalClient()
	if err != nil {
		_ = server.Close()
		return fmt.Errorf("tsnet_local_client:%w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultStartTimeout)
	defer cancel()
	if _, err := server.Up(ctx); err != nil {
		_ = server.Close()
		return fmt.Errorf("tsnet_up:%w", err)
	}

	b.server = server
	b.localClient = localClient
	b.started = true
	return nil
}

func (b *bridge) rememberRemoteMapping(rawAddress, remoteAddress string) {
	if rawAddress == "" || remoteAddress == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rawRemote[rawAddress] = remoteAddress
}

func (b *bridge) forgetRemoteMapping(rawAddress string) {
	if rawAddress == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.rawRemote, rawAddress)
}

func (b *bridge) resolveRemote(rawAddress string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rawRemote[strings.TrimSpace(rawAddress)]
}

func closeWriteQuietly(conn net.Conn) {
	type closeWriter interface {
		CloseWrite() error
	}
	if value, ok := conn.(closeWriter); ok {
		_ = value.CloseWrite()
		return
	}
	_ = conn.Close()
}

func proxyStreams(left net.Conn, right net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(left, right)
		closeWriteQuietly(left)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(right, left)
		closeWriteQuietly(right)
	}()
	wg.Wait()
	_ = left.Close()
	_ = right.Close()
}

func (p *tcpListenerProxy) run(b *bridge) {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			b.logf("tcp listener proxy accept failed: %v", err)
			return
		}
		go p.handleConn(b, conn)
	}
}

func (p *tcpListenerProxy) handleConn(b *bridge, incoming net.Conn) {
	defer incoming.Close()
	outgoing, err := net.DialTimeout("tcp", p.localTarget, 10*time.Second)
	if err != nil {
		b.logf("tcp listener proxy local dial failed: %v", err)
		return
	}
	rawAddress, rawErr := netAddrToMultiaddr(outgoing.LocalAddr(), proxyKindTCP, false)
	remoteAddress, remoteErr := netAddrToMultiaddr(incoming.RemoteAddr(), proxyKindTCP, true)
	if rawErr == nil && remoteErr == nil {
		b.rememberRemoteMapping(rawAddress, remoteAddress)
		defer b.forgetRemoteMapping(rawAddress)
	}
	proxyStreams(outgoing, incoming)
}

func (p *tcpListenerProxy) close() error {
	if p == nil || p.listener == nil {
		return nil
	}
	return p.listener.Close()
}

func (p *tcpDialProxy) run(b *bridge) {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			b.logf("tcp dial proxy accept failed: %v", err)
			return
		}
		go p.handleConn(b, conn)
	}
}

func (p *tcpDialProxy) handleConn(b *bridge, localConn net.Conn) {
	defer localConn.Close()
	if err := b.ensureStarted(); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b.mu.Lock()
	server := b.server
	b.mu.Unlock()
	if server == nil {
		return
	}
	remoteConn, err := server.Dial(ctx, p.network, p.remoteTarget)
	if err != nil {
		b.logf("tcp dial proxy tsnet dial failed: %v", err)
		return
	}
	proxyStreams(remoteConn, localConn)
}

func (p *tcpDialProxy) close() error {
	if p == nil || p.listener == nil {
		return nil
	}
	return p.listener.Close()
}

func (r *udpListenerRelay) run() {
	buffer := make([]byte, maxDatagramPayloadLen)
	for {
		n, _, err := r.conn.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		_, _ = r.parent.packet.WriteTo(buffer[:n], r.remote)
	}
}

func (r *udpListenerRelay) close() error {
	if r == nil || r.conn == nil {
		return nil
	}
	return r.conn.Close()
}

func (p *udpListenerProxy) relayForRemote(b *bridge, remote net.Addr) (*udpListenerRelay, error) {
	key := remote.String()
	p.mu.Lock()
	defer p.mu.Unlock()
	if relay, ok := p.relays[key]; ok {
		return relay, nil
	}
	conn, err := net.ListenUDP(networkFor(p.family, proxyKindQUIC), &net.UDPAddr{IP: loopbackIP(p.family)})
	if err != nil {
		return nil, err
	}
	rawAddress, err := netAddrToMultiaddr(conn.LocalAddr(), proxyKindQUIC, false)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	remoteAddress, err := netAddrToMultiaddr(remote, proxyKindQUIC, true)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	relay := &udpListenerRelay{
		conn:          conn,
		parent:        p,
		remote:        remote,
		rawAddress:    rawAddress,
		remoteAddress: remoteAddress,
	}
	p.relays[key] = relay
	b.rememberRemoteMapping(rawAddress, remoteAddress)
	go relay.run()
	return relay, nil
}

func (p *udpListenerProxy) run(b *bridge) {
	buffer := make([]byte, maxDatagramPayloadLen)
	for {
		n, remote, err := p.packet.ReadFrom(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			b.logf("udp listener proxy read failed: %v", err)
			return
		}
		relay, err := p.relayForRemote(b, remote)
		if err != nil {
			b.logf("udp listener relay failed: %v", err)
			continue
		}
		_, _ = relay.conn.WriteToUDP(buffer[:n], p.localTarget)
	}
}

func (p *udpListenerProxy) close(b *bridge) error {
	if p == nil {
		return nil
	}
	if p.packet != nil {
		_ = p.packet.Close()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, relay := range p.relays {
		b.forgetRemoteMapping(relay.rawAddress)
		_ = relay.close()
		delete(p.relays, key)
	}
	return nil
}

func (p *udpDialProxy) setClient(addr *net.UDPAddr) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clientAddr = addr
}

func (p *udpDialProxy) getClient() *net.UDPAddr {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.clientAddr == nil {
		return nil
	}
	copyAddr := *p.clientAddr
	return &copyAddr
}

func (p *udpDialProxy) runLocalToRemote() {
	buffer := make([]byte, maxDatagramPayloadLen)
	for {
		n, source, err := p.localConn.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		p.setClient(source)
		_, _ = p.packet.WriteTo(buffer[:n], p.remote)
	}
}

func (p *udpDialProxy) runRemoteToLocal() {
	buffer := make([]byte, maxDatagramPayloadLen)
	for {
		n, remote, err := p.packet.ReadFrom(buffer)
		if err != nil {
			return
		}
		if remote != nil {
			if udpAddr, ok := remote.(*net.UDPAddr); ok {
				if !udpAddr.IP.Equal(p.remote.IP) || udpAddr.Port != p.remote.Port {
					continue
				}
			}
		}
		client := p.getClient()
		if client == nil {
			continue
		}
		_, _ = p.localConn.WriteToUDP(buffer[:n], client)
	}
}

func (p *udpDialProxy) close() error {
	if p == nil {
		return nil
	}
	if p.packet != nil {
		_ = p.packet.Close()
	}
	if p.localConn != nil {
		_ = p.localConn.Close()
	}
	return nil
}

func (b *bridge) queryStatus() (*ipnstate.Status, error) {
	if err := b.ensureStarted(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	client := b.localClient
	b.mu.Unlock()
	if client == nil {
		return nil, errors.New("missing_tailnet_local_client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultStatusTimeout)
	defer cancel()
	status, err := client.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("tailnet_status:%w", err)
	}
	return status, nil
}

func (b *bridge) queryDERPMap() (*tailcfg.DERPMap, error) {
	if err := b.ensureStarted(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	client := b.localClient
	b.mu.Unlock()
	if client == nil {
		return nil, errors.New("missing_tailnet_local_client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultStatusTimeout)
	defer cancel()
	derpMap, err := client.CurrentDERPMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("tailnet_derp_map:%w", err)
	}
	return derpMap, nil
}

func (b *bridge) statusPayload() any {
	status, err := b.queryStatus()
	if err != nil {
		return tailnetStatusResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: err.Error()},
			Started:          false,
		}
	}
	var derpSummary string
	if derpMap, err := b.queryDERPMap(); err == nil {
		derpSummary = derpMapSummary(derpMap)
	}

	response := tailnetStatusResponse{
		responseEnvelope: responseEnvelope{OK: true},
		Started:          true,
		Hostname:         sanitizeTailnetHostname(b.cfg.Hostname),
		ControlURL:       strings.TrimSpace(b.cfg.ControlURL),
		WireGuardPort:    b.cfg.WireGuardPort,
		TailnetIPs:       []string{},
		TailnetPeers:     []tailnetPeerSummary{},
	}
	if status != nil {
		response.BackendState = status.BackendState
		response.AuthURL = strings.TrimSpace(status.AuthURL)
		for _, addr := range status.TailscaleIPs {
			response.TailnetIPs = append(response.TailnetIPs, addr.String())
		}
		response.TailnetPath, response.TailnetRelay = currentTailnetPath(status)
		if len(status.Peer) > 0 {
			response.TailnetPeers = make([]tailnetPeerSummary, 0, len(status.Peer))
			for _, peer := range status.Peer {
				if peer == nil {
					continue
				}
				row := tailnetPeerSummary{
					HostName:               peer.HostName,
					DNSName:                strings.TrimSuffix(peer.DNSName, "."),
					Online:                 peer.Online,
					Active:                 peer.Active,
					CurAddr:                peer.CurAddr,
					Addrs:                  append([]string(nil), peer.Addrs...),
					Relay:                  peer.Relay,
					PeerRelay:              peer.PeerRelay,
					InMagicSock:            peer.InMagicSock,
					InEngine:               peer.InEngine,
					LastHandshakeUnixMilli: peer.LastHandshake.UnixMilli(),
				}
				for _, addr := range peer.TailscaleIPs {
					row.TailscaleIPs = append(row.TailscaleIPs, addr.String())
				}
				response.TailnetPeers = append(response.TailnetPeers, row)
			}
			sort.Slice(response.TailnetPeers, func(i, j int) bool {
				left := response.TailnetPeers[i]
				right := response.TailnetPeers[j]
				if left.DNSName != right.DNSName {
					return left.DNSName < right.DNSName
				}
				return left.HostName < right.HostName
			})
		}
	}
	response.TailnetDerpMapSummary = derpSummary
	return response
}

func tailnetDERPMapPayload(derpMap *tailcfg.DERPMap, err error) any {
	if err != nil {
		return tailnetDERPMapResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: err.Error()},
			Regions:          []tailnetDERPRegionSummary{},
		}
	}
	response := tailnetDERPMapResponse{
		responseEnvelope:   responseEnvelope{OK: derpMap != nil, Error: ""},
		OmitDefaultRegions: derpMap != nil && derpMap.OmitDefaultRegions,
		Regions:            []tailnetDERPRegionSummary{},
	}
	if derpMap == nil {
		response.OK = false
		response.Error = "missing_tailnet_derp_map"
		return response
	}
	regionIDs := make([]int, 0, len(derpMap.Regions))
	for regionID := range derpMap.Regions {
		regionIDs = append(regionIDs, regionID)
	}
	sort.Ints(regionIDs)
	response.RegionCount = len(regionIDs)
	for _, regionID := range regionIDs {
		region := derpMap.Regions[regionID]
		if region == nil {
			continue
		}
		row := tailnetDERPRegionSummary{
			RegionID:        region.RegionID,
			RegionCode:      region.RegionCode,
			RegionName:      region.RegionName,
			Avoid:           region.Avoid,
			NoMeasureNoHome: region.NoMeasureNoHome,
			NodeCount:       len(region.Nodes),
			Nodes:           make([]tailnetDERPNodeSummary, 0, len(region.Nodes)),
		}
		for _, node := range region.Nodes {
			if node == nil {
				continue
			}
			row.Nodes = append(row.Nodes, tailnetDERPNodeSummary{
				Name:      node.Name,
				RegionID:  node.RegionID,
				HostName:  node.HostName,
				CertName:  node.CertName,
				IPv4:      node.IPv4,
				IPv6:      node.IPv6,
				STUNPort:  node.STUNPort,
				STUNOnly:  node.STUNOnly,
				DERPPort:  node.DERPPort,
				CanPort80: node.CanPort80,
			})
		}
		response.Regions = append(response.Regions, row)
	}
	return response
}

func (b *bridge) pingPayload(requestJSON string) any {
	req := tailnetPingRequest{
		PingType:    string(tailcfg.PingTSMP),
		SampleCount: defaultPingSamples,
		TimeoutMs:   int(defaultPingTimeout / time.Millisecond),
	}
	if strings.TrimSpace(requestJSON) != "" {
		if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
			return tailnetPingResponse{
				responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("invalid_ping_request:%v", err)},
			}
		}
	}
	peerIP, err := netip.ParseAddr(strings.TrimSpace(req.PeerIP))
	if err != nil {
		return tailnetPingResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("invalid_peer_ip:%v", err)},
			PeerIP:           strings.TrimSpace(req.PeerIP),
		}
	}
	if req.SampleCount <= 0 {
		req.SampleCount = defaultPingSamples
	}
	if req.TimeoutMs <= 0 {
		req.TimeoutMs = int(defaultPingTimeout / time.Millisecond)
	}
	if err := b.ensureStarted(); err != nil {
		return tailnetPingResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: err.Error()},
			PeerIP:           peerIP.String(),
		}
	}
	b.mu.Lock()
	client := b.localClient
	b.mu.Unlock()
	if client == nil {
		return tailnetPingResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: "missing_tailnet_local_client"},
			PeerIP:           peerIP.String(),
		}
	}

	started := time.Now()
	samples := make([]tailnetPingSample, 0, req.SampleCount)
	var total int64
	var minLatency int64
	var maxLatency int64
	successCount := 0
	lastError := ""
	pingType := normalizePingType(req.PingType)
	for index := 0; index < req.SampleCount; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.TimeoutMs)*time.Millisecond)
		result, pingErr := client.Ping(ctx, peerIP, pingType)
		cancel()

		sample := tailnetPingSample{Sequence: index + 1}
		if pingErr != nil {
			sample.Error = pingErr.Error()
			lastError = pingErr.Error()
		} else if result.Err != "" {
			sample.Error = result.Err
			sample.Endpoint = result.Endpoint
			sample.PeerRelay = result.PeerRelay
			sample.DERPRegionID = result.DERPRegionID
			sample.DERPRegionCode = result.DERPRegionCode
			lastError = result.Err
		} else {
			latencyMs := int64(result.LatencySeconds * 1000)
			sample.LatencyMs = latencyMs
			sample.Endpoint = result.Endpoint
			sample.PeerRelay = result.PeerRelay
			sample.DERPRegionID = result.DERPRegionID
			sample.DERPRegionCode = result.DERPRegionCode
			if successCount == 0 || latencyMs < minLatency {
				minLatency = latencyMs
			}
			if successCount == 0 || latencyMs > maxLatency {
				maxLatency = latencyMs
			}
			total += latencyMs
			successCount++
		}
		samples = append(samples, sample)
	}

	response := tailnetPingResponse{
		responseEnvelope: responseEnvelope{OK: successCount > 0, Error: lastError},
		PeerIP:           peerIP.String(),
		PingType:         string(pingType),
		Samples:          samples,
		ElapsedMs:        time.Since(started).Milliseconds(),
	}
	if successCount > 0 {
		response.MinLatencyMs = minLatency
		response.MaxLatencyMs = maxLatency
		response.AvgLatencyMs = total / int64(successCount)
	}
	return response
}

func (b *bridge) listenTCPProxy(requestJSON string) any {
	var req listenerProxyRequest
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		return listenerProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("invalid_tcp_listener_request:%v", err)},
		}
	}
	if err := b.ensureStarted(); err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	localAddr, err := parseProxyMultiaddr(req.LocalAddress)
	if err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	if localAddr.kind != proxyKindTCP {
		return listenerProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: "tcp_listener_requires_tcp_local_address"},
		}
	}
	family := strings.TrimSpace(req.Family)
	if family == "" {
		family = localAddr.family
	}
	status, err := b.queryStatus()
	if err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	localIP, err := tailnetIPForFamily(status, family)
	if err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}

	b.mu.Lock()
	server := b.server
	b.mu.Unlock()
	if server == nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: "missing_tsnet_server"}}
	}
	listener, err := server.Listen(networkFor(family, proxyKindTCP), ":"+strconv.Itoa(req.Port))
	if err != nil {
		return listenerProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("tsnet_listen_tcp:%v", err)},
		}
	}
	actualPort, err := netAddrPort(listener.Addr())
	if err != nil {
		_ = listener.Close()
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	proxy := &tcpListenerProxy{
		listener:         listener,
		family:           family,
		localTarget:      hostPort(localAddr.host, localAddr.port),
		listeningAddress: buildMultiaddr(family, localIP.String(), proxyKindTCP, actualPort, true),
		listeningPort:    actualPort,
	}

	b.mu.Lock()
	b.tcpListeners[proxy.listeningAddress] = proxy
	b.mu.Unlock()
	go proxy.run(b)
	return listenerProxyResponse{
		responseEnvelope: responseEnvelope{OK: true},
		ListeningAddress: proxy.listeningAddress,
		Port:             proxy.listeningPort,
	}
}

func (b *bridge) listenUDPProxy(requestJSON string) any {
	var req listenerProxyRequest
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		return listenerProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("invalid_udp_listener_request:%v", err)},
		}
	}
	if err := b.ensureStarted(); err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	localAddr, err := parseProxyMultiaddr(req.LocalAddress)
	if err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	if localAddr.kind != proxyKindQUIC {
		return listenerProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: "udp_listener_requires_quic_local_address"},
		}
	}
	family := strings.TrimSpace(req.Family)
	if family == "" {
		family = localAddr.family
	}
	status, err := b.queryStatus()
	if err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	localIP, err := tailnetIPForFamily(status, family)
	if err != nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}

	b.mu.Lock()
	server := b.server
	b.mu.Unlock()
	if server == nil {
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: "missing_tsnet_server"}}
	}
	packet, err := server.ListenPacket(
		networkFor(family, proxyKindQUIC),
		hostPort(localIP.String(), req.Port),
	)
	if err != nil {
		return listenerProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("tsnet_listen_udp:%v", err)},
		}
	}
	actualPort, err := netAddrPort(packet.LocalAddr())
	if err != nil {
		_ = packet.Close()
		return listenerProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	proxy := &udpListenerProxy{
		packet:           packet,
		family:           family,
		localTarget:      &net.UDPAddr{IP: loopbackIP(localAddr.family), Port: localAddr.port},
		listeningAddress: buildMultiaddr(family, localIP.String(), proxyKindQUIC, actualPort, true),
		listeningPort:    actualPort,
		relays:           map[string]*udpListenerRelay{},
	}

	b.mu.Lock()
	b.udpListeners[proxy.listeningAddress] = proxy
	b.mu.Unlock()
	go proxy.run(b)
	return listenerProxyResponse{
		responseEnvelope: responseEnvelope{OK: true},
		ListeningAddress: proxy.listeningAddress,
		Port:             proxy.listeningPort,
	}
}

func (b *bridge) dialTCPProxy(requestJSON string) any {
	var req dialProxyRequest
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		return dialProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("invalid_tcp_dial_request:%v", err)},
		}
	}
	if req.Port <= 0 || req.Port > 65535 {
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: "invalid_tcp_port"}}
	}
	if err := b.ensureStarted(); err != nil {
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	key := fmt.Sprintf("%s|%s|%d|tcp", req.Family, strings.TrimSpace(req.IP), req.Port)
	b.mu.Lock()
	if proxy, ok := b.tcpDials[key]; ok {
		address := proxy.proxyAddress
		b.mu.Unlock()
		return dialProxyResponse{
			responseEnvelope: responseEnvelope{OK: true},
			ProxyAddress:     address,
		}
	}
	b.mu.Unlock()

	listener, err := net.Listen(networkFor(req.Family, proxyKindTCP), hostPort(loopbackIP(req.Family).String(), 0))
	if err != nil {
		return dialProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("listen_local_tcp_proxy:%v", err)},
		}
	}
	proxyAddress, err := netAddrToMultiaddr(listener.Addr(), proxyKindTCP, false)
	if err != nil {
		_ = listener.Close()
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	proxy := &tcpDialProxy{
		listener:     listener,
		network:      networkFor(req.Family, proxyKindTCP),
		remoteTarget: hostPort(strings.TrimSpace(req.IP), req.Port),
		proxyAddress: proxyAddress,
	}

	b.mu.Lock()
	b.tcpDials[key] = proxy
	b.mu.Unlock()
	go proxy.run(b)
	return dialProxyResponse{
		responseEnvelope: responseEnvelope{OK: true},
		ProxyAddress:     proxy.proxyAddress,
	}
}

func (b *bridge) dialUDPProxy(requestJSON string) any {
	var req dialProxyRequest
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		return dialProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("invalid_udp_dial_request:%v", err)},
		}
	}
	if req.Port <= 0 || req.Port > 65535 {
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: "invalid_udp_port"}}
	}
	if err := b.ensureStarted(); err != nil {
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	status, err := b.queryStatus()
	if err != nil {
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	localIP, err := tailnetIPForFamily(status, req.Family)
	if err != nil {
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	key := fmt.Sprintf("%s|%s|%d|udp", req.Family, strings.TrimSpace(req.IP), req.Port)
	b.mu.Lock()
	if proxy, ok := b.udpDials[key]; ok {
		address := proxy.proxyAddress
		b.mu.Unlock()
		return dialProxyResponse{
			responseEnvelope: responseEnvelope{OK: true},
			ProxyAddress:     address,
		}
	}
	server := b.server
	b.mu.Unlock()
	if server == nil {
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: "missing_tsnet_server"}}
	}

	packet, err := server.ListenPacket(
		networkFor(req.Family, proxyKindQUIC),
		hostPort(localIP.String(), 0),
	)
	if err != nil {
		return dialProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("listen_tailnet_udp_proxy:%v", err)},
		}
	}
	localConn, err := net.ListenUDP(networkFor(req.Family, proxyKindQUIC), &net.UDPAddr{IP: loopbackIP(req.Family)})
	if err != nil {
		_ = packet.Close()
		return dialProxyResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("listen_local_udp_proxy:%v", err)},
		}
	}
	proxyAddress, err := netAddrToMultiaddr(localConn.LocalAddr(), proxyKindQUIC, false)
	if err != nil {
		_ = packet.Close()
		_ = localConn.Close()
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: err.Error()}}
	}
	remote, err := net.ResolveUDPAddr(networkFor(req.Family, proxyKindQUIC), hostPort(strings.TrimSpace(req.IP), req.Port))
	if err != nil {
		_ = packet.Close()
		_ = localConn.Close()
		return dialProxyResponse{responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("resolve_tailnet_udp:%v", err)}}
	}
	proxy := &udpDialProxy{
		packet:       packet,
		localConn:    localConn,
		remote:       remote,
		proxyAddress: proxyAddress,
	}

	b.mu.Lock()
	b.udpDials[key] = proxy
	b.mu.Unlock()
	go proxy.runLocalToRemote()
	go proxy.runRemoteToLocal()
	return dialProxyResponse{
		responseEnvelope: responseEnvelope{OK: true},
		ProxyAddress:     proxy.proxyAddress,
	}
}

func (b *bridge) resolveRemotePayload(requestJSON string) any {
	var req resolveRemoteRequest
	if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
		return resolveRemoteResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: fmt.Sprintf("invalid_resolve_request:%v", err)},
		}
	}
	remoteAddress := b.resolveRemote(req.RawAddress)
	if strings.TrimSpace(remoteAddress) == "" {
		return resolveRemoteResponse{
			responseEnvelope: responseEnvelope{OK: false, Error: "remote_mapping_not_found"},
		}
	}
	return resolveRemoteResponse{
		responseEnvelope: responseEnvelope{OK: true},
		RemoteAddress:    remoteAddress,
	}
}

func (b *bridge) reset() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, proxy := range b.tcpListeners {
		_ = proxy.close()
		delete(b.tcpListeners, key)
	}
	for key, proxy := range b.tcpDials {
		_ = proxy.close()
		delete(b.tcpDials, key)
	}
	for key, proxy := range b.udpListeners {
		_ = proxy.close(b)
		delete(b.udpListeners, key)
	}
	for key, proxy := range b.udpDials {
		_ = proxy.close()
		delete(b.udpDials, key)
	}
	clear(b.rawRemote)
	return nil
}

func (b *bridge) close() error {
	_ = b.reset()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.server != nil {
		if err := b.server.Close(); err != nil {
			return err
		}
	}
	b.server = nil
	b.localClient = nil
	b.started = false
	return nil
}

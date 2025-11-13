import { NimLibp2p } from '../common/NimLibp2p';
import { parseNodeEvents, ParsedNodeEvent } from '../common/EventWorker';
import image from '@ohos.multimedia.image';
import connection from '@ohos.net.connection';
import deviceInfo from '@ohos.deviceInfo';
import hilog from '@ohos.hilog';

interface NimBridgeStorage {
  Get(key: string): string[] | undefined;
  SetOrCreate?(key: string, value: string[]): void;
}

declare const AppStorage: NimBridgeStorage;

const DEMO_IMAGE_BASE64: string =
  'iVBORw0KGgoAAAANSUhEUgAAAA4AAAAOCAYAAAAfSC3RAAAAH0lEQVR42mNgGAUEA/n///8fQQ0wGoaBEYTEAAGbCAG8LGWErwAAAABJRU5ErkJggg==';
const DEMO_IMAGE_MIME: string = 'image/png';
const BASE64_TRIMMED_SENTINEL: string = '...<trimmed>';
const BASE64_ALPHABET: string = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
const FEED_ARRAY_KEYS: string[] = ['media', 'attachments', 'assets', 'content'];
const FEED_IMAGE_KEYS: string[] = ['cover', 'thumbnail', 'image'];
const FEED_SUMMARY_KEYS: string[] = ['body', 'summary', 'text', 'caption', 'title', 'name'];
const IDENTITY_STORAGE_KEY: string = '__nimPersistedIdentity_v2';
const PLATFORM_IDENTITY_SALT: string = 'ohos-hardware';
const DEFAULT_BOOTSTRAP_MULTIADDRS: string[] = [];
const DEFAULT_RELAY_MULTIADDRS: string[] = [];

type AddressCandidate = string | number | boolean | null | undefined;

interface MdnsEndpoint {
  peer_id?: string;
  addresses?: AddressCandidate[];
  is_local?: boolean;
}

interface MdnsEndpointsResponse {
  endpoints?: MdnsEndpoint[];
}

interface NodeEventEnvelope {
  payload?: string;
}

interface NodeEventPayload {
  type?: string;
  peer_id?: string;
  author?: string;
  addresses?: AddressCandidate[];
  timestamp?: number;
  timestamp_ms?: number;
  message_id?: string;
  success?: boolean;
  payload?: string | FeedRecord | null;
  content?: FeedRecord | null;
  stream_key?: string;
  frame_index?: number;
  payload_size?: number;
  reason?: string;
}

interface IdentityPayload {
  privateKey: string;
  publicKey: string;
  peerId: string;
  source?: string;
}

interface FeedAttachmentNode {
  id?: string;
  type?: string;
  kind?: string;
  label?: string;
  title?: string;
  name?: string;
  mime?: string;
  mediaType?: string;
  contentType?: string;
  encoding?: string;
  data?: string;
  base64?: string;
  payload?: string;
  blob?: string;
  bytes?: string;
  value?: string;
  text?: string;
  body?: string;
  caption?: string;
  url?: string;
  uri?: string;
  href?: string;
  src?: string;
}

interface PeerHint {
  peerId: string;
  addresses: string[];
}

interface FeedRecord {
  id?: string;
  cid?: string;
  author?: string;
  timestamp?: number;
  timestamp_ms?: number;
  body?: string;
  summary?: string;
  text?: string;
  caption?: string;
  title?: string;
  name?: string;
  media?: Array<FeedAttachmentNode | string>;
  attachments?: Array<FeedAttachmentNode | string>;
  assets?: Array<FeedAttachmentNode | string>;
  content?: Array<FeedAttachmentNode | string>;
  cover?: FeedAttachmentNode | string;
  thumbnail?: FeedAttachmentNode | string;
  image?: FeedAttachmentNode | string;
}

interface LocalFeedAttachment extends FeedAttachmentNode {
  id: string;
  type: string;
  label: string;
  mime?: string;
  encoding?: string;
  data?: string;
  text?: string;
}

interface LocalFeedEnvelope extends FeedRecord {
  id: string;
  author: string;
  body: string;
  timestamp: number;
  timestamp_ms: number;
  media?: LocalFeedAttachment[];
}

interface PrimaryAddresses {
  ipv4?: string;
  ipv6?: string;
  iface?: string;
}

interface ExtendedConnectionProps extends ConnectionPropertiesLike {
  interfaceName?: string;
}

type NetAddressLike = string | LinkAddressNode | connection.NetAddress | null | undefined;

interface LinkAddressNode {
  address?: NetAddressLike;
  family?: number;
  value?: NetAddressLike;
}

type LinkAddressLike = string | LinkAddressNode | connection.LinkAddress | null | undefined;

interface NormalizedLinkAddress {
  address: string;
  family?: number;
}

interface ConnectionPropertiesLike {
  linkAddresses?: LinkAddressLike[];
  interfaceName?: string;
}

interface NodeAutomationConfig {
  gossipsub: boolean;
  directStream: boolean;
  livestream: boolean;
  rendezvous?: boolean;
  autonat?: boolean;
  circuitRelay?: boolean;
}

interface MdnsConfig {
  service: string;
  preferredIpv4?: string;
  forceSearchMeta?: boolean;
}

interface NodeExtraConfig {
  mdns: MdnsConfig;
  bootstrapMultiaddrs?: string[];
  relayMultiaddrs?: string[];
}

function isGlobalIpv6(ip: string | undefined): boolean {
  if (!ip || ip.length === 0) {
    return false;
  }
  const normalized = ip.toLowerCase();
  if (normalized.startsWith('fe80') ||
    normalized.startsWith('fc') ||
    normalized.startsWith('fd') ||
    normalized === '::' ||
    normalized === '::1') {
    return false;
  }
  return true;
}

interface NodeConfig {
  listenAddresses: string[];
  automations: NodeAutomationConfig;
  extra: NodeExtraConfig;
  identity?: IdentityPayload;
}

interface PeerInfo {
  peerId: string;
  addresses: string[];
  connected: boolean;
  lastSeen: number;
}

interface MessageItem {
  messageId: string;
  fromSelf: boolean;
  body: string;
  timestamp: number;
  acked: boolean;
}

type FeedAttachmentKind = 'image' | 'audio' | 'video' | 'link' | 'text' | 'data' | 'unknown';

interface FeedAttachment {
  id: string;
  kind: FeedAttachmentKind;
  label: string;
  uri?: string;
  mime?: string;
  text?: string;
  data?: string;
}

interface FeedItem {
  id: string;
  author: string;
  timestamp: number;
  summary: string;
  attachments: FeedAttachment[];
  raw: string;
}

interface LiveFrame {
  streamKey: string;
  frameIndex: number;
  payloadSize: number;
  timestamp: number;
}

const LOG_DOMAIN: number = 0xD0B0;
const LOG_TAG: string = 'nimlib-ui';

@Entry
@Component
struct Index {
  private node: NimLibp2p = new NimLibp2p();
  private pollTimer: number = -1;
  private pollIntervalMs: number = 600;
  private idlePollIntervalMs: number = 1500;
  private chatStore: Map<string, MessageItem[]> = new Map();
  private imageCache: Map<string, image.PixelMap> = new Map();
  private readonly tabLabels: string[] = ['Overview', 'Chat', 'Feed', 'Live'];
  private endpointsRefreshInFlight: boolean = false;

  @State selectedTab: number = 0;
  @State statusMessage: string = 'Starting libp2p…';
  @State localPeer: string = '';
  @State ipv4List: string[] = [];
  @State ipv6List: string[] = [];
  @State peers: PeerInfo[] = [];
  @State selectedPeer: string = '';
  @State messageList: MessageItem[] = [];
  @State chatInput: string = '';
  @State feedInput: string = '';
  @State feedItems: FeedItem[] = [];
  @State attachDemo: boolean = false;
  @State streamKey: string = 'demo-stream';
  @State streamPayload: string = '';
  @State liveFrames: LiveFrame[] = [];
  private activeIpv4: string = '';
  private lastAppliedIpv4: string = '';
  private dmPending: boolean = false;
  private dmPendingTimer: number = -1;
  private maintainTimer: number = -1;
  private bootstrapMultiaddrs: string[] = [];
  private relayMultiaddrs: string[] = [];
  private lastConnectAttempt: Map<string, number> = new Map();
  private interfaceCache: PrimaryAddresses = {};
  private isPreferredInterface(name?: string): boolean {
    if (!name || name.length === 0) {
      return true;
    }
    const lowered = name.toLowerCase();
    return lowered.startsWith('wlan') ||
      lowered.startsWith('p2p') ||
      lowered.startsWith('ap') ||
      lowered.startsWith('eth');
  }
  private lastInterfaceRefresh: number = 0;
  private interfaceRefreshBackoffMs: number = 2000;
  private interfaceRefreshInFlight: boolean = false;
  private eventRawQueue: string[] = [];
  private eventParseActive: boolean = false;
  private lastEndpointsRefresh: number = 0;
  private endpointsIntervalMs: number = 1600;
  private disableSyncInterfaceLookup: boolean = false;
  private lanEndpointsLogged: boolean = false;

  aboutToAppear() {
    this.startNode();
  }

  aboutToDisappear() {
    this.stopNode();
  }

  private startNode() {
    const config = this.buildConfig();
    if (!this.node.start(config)) {
      this.statusMessage = `Init failed: ${this.node.lastInitError()}`;
      return;
    }
    this.applyPreferredIpv4();
    const peerLabel = this.localPeer && this.localPeer.length > 0 ? ` (${this.localPeer})` : '';
    this.statusMessage = `Node running${peerLabel}`;
    void this.requestInterfaceAddressesAsync(true);
    void this.refreshEndpoints(true);
    this.schedulePoll(50);
    this.lastConnectAttempt.clear();
    this.registerStaticPeerHints();
    void this.connectStaticMultiaddrs();
    this.startConnectivityWatchdog();
  }

  private stopNode() {
    if (this.pollTimer >= 0) {
      clearTimeout(this.pollTimer);
      this.pollTimer = -1;
    }
    this.stopConnectivityWatchdog();
    this.lastConnectAttempt.clear();
    this.node.stop();
    this.releaseImages();
    this.setDmPending(false);
  }

  private tick() {
    const raw = this.node.poll(128);
    if (raw && raw.length > 2) {
      this.enqueueNodeEvents(raw);
    }
    if (this.ipv6List.length === 0 && !this.interfaceRefreshInFlight) {
      void this.requestInterfaceAddressesAsync();
    }
    if (!this.dmPending) {
      void this.refreshEndpoints();
    }
    const delay = this.computeNextPollDelay();
    this.schedulePoll(delay);
  }

  private schedulePoll(delay: number) {
    if (this.pollTimer >= 0) {
      clearTimeout(this.pollTimer);
      this.pollTimer = -1;
    }
    this.pollTimer = setTimeout(() => {
      this.pollTimer = -1;
      this.tick();
    }, Math.max(50, delay));
  }

  private computeNextPollDelay(): number {
    if (this.eventRawQueue.length > 0) {
      return Math.min(300, this.pollIntervalMs);
    }
    if (this.dmPending || this.ipv6List.length === 0) {
      return this.pollIntervalMs;
    }
    return this.idlePollIntervalMs;
  }

  private async refreshEndpoints(force: boolean = false) {
    const now = Date.now();
    const cadence = this.ipv6List.length === 0 ? this.endpointsIntervalMs : this.endpointsIntervalMs * 3;
    if (!force && now - this.lastEndpointsRefresh < cadence) {
      return;
    }
    if (this.endpointsRefreshInFlight) {
      return;
    }
    const handle = this.node.handle;
    if (!handle) {
      return;
    }
    this.lastEndpointsRefresh = now;
    this.endpointsRefreshInFlight = true;
    try {
      const raw = await this.node.lanEndpointsAsync();
      this.applyEndpoints(raw);
    } catch (err) {
      const message = err ? `${err}` : 'unknown error';
      console.warn(`refreshEndpoints failed: ${message}`);
      this.statusMessage = `Parse endpoints failed`;
    } finally {
      this.endpointsRefreshInFlight = false;
    }
  }

  private applyEndpoints(raw: string) {
    try {
      if (!this.lanEndpointsLogged) {
        this.lanEndpointsLogged = true;
        const preview = raw && raw.length > 512 ? `${raw.substring(0, 512)}…` : raw;
        hilog.info(LOG_DOMAIN, LOG_TAG, 'lanEndpoints raw %{public}s', preview ?? '<empty>');
      }
      const parsed = JSON.parse(raw) as MdnsEndpointsResponse;
      const interfaceAddrs = this.collectInterfaceAddresses();
      const endpoints: PeerInfo[] = [];
      const ipv4: Set<string> = new Set();
      const ipv6All: Set<string> = new Set();
      const ipv6Global: Set<string> = new Set();
      let discoveredIpv4: string | undefined = undefined;
      const peerHints: PeerHint[] = [];
      const candidateMultiaddrs: string[] = [];
      const previousPeers: Map<string, PeerInfo> = new Map();
      this.peers.forEach((peer: PeerInfo) => {
        if (peer && peer.peerId) {
          previousPeers.set(peer.peerId, peer);
        }
      });
      if (parsed && parsed.endpoints) {
        parsed.endpoints.forEach((entry: MdnsEndpoint) => {
          const peerId = entry.peer_id ?? '';
          const isLocal = entry.is_local === true;
          const addresses: string[] = [];
          if (Array.isArray(entry.addresses)) {
            entry.addresses.forEach((candidate: AddressCandidate) => {
              if (typeof candidate === 'string' && candidate.length > 0) {
                const segments = this.splitMultiaddr(candidate);
                segments.forEach((segment: string) => {
                  addresses.push(segment);
                  if (isLocal) {
                    candidateMultiaddrs.push(segment);
                    const ip = this.extractIp(segment);
                    if (ip && ip.length > 0 && ip !== '0.0.0.0' && ip !== '::') {
                      if (segment.indexOf('/ip6/') >= 0) {
                        ipv6All.add(ip);
                        if (isGlobalIpv6(ip)) {
                          ipv6Global.add(ip);
                        }
                      } else {
                        ipv4.add(ip);
                        if (!discoveredIpv4 && ip !== '127.0.0.1') {
                          discoveredIpv4 = ip;
                        }
                      }
                    }
                  }
                });
              } else if (candidate !== undefined && candidate !== null) {
                console.warn(`Skipping non-string endpoint address: ${JSON.stringify(candidate)}`);
              }
            });
          }
          if (!isLocal) {
            const normalizedAddresses = this.uniqueStrings(addresses);
            const previous = previousPeers.get(peerId);
            const connected = previous ? previous.connected : false;
            const lastSeen = previous && this.arraysEqual(previous.addresses, normalizedAddresses)
              ? previous.lastSeen
              : Date.now();
            endpoints.push({
              peerId: peerId,
              addresses: normalizedAddresses,
              connected: connected,
              lastSeen: lastSeen
            });
            if (normalizedAddresses.length > 0) {
              peerHints.push({
                peerId: peerId,
                addresses: normalizedAddresses
              } as PeerHint);
            }
          } else {
            this.localPeer = peerId;
            if (this.streamKey === 'demo-stream') {
              this.streamKey = `${peerId}-stream`;
            }
          }
        });
      }
      this.consumeAddressArray(this.node.listenAddresses(), ipv4, ipv6All, ipv6Global, candidateMultiaddrs);
      this.consumeAddressArray(this.node.dialableAddresses(), ipv4, ipv6All, ipv6Global, candidateMultiaddrs);
      if (this.havePeersChanged(endpoints)) {
        this.peers = endpoints;
      }
      const ipv4Array: string[] = Array.from(ipv4);
      let ipv6AllArray: string[] = this.uniqueStrings(Array.from(ipv6All));
      const ipv6GlobalArray: string[] = Array.from(ipv6Global);
      const persistedIpv6 = this.uniqueStrings(this.readStorageArray('__nimActiveIPv6'));
      if (ipv6AllArray.length === 0 && persistedIpv6.length > 0) {
        ipv6AllArray = this.uniqueStrings(persistedIpv6);
      }
      if (discoveredIpv4 && discoveredIpv4 !== this.activeIpv4) {
        this.activeIpv4 = discoveredIpv4;
        this.applyPreferredIpv4();
      }
      if (peerHints.length > 0) {
        this.registerPeerHintsBatch(peerHints, 'lan-endpoints');
      }
      const preferredIpv4 = interfaceAddrs.ipv4 && interfaceAddrs.ipv4.length > 0 ? interfaceAddrs.ipv4 : ipv4Array[0];
      const preferredIpv6Candidate =
        interfaceAddrs.ipv6 && interfaceAddrs.ipv6.length > 0
          ? interfaceAddrs.ipv6
          : (ipv6GlobalArray.length > 0 ? ipv6GlobalArray[0] : ipv6AllArray[0]);
      const preferredIpv6 = preferredIpv6Candidate && preferredIpv6Candidate.length > 0 ? preferredIpv6Candidate : '';
      const resolvedIpv4List = preferredIpv4 && preferredIpv4.length > 0 ? [preferredIpv4] : ipv4Array;
      let resolvedIpv6List = preferredIpv6 && preferredIpv6.length > 0 ? [preferredIpv6] : ipv6AllArray;
      let ipv6Source = '';
      if (interfaceAddrs.ipv6 && interfaceAddrs.ipv6.length > 0) {
        ipv6Source = 'interface';
      } else if (ipv6GlobalArray.length > 0) {
        ipv6Source = 'lan-endpoint-global';
      } else if (ipv6AllArray.length > 0) {
        ipv6Source = 'lan-endpoint';
      }
      if (resolvedIpv6List.length === 0 && persistedIpv6.length > 0) {
        resolvedIpv6List = this.uniqueStrings(persistedIpv6);
        if (!ipv6Source) {
          ipv6Source = 'persisted';
        }
      }
      if (ipv6AllArray.length > 0) {
        if (!this.arraysEqual(persistedIpv6, ipv6AllArray)) {
          this.persistInterfaceArray('__nimActiveIPv6', ipv6AllArray);
        }
      }
      if (!this.arraysEqual(this.ipv4List, resolvedIpv4List)) {
        this.ipv4List = resolvedIpv4List;
      }
      const existingIpv6 = this.ipv6List ? this.ipv6List.slice() : [];
      const mergedIpv6 = resolvedIpv6List.length > 0 ? resolvedIpv6List : existingIpv6;
      if (mergedIpv6.length > 0) {
        const uniqueIpv6 = this.uniqueStrings(mergedIpv6);
        if (!this.arraysEqual(this.ipv6List, uniqueIpv6)) {
          this.ipv6List = uniqueIpv6;
        }
        if (ipv6Source.length === 0) {
          ipv6Source = 'existing';
        }
        hilog.info(
          LOG_DOMAIN,
          LOG_TAG,
          'applyEndpoints resolved IPv6 source=%{public}s values=%{public}s',
          ipv6Source,
          JSON.stringify(this.ipv6List)
        );
      } else {
        hilog.warn(LOG_DOMAIN, LOG_TAG, 'applyEndpoints still missing IPv6; existing=%{public}d', existingIpv6.length);
      }
      this.publishExternalAddresses(this.uniqueStrings(candidateMultiaddrs));
      this.ensureSelectedPeer();
    } catch (err) {
      this.statusMessage = `Parse endpoints failed`;
    }
  }

  private registerPeerHintsBatch(hints: PeerHint[], source: string) {
    if (!this.node || !this.node.handle) {
      return;
    }
    hints.forEach((hint: PeerHint) => {
      if (!hint.peerId || hint.peerId.length === 0 || !this.isForeignPeer(hint.peerId)) {
        return;
      }
      const addresses = this.uniqueStrings(hint.addresses ?? []).filter(addr => {
        if (addr.indexOf('/ip6/') >= 0) {
          const ip = this.extractIp(addr);
          return isGlobalIpv6(ip);
        }
        return true;
      });
      const effective = addresses.length > 0 ? addresses : this.uniqueStrings(hint.addresses ?? []);
      if (effective.length === 0) {
        return;
      }
      const ok = this.node.registerPeerHints(hint.peerId, effective, source);
      if (!ok) {
        console.warn(`registerPeerHints failed peer=${hint.peerId} source=${source}`);
      }
    });
  }

  private publishExternalAddresses(candidates: string[]) {
    if (!this.node || !this.node.handle) {
      return;
    }
    if (!this.localPeer || this.localPeer.length === 0) {
      return;
    }
    this.uniqueStrings(candidates).forEach((addr: string) => {
      if (!addr || addr.length === 0) {
        return;
      }
      if (addr.indexOf('/p2p/') >= 0 || addr.indexOf('/ip4/0.0.0.0/') >= 0 || addr.indexOf('/ip4/127.0.0.1/') >= 0) {
        return;
      }
      if (addr.indexOf('/ip6/') >= 0) {
        const ip = this.extractIp(addr);
        if (!isGlobalIpv6(ip)) {
          return;
        }
      }
      const sanitized = addr.endsWith('/') ? addr.substring(0, addr.length - 1) : addr;
      const candidate = `${sanitized}/p2p/${this.localPeer}`;
      const ok = this.node.addExternalAddress(candidate);
      if (!ok) {
        console.info(`addExternalAddress skipped: ${candidate}`);
      }
    });
  }

  private splitMultiaddr(value: string): string[] {
    if (!value || value.length === 0) {
      return [];
    }
    try {
      return value
        .replace(/\n/g, ',')
        .split(',')
        .map((item: string) => item.trim())
        .filter((item: string) => item.length > 0);
    } catch (err) {
      console.warn(`splitMultiaddr failed: ${err}`);
      return [value.trim()];
    }
  }

  private uniqueStrings(values: string[]): string[] {
    const seen: Set<string> = new Set();
    const result: string[] = [];
    values.forEach((value: string) => {
      if (typeof value === 'string') {
        const trimmed = value.trim();
        if (trimmed.length > 0 && !seen.has(trimmed)) {
          seen.add(trimmed);
          result.push(trimmed);
        }
      }
    });
    return result;
  }

  private arraysEqual(a: string[], b: string[]): boolean {
    if (a === b) {
      return true;
    }
    if (!a || !b || a.length !== b.length) {
      return false;
    }
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) {
        return false;
      }
    }
    return true;
  }

  private havePeersChanged(next: PeerInfo[]): boolean {
    if (!Array.isArray(next) || !Array.isArray(this.peers)) {
      return true;
    }
    if (this.peers.length !== next.length) {
      return true;
    }
    for (let i = 0; i < next.length; i++) {
      const prev = this.peers[i];
      const curr = next[i];
      if (!prev || prev.peerId !== curr.peerId || prev.connected !== curr.connected) {
        return true;
      }
      if (!this.arraysEqual(prev.addresses, curr.addresses)) {
        return true;
      }
    }
    return false;
  }

  private getIPv4DisplayLines(): string[] {
    return this.ipv4List.length > 0 ? this.ipv4List : ['N/A'];
  }

  private getIPv6DisplayLines(): string[] {
    if (this.ipv6List.length > 0) {
      return this.ipv6List;
    }
    const stored = this.readStorageArray('__nimActiveIPv6');
    if (stored.length > 0) {
      return this.uniqueStrings(stored);
    }
    return ['N/A'];
  }

  private getAddressDisplayLines(addresses: string[]): string[] {
    return addresses.length > 0 ? addresses : ['No addresses'];
  }

  private consumeAddressArray(raw: string, ipv4Set: Set<string>, ipv6AllSet: Set<string>, ipv6GlobalSet: Set<string>, collector: string[]) {
    if (!raw || raw.length === 0) {
      return;
    }
    try {
      const parsedResult = JSON.parse(raw) as Array<string>;
      if (!Array.isArray(parsedResult)) {
        return;
      }
      parsedResult.forEach((item: string) => {
        if (typeof item !== 'string' || item.length === 0) {
          return;
        }
        const segments = this.splitMultiaddr(item);
        segments.forEach((segment: string) => {
          collector.push(segment);
          const ip = this.extractIp(segment);
          if (!ip || ip.length === 0 || ip === '0.0.0.0' || ip === '::') {
            return;
          }
          if (segment.indexOf('/ip6/') >= 0) {
            ipv6AllSet.add(ip);
            if (isGlobalIpv6(ip)) {
              ipv6GlobalSet.add(ip);
            }
          } else {
            ipv4Set.add(ip);
          }
        });
      });
    } catch (err) {
      console.warn(`consumeAddressArray parse failed: ${err}`);
    }
  }

  private loadPeerMultiaddrs(key: string, fallback: string[]): string[] {
    const stored = this.readStorageArray(key);
    if (stored.length > 0) {
      return this.uniqueStrings(stored);
    }
    return this.uniqueStrings(fallback);
  }

  private aggregatePeerHints(multiaddrs: string[]): PeerHint[] {
    const grouped: Map<string, string[]> = new Map();
    multiaddrs.forEach((addr: string) => {
      const trimmed = addr ? addr.trim() : '';
      if (!trimmed) {
        return;
      }
      const segments = this.splitMultiaddr(trimmed);
      segments.forEach((segment: string) => {
        const peerId = this.extractPeerIdFromMultiaddr(segment);
        if (!peerId) {
          return;
        }
        if (!grouped.has(peerId)) {
          grouped.set(peerId, []);
        }
        const list = grouped.get(peerId)!;
        if (list.indexOf(segment) < 0) {
          list.push(segment);
        }
      });
    });
    const hints: PeerHint[] = [];
    grouped.forEach((value: string[], key: string) => {
      hints.push({ peerId: key, addresses: value } as PeerHint);
    });
    return hints;
  }

  private extractPeerIdFromMultiaddr(multiaddr: string): string {
    const marker = '/p2p/';
    const idx = multiaddr.lastIndexOf(marker);
    if (idx < 0) {
      return '';
    }
    const start = idx + marker.length;
    if (start >= multiaddr.length) {
      return '';
    }
    const endIdx = multiaddr.indexOf('/', start);
    return endIdx >= 0 ? multiaddr.substring(start, endIdx) : multiaddr.substring(start);
  }

  private registerStaticPeerHints() {
    const hints = this.aggregatePeerHints(this.bootstrapMultiaddrs.concat(this.relayMultiaddrs));
    if (hints.length > 0) {
      this.registerPeerHintsBatch(hints, 'static-config');
    }
  }

  private async connectStaticMultiaddrs() {
    if (!this.node || !this.node.handle) {
      return;
    }
    const addresses = this.uniqueStrings(this.bootstrapMultiaddrs.concat(this.relayMultiaddrs));
    for (const addr of addresses) {
      if (!addr || addr.length === 0) {
        continue;
      }
      const ok = await this.node.connectMultiaddrAsync(addr);
      if (!ok) {
        console.info(`connectMultiaddr failed addr=${addr}`);
      }
    }
  }

  private startConnectivityWatchdog() {
    this.stopConnectivityWatchdog();
    this.maintainTimer = setInterval(() => {
      this.maintainConnectivity().catch((error: Error | string) => {
        const reason = error instanceof Error ? error.message : `${error}`;
        console.warn(`maintainConnectivity error: ${reason}`);
      });
    }, 7000);
  }

  private stopConnectivityWatchdog() {
    if (this.maintainTimer >= 0) {
      clearInterval(this.maintainTimer);
      this.maintainTimer = -1;
    }
  }

  private async maintainConnectivity() {
    if (!this.node || !this.node.handle) {
      return;
    }
    const now = Date.now();
    for (const peer of this.peers) {
      if (!peer.peerId || peer.peerId.length === 0 || !this.isForeignPeer(peer.peerId)) {
        continue;
      }
      if (peer.connected) {
        continue;
      }
      if (this.node.isPeerConnected(peer.peerId)) {
        continue;
      }
      const addresses = this.uniqueStrings(peer.addresses ?? []).filter((addr: string) => {
        if (addr.indexOf('/ip6/') >= 0) {
          const ip = this.extractIp(addr);
          return isGlobalIpv6(ip);
        }
        return true;
      });
      const effectiveAddresses = addresses.length > 0 ? addresses : this.uniqueStrings(peer.addresses ?? []);
      if (effectiveAddresses.length === 0) {
        continue;
      }
      const sortedAddresses = effectiveAddresses.slice().sort((a: string, b: string) => {
        const score = (addr: string) => (addr.indexOf('/ip6/') >= 0 ? 0 : 1);
        return score(a) - score(b);
      });
      const last = this.lastConnectAttempt.get(peer.peerId) ?? 0;
      if (now - last < 6000) {
        continue;
      }
      this.lastConnectAttempt.set(peer.peerId, now);
      this.node.registerPeerHints(peer.peerId, sortedAddresses, 'watchdog');
      const code = await this.node.connectPeerAsync(peer.peerId);
      if (code !== 0) {
        console.info(`watchdog connectPeer async failed peer=${peer.peerId} code=${code}`);
      }
      for (const addr of sortedAddresses) {
        if (!addr || addr.length === 0) {
          continue;
        }
        const ok = await this.node.connectMultiaddrAsync(addr);
        if (!ok) {
          console.info(`watchdog connectMultiaddr failed addr=${addr}`);
        }
      }
    }
  }

  private enqueueNodeEvents(raw: string) {
    if (!raw || raw.length === 0) {
      return;
    }
    if (this.eventRawQueue.length >= 24) {
      this.eventRawQueue.shift();
    }
    this.eventRawQueue.push(raw);
    if (!this.eventParseActive) {
      this.eventParseActive = true;
      this.scheduleEventPump();
    }
  }

  private scheduleEventPump() {
    Promise.resolve().then(() => {
      this.processEventQueueChunk();
    });
  }

  private processEventQueueChunk() {
    let processed = 0;
    while (this.eventRawQueue.length > 0 && processed < 4) {
      const next = this.eventRawQueue.shift();
      if (!next) {
        continue;
      }
      let events: ParsedNodeEvent[] = [];
      try {
        events = parseNodeEvents(next);
      } catch (err) {
        const message = err ? `${err}` : 'unknown error';
        hilog.warn(LOG_DOMAIN, LOG_TAG, 'processEventQueue parse failed %{public}s', message);
        continue;
      }
      if (!events || events.length === 0) {
        processed++;
        continue;
      }
      this.processEvents(events);
      processed++;
    }
    if (this.eventRawQueue.length > 0) {
      this.scheduleEventPump();
    } else {
      this.eventParseActive = false;
    }
  }

  private processEvents(events: ParsedNodeEvent[]) {
    events.forEach((entry: ParsedNodeEvent) => {
      if (!entry || !entry.payload) {
        return;
      }
      let payload: NodeEventPayload | null = null;
      try {
        payload = JSON.parse(entry.payload) as NodeEventPayload;
      } catch (_) {
        return;
      }
      if (payload) {
        try {
          this.handleEvent(payload);
        } catch (err) {
          console.warn(`handleEvent failed: ${err}`);
        }
      }
    });
  }

  private handleEvent(payload: NodeEventPayload) {
    const type = payload.type ?? '';
    switch (type) {
      case 'MdnsPeerDiscovered': {
        const peerId = payload.peer_id ?? '';
        if (!this.isForeignPeer(peerId)) {
          break;
        }
        const addresses: string[] = [];
        if (Array.isArray(payload.addresses)) {
          payload.addresses.forEach((candidate: AddressCandidate) => {
            if (typeof candidate === 'string' && candidate.length > 0) {
              const segments = this.splitMultiaddr(candidate);
              segments.forEach((segment: string) => addresses.push(segment));
            } else if (candidate !== undefined && candidate !== null) {
              console.warn(`Ignoring non-string mdns address: ${JSON.stringify(candidate)}`);
            }
          });
        }
        this.updatePeer(peerId, {
          peerId: peerId,
          addresses: addresses,
          connected: false,
          lastSeen: payload.timestamp_ms ?? Date.now()
        });
        break;
      }
      case 'MdnsDialError': {
        const peerId = payload.peer_id ?? '';
        const reason = payload.reason ?? 'unknown';
        const addresses = Array.isArray(payload.addresses)
          ? payload.addresses.map((item: AddressCandidate) => (typeof item === 'string' ? item : JSON.stringify(item ?? null)))
          : [];
        const message = `mDNS拨号失败 peer=${peerId} reason=${reason} addr=${addresses.join(',')}`;
        this.statusMessage = message;
        console.warn(message);
        if (this.isForeignPeer(peerId)) {
          this.updatePeer(peerId, {
            peerId: peerId,
            addresses: addresses,
            connected: false,
            lastSeen: payload.timestamp_ms ?? Date.now()
          });
        }
        break;
      }
      case 'ConnectionEstablished': {
        const peerId = payload.peer_id ?? '';
        if (!this.isForeignPeer(peerId)) {
          break;
        }
        this.updatePeer(peerId, undefined, true);
        break;
      }
      case 'ConnectionClosed': {
        const peerId = payload.peer_id ?? '';
        if (!this.isForeignPeer(peerId)) {
          break;
        }
        this.updatePeer(peerId, undefined, false);
        break;
      }
      case 'MessageReceived': {
        const peerId = payload.peer_id ?? '';
        if (!this.isForeignPeer(peerId)) {
          break;
        }
        const payloadValue = payload.payload;
        const body = typeof payloadValue === 'string'
          ? payloadValue
          : JSON.stringify(payloadValue ?? {});
        const messageId = payload.message_id ?? `${Date.now()}`;
        const timestamp = payload.timestamp_ms ?? Date.now();
        this.recordMessage(peerId, {
          messageId: messageId,
          fromSelf: false,
          body: body,
          timestamp: timestamp,
          acked: true
        });
        break;
      }
      case 'DirectMessageAck': {
        const peerId = payload.peer_id ?? '';
        if (!this.isForeignPeer(peerId)) {
          break;
        }
        const messageId = payload.message_id ?? '';
        const success = payload.success ?? false;
        if (success && messageId) {
          this.markMessageAck(peerId, messageId);
        }
        this.setDmPending(false);
        break;
      }
      case 'ContentFeedItem': {
        const entry = this.parseFeedEnvelope(payload);
        if (entry) {
          const items = [entry].concat(this.feedItems);
          this.feedItems = items.slice(0, 128);
        }
        break;
      }
      case 'LivestreamFrame': {
        const frame: LiveFrame = {
          streamKey: payload.stream_key ?? '',
          frameIndex: payload.frame_index ?? 0,
          payloadSize: payload.payload_size ?? 0,
          timestamp: payload.timestamp_ms ?? Date.now()
        };
        const frames = [frame].concat(this.liveFrames);
        this.liveFrames = frames.slice(0, 64);
        break;
      }
      default:
        break;
    }
  }

  private isForeignPeer(peerId: string | undefined | null): boolean {
    if (!peerId || peerId.length === 0) {
      return false;
    }
    if (!this.localPeer || this.localPeer.length === 0) {
      return true;
    }
    return peerId !== this.localPeer;
  }

  private updatePeer(peerId: string, replacement?: PeerInfo, connected?: boolean) {
    if (!this.isForeignPeer(peerId)) {
      return;
    }
    const peers = this.peers.filter(item => item.peerId !== peerId);
    let next = replacement;
    if (!next) {
      const current = this.peers.find(item => item.peerId === peerId);
      if (current) {
        next = {
          peerId: current.peerId,
          addresses: current.addresses,
          connected: connected !== undefined ? connected : current.connected,
          lastSeen: Date.now()
        };
      }
    } else if (connected !== undefined) {
      next.connected = connected;
    }
    if (!next) {
      return;
    }
    this.peers = [next].concat(peers);
    this.ensureSelectedPeer();
  }

  private recordMessage(peerId: string, message: MessageItem) {
    if (!this.isForeignPeer(peerId)) {
      return;
    }
    const existing = this.chatStore.get(peerId) ?? [];
    existing.push(message);
    if (existing.length > 200) {
      existing.shift();
    }
    this.chatStore.set(peerId, existing);
    if (this.selectedPeer === peerId) {
      this.messageList = existing.slice();
    }
  }

  private ensureSelectedPeer() {
    if (this.peers.length === 0) {
      if (this.selectedPeer) {
        this.selectedPeer = '';
        this.messageList = [];
      }
      return;
    }
    const stillPresent = this.selectedPeer && this.peers.find(item => item.peerId === this.selectedPeer);
    if (!stillPresent) {
      const fallback = this.peers[0]?.peerId ?? '';
      this.selectedPeer = fallback;
      if (fallback) {
        this.updateMessageList(fallback);
      } else {
        this.messageList = [];
      }
    }
  }

  private markMessageAck(peerId: string, messageId: string) {
    if (!this.isForeignPeer(peerId)) {
      return;
    }
    const existing = this.chatStore.get(peerId);
    if (!existing) {
      return;
    }
    existing.forEach(item => {
      if (item.messageId === messageId) {
        item.acked = true;
      }
    });
    if (this.selectedPeer === peerId) {
      this.messageList = existing.slice();
    }
  }

  private updateMessageList(peerId: string) {
    const existing = this.chatStore.get(peerId) ?? [];
    this.messageList = existing.slice();
  }

  private extractIp(addr: string): string {
    if (!addr) {
      return '';
    }
    const marker = addr.indexOf('/ip6/') >= 0 ? '/ip6/' : (addr.indexOf('/ip4/') >= 0 ? '/ip4/' : '');
    if (!marker) {
      return '';
    }
    const start = addr.indexOf(marker) + marker.length;
    const rest = addr.substring(start);
    const slash = rest.indexOf('/');
    return slash >= 0 ? rest.substring(0, slash) : rest;
  }

  private buildHardwareFingerprint(): string {
    const hardwareParts: string[] = [];
    try {
      const appendPart = (label: string, getter: () => string) => {
        try {
          const value = getter();
          if (value && value.length > 0) {
            hardwareParts.push(`${label}:${value}`);
          }
        } catch (err) {
          console.warn(`deviceInfo ${label} read failed: ${err}`);
        }
      };
      // HarmonyOS 对部分设备标识（UDID/serial）需要高权限；直接读取会导致拒绝访问甚至崩溃，跳过这类字段。
      appendPart('marketName', () => deviceInfo.marketName);
      appendPart('productSeries', () => deviceInfo.productSeries);
      appendPart('productModel', () => deviceInfo.productModel);
      appendPart('productModelAlias', () => deviceInfo.productModelAlias);
      appendPart('softwareModel', () => deviceInfo.softwareModel);
      appendPart('hardwareModel', () => deviceInfo.hardwareModel);
      appendPart('brand', () => deviceInfo.brand);
      appendPart('manufacture', () => deviceInfo.manufacture);
      appendPart('displayVersion', () => deviceInfo.displayVersion);
      appendPart('incrementalVersion', () => deviceInfo.incrementalVersion);
      appendPart('osFullName', () => deviceInfo.osFullName);
      appendPart('bootloaderVersion', () => deviceInfo.bootloaderVersion);
      appendPart('abiList', () => deviceInfo.abiList);
    } catch (err) {
      console.warn(`deviceInfo fingerprint failed: ${err}`);
    }
    if (hardwareParts.length > 0) {
      return `${PLATFORM_IDENTITY_SALT}|${hardwareParts.join('|')}`;
    }

    const parts: string[] = [];
    const iface = this.readStorageArray('__nimActiveInterface');
    if (iface.length > 0) {
      parts.push(`if:${iface[0]}`);
    }
    const ipv4 = this.readStorageArray('__nimActiveIPv4');
    ipv4.forEach((item: string) => {
      if (item.length > 0) {
        parts.push(`v4:${item}`);
      }
    });
    const ipv6 = this.readStorageArray('__nimActiveIPv6');
    ipv6.forEach((item: string) => {
      if (item.length > 0) {
        parts.push(`v6:${item}`);
      }
    });
    const netId = this.readStorageArray('__nimActiveNetId');
    if (netId.length > 0) {
      parts.push(`net:${netId[0]}`);
    }
    if (parts.length === 0) {
      parts.push('fallback');
    }
    return `${PLATFORM_IDENTITY_SALT}|${parts.join('|')}`;
  }

  private deriveIdentityPayload(): IdentityPayload | null {
    const fingerprint = `${PLATFORM_IDENTITY_SALT}|${this.buildHardwareFingerprint()}`;
    if (!fingerprint || fingerprint.length === 0) {
      return null;
    }
    const payload = this.node.identityFromSeed(fingerprint);
    if (!payload) {
      return null;
    }
    try {
      const raw = JSON.parse(payload) as Record<string, string | undefined | null>;
      const errorValue = raw['error'];
      if (errorValue && errorValue.length > 0) {
        const detailValue = raw['detail'];
        const detail = detailValue && detailValue.length > 0 ? detailValue : '';
        console.warn(`identityFromSeed error: ${errorValue} detail=${detail}`);
        return null;
      }
      const privateKey = raw['privateKey'] ?? '';
      const publicKey = raw['publicKey'] ?? '';
      const peerId = raw['peerId'] ?? '';
      if (!privateKey || !publicKey || !peerId) {
        console.warn('identityFromSeed returned incomplete payload');
        return null;
      }
      const identity: IdentityPayload = {
        privateKey: privateKey,
        publicKey: publicKey,
        peerId: peerId,
        source: 'ohos-hardware'
      };
      return identity;
    } catch (err) {
      console.warn(`identityFromSeed parse failed: ${err}`);
      return null;
    }
  }

  private loadPersistedIdentity(): IdentityPayload | null {
    try {
      const stored = AppStorage.Get(IDENTITY_STORAGE_KEY);
      if (!stored || stored.length === 0) {
        return null;
      }
      const payload = stored[0];
      if (!payload || payload.length === 0) {
        return null;
      }
      const parsed = JSON.parse(payload) as IdentityPayload;
      if (!parsed || !parsed.privateKey || !parsed.publicKey || !parsed.peerId) {
        return null;
      }
      return parsed;
    } catch (err) {
      console.warn(`loadPersistedIdentity failed: ${err}`);
      return null;
    }
  }

  private persistIdentity(identity: IdentityPayload): void {
    const write = AppStorage.SetOrCreate;
    if (typeof write !== 'function') {
      return;
    }
    try {
      write(IDENTITY_STORAGE_KEY, [JSON.stringify(identity)]);
    } catch (err) {
      console.warn(`persistIdentity failed: ${err}`);
    }
  }

  private persistInterfaceArray(key: string, values: string[]): void {
    if (!values || values.length === 0) {
      return;
    }
    const writer = AppStorage.SetOrCreate;
    if (typeof writer !== 'function') {
      return;
    }
    try {
      writer(key, values);
    } catch (err) {
      console.warn(`persistInterfaceArray failed key=${key}: ${err}`);
    }
  }

  private buildConfig(): string {
    const interfaceAddrs = this.collectInterfaceAddresses();
    const storedIpv4 = this.readStorageArray('__nimActiveIPv4');
    const storedIpv6 = this.readStorageArray('__nimActiveIPv6');
    const listen: string[] = [];
    const ipv4 = interfaceAddrs.ipv4 && interfaceAddrs.ipv4.length > 0 ? interfaceAddrs.ipv4 : storedIpv4.find((ip: string) => ip !== '0.0.0.0');
    const selectedIpv4 = ipv4 && ipv4.length > 0 ? ipv4 : '0.0.0.0';
    this.activeIpv4 = selectedIpv4;
    if (selectedIpv4 && selectedIpv4.length > 0 && selectedIpv4 !== '0.0.0.0') {
      this.persistInterfaceArray('__nimActiveIPv4', [selectedIpv4]);
    }
    if (interfaceAddrs.ipv6 && interfaceAddrs.ipv6.length > 0) {
      this.persistInterfaceArray('__nimActiveIPv6', [interfaceAddrs.ipv6]);
    }
    listen.push(`/ip4/${selectedIpv4}/tcp/0`);
    listen.push(`/ip4/${selectedIpv4}/udp/0/quic-v1`);
    const storedIpv6Value = storedIpv6.length > 0 ? storedIpv6[0] : '';
    const selectedIpv6 = interfaceAddrs.ipv6 && interfaceAddrs.ipv6.length > 0 ? interfaceAddrs.ipv6 : storedIpv6Value;
    if (selectedIpv6 && selectedIpv6.length > 0) {
      listen.push(`/ip6/${selectedIpv6}/tcp/0`);
      listen.push(`/ip6/${selectedIpv6}/udp/0/quic-v1`);
    } else {
      listen.push('/ip6/::/tcp/0');
      listen.push('/ip6/::/udp/0/quic-v1');
    }
    const bootstrapMultiaddrs = this.loadPeerMultiaddrs('__nimBootstrapMultiaddrs', DEFAULT_BOOTSTRAP_MULTIADDRS);
    const relayMultiaddrs = this.loadPeerMultiaddrs('__nimRelayMultiaddrs', DEFAULT_RELAY_MULTIADDRS);
    this.bootstrapMultiaddrs = bootstrapMultiaddrs;
    this.relayMultiaddrs = relayMultiaddrs;
    const config: NodeConfig = {
      listenAddresses: listen,
      automations: {
        gossipsub: true,
        directStream: true,
       livestream: true,
        rendezvous: true,
        autonat: true,
        circuitRelay: true
      },
      extra: {
        mdns: {
          service: '_unimaker._udp.',
          preferredIpv4: selectedIpv4 !== '0.0.0.0' ? selectedIpv4 : '',
          forceSearchMeta: true
        },
        bootstrapMultiaddrs: bootstrapMultiaddrs,
        relayMultiaddrs: relayMultiaddrs
      }
    };
    const persistedIdentity = this.loadPersistedIdentity();
    if (persistedIdentity) {
      config.identity = persistedIdentity;
      this.localPeer = persistedIdentity.peerId;
      if (this.streamKey === 'demo-stream') {
        this.streamKey = `${persistedIdentity.peerId}-stream`;
      }
      return JSON.stringify(config);
    }
    const identity = this.deriveIdentityPayload();
    if (identity) {
      config.identity = identity;
      this.localPeer = identity.peerId;
      if (this.streamKey === 'demo-stream') {
        this.streamKey = `${identity.peerId}-stream`;
      }
      this.persistIdentity(identity);
    }
    return JSON.stringify(config);
  }

  private readStorageArray(key: string): string[] {
    try {
      const stored = AppStorage.Get(key);
      if (!stored) {
        return [];
      }
      const filtered: string[] = [];
      stored.forEach((value: string) => {
        if (typeof value === 'string' && value.length > 0) {
          filtered.push(value);
        }
      });
      return filtered;
    } catch (_) {
      return [];
    }
  }

  private sanitizeIpAddress(address: string | undefined | null): string {
    if (!address) {
      return '';
    }
    const trimmed = address.trim();
    if (trimmed.length === 0) {
      return '';
    }
    const percentIdx = trimmed.indexOf('%');
    return percentIdx >= 0 ? trimmed.substring(0, percentIdx) : trimmed;
  }

  private pickStoredInterfaceAddress(key: string, predicate: (value: string) => boolean): string | undefined {
    const stored = this.readStorageArray(key);
    for (const raw of stored) {
      const sanitized = this.sanitizeIpAddress(raw);
      if (sanitized && predicate(sanitized)) {
        return sanitized;
      }
    }
    return undefined;
  }

  private unwrapNetAddress(candidate: NetAddressLike, depth: number = 0): string {
    if (candidate === undefined || candidate === null || depth > 6) {
      return '';
    }
    if (typeof candidate === 'string') {
      return candidate;
    }
    const serialized = this.safeSerialize(candidate);
    const extracted = this.extractIpFromSerialized(serialized);
    if (extracted.length > 0) {
      return extracted;
    }
    const node = candidate as LinkAddressNode;
    if (node.address !== undefined) {
      const next = this.unwrapNetAddress(node.address, depth + 1);
      if (next.length > 0) {
        return next;
      }
    }
    if (node.value !== undefined) {
      const next = this.unwrapNetAddress(node.value, depth + 1);
      if (next.length > 0) {
        return next;
      }
    }
    return '';
  }

  private readNetFamily(candidate: NetAddressLike, depth: number = 0): number | undefined {
    if (candidate === undefined || candidate === null || depth > 6) {
      return undefined;
    }
    if (typeof candidate === 'string') {
      if (candidate.indexOf(':') >= 0) {
        return 2;
      }
      if (candidate.indexOf('.') >= 0) {
        return 1;
      }
      return undefined;
    }
    const serialized = this.safeSerialize(candidate);
    const fromSerialized = this.extractFamilyFromSerialized(serialized);
    if (fromSerialized !== undefined) {
      return fromSerialized;
    }
    const node = candidate as LinkAddressNode;
    if (typeof node.family === 'number') {
      return node.family;
    }
    if (node.address !== undefined) {
      const nested = this.readNetFamily(node.address, depth + 1);
      if (nested !== undefined) {
        return nested;
      }
    }
    if (node.value !== undefined) {
      return this.readNetFamily(node.value, depth + 1);
    }
    return undefined;
  }

  private safeSerialize(
    value: NetAddressLike | LinkAddressLike | connection.LinkAddress | connection.NetAddress | string | number | boolean | null | undefined
  ): string {
    if (!value) {
      return '';
    }
    try {
      return JSON.stringify(value);
    } catch (_) {
      return '';
    }
  }

  private extractIpFromSerialized(serialized: string): string {
    if (!serialized || serialized.length === 0) {
      return '';
    }
    const matcher = /\"([0-9A-Fa-f:%.\-]+)\"/g;
    let execResult: RegExpExecArray | null = matcher.exec(serialized);
    while (execResult) {
      const candidate = execResult[1];
      if (candidate.indexOf(':') >= 0 || candidate.indexOf('.') >= 0) {
        if (candidate.length > 1) {
          return candidate;
        }
      }
      execResult = matcher.exec(serialized);
    }
    return '';
  }

  private extractFamilyFromSerialized(serialized: string): number | undefined {
    if (!serialized || serialized.length === 0) {
      return undefined;
    }
    const match = /\"family\"\s*:\s*(\d+)/.exec(serialized);
    if (match && match[1]) {
      const parsed = Number(match[1]);
      if (!Number.isNaN(parsed)) {
        return parsed;
      }
    }
    return undefined;
  }

  private unwrapLinkAddress(candidate: LinkAddressLike): string {
    if (candidate === undefined || candidate === null) {
      return '';
    }
    if (typeof candidate === 'string') {
      return candidate;
    }
    const link = candidate as connection.LinkAddress;
    if (link.address !== undefined) {
      const resolved = this.unwrapNetAddress(link.address as NetAddressLike);
      if (resolved.length > 0) {
        return resolved;
      }
    }
    return this.unwrapNetAddress(candidate as NetAddressLike);
  }

  private readLinkFamily(candidate: LinkAddressLike): number | undefined {
    if (candidate === undefined || candidate === null) {
      return undefined;
    }
    if (typeof candidate === 'string') {
      if (candidate.indexOf(':') >= 0) {
        return 2;
      }
      if (candidate.indexOf('.') >= 0) {
        return 1;
      }
      return undefined;
    }
    const link = candidate as connection.LinkAddress;
    if (link.address !== undefined) {
      const nested = this.readNetFamily(link.address as NetAddressLike);
      if (nested !== undefined) {
        return nested;
      }
    }
    const node = candidate as LinkAddressNode;
    if (node.address !== undefined) {
      const nestedNode = this.readNetFamily(node.address);
      if (nestedNode !== undefined) {
        return nestedNode;
      }
    }
    if (node.value !== undefined) {
      return this.readNetFamily(node.value);
    }
    return undefined;
  }

  private normalizeLinkAddress(entry: LinkAddressLike): NormalizedLinkAddress | null {
    if (entry === undefined || entry === null) {
      return null;
    }
    if (typeof entry === 'string') {
      const sanitized = this.sanitizeIpAddress(entry);
      if (!sanitized) {
        return null;
      }
      const familyGuess = sanitized.indexOf(':') >= 0 ? 2 : (sanitized.indexOf('.') >= 0 ? 1 : undefined);
      return { address: sanitized, family: familyGuess };
    }
    const container = entry as connection.LinkAddress;
    let addressSource: NetAddressLike = undefined;
    if (container.address !== undefined) {
      addressSource = container.address as NetAddressLike;
    } else {
      const node = entry as LinkAddressNode;
      if (node.address !== undefined) {
        addressSource = node.address;
      } else if (node.value !== undefined) {
        addressSource = node.value;
      }
    }
    const address = this.unwrapNetAddress(addressSource);
    const sanitized = this.sanitizeIpAddress(address);
    if (!sanitized) {
      return null;
    }
    let family = this.readLinkFamily(entry);
    if (family === undefined) {
      if (sanitized.indexOf(':') >= 0) {
        family = 2;
      } else if (sanitized.indexOf('.') >= 0) {
        family = 1;
      }
    }
    return { address: sanitized, family: family };
  }

  private collectInterfaceAddresses(): PrimaryAddresses {
    const now = Date.now();
    const hasCached = (this.interfaceCache.ipv4 && this.interfaceCache.ipv4.length > 0) ||
      (this.interfaceCache.ipv6 && this.interfaceCache.ipv6.length > 0);
    if (hasCached && now - this.lastInterfaceRefresh < this.interfaceRefreshBackoffMs) {
      return this.interfaceCache;
    }

    let primaryIpv4: string | undefined = undefined;
    let primaryIpv6: string | undefined = undefined;
    let fallbackIpv4: string | undefined = undefined;
    let fallbackIpv6: string | undefined = undefined;
    let refreshSucceeded: boolean = false;
    let primaryIface: string | undefined = this.interfaceCache.iface;
    let fallbackIface: string | undefined = undefined;
    const syncLinkSnapshots: string[] = [];
    const storedIfaceList = this.readStorageArray('__nimActiveInterface');
    const storedIface = storedIfaceList.length > 0 ? storedIfaceList[0] : undefined;

    if (!this.disableSyncInterfaceLookup &&
      typeof connection.getAllNetsSync === 'function' &&
      typeof connection.getConnectionPropertiesSync === 'function') {
      const syncStart = Date.now();
      try {
        const nets = connection.getAllNetsSync();
        hilog.info(LOG_DOMAIN, LOG_TAG, 'collectInterfaceAddresses nets=%{public}d', nets.length);
        nets.forEach((handle) => {
          try {
            const props = connection.getConnectionPropertiesSync(handle) as ExtendedConnectionProps;
            const linksRaw: LinkAddressLike[] = Array.isArray(props.linkAddresses) ? props.linkAddresses : [];
            hilog.info(
              LOG_DOMAIN,
              LOG_TAG,
              'collectInterfaceAddresses netId=%{public}s linkCount=%{public}d',
              JSON.stringify(handle),
              linksRaw.length
            );
            const ifaceName = (props as ExtendedConnectionProps).interfaceName;
            const candidateIfName = typeof ifaceName === 'string' && ifaceName.length > 0 ? ifaceName : undefined;
            const ifacePreferred = this.isPreferredInterface(candidateIfName);
            linksRaw.forEach((link: LinkAddressLike) => {
              const normalized = this.normalizeLinkAddress(link);
              if (normalized) {
                syncLinkSnapshots.push(`${normalized.address}|${normalized.family}`);
              } else {
                syncLinkSnapshots.push(this.safeSerialize(link));
                return;
              }
              const address = normalized.address;
              const family = normalized.family;
              if (!address || address === '::') {
                return;
              }
              if (family === 1 || (family === undefined && address.indexOf(':') < 0)) {
                if (address !== '0.0.0.0' && address !== '127.0.0.1') {
                  if (!primaryIpv4 && (ifacePreferred || (!primaryIface && storedIface === candidateIfName))) {
                    primaryIpv4 = address;
                    primaryIface = candidateIfName ?? storedIface ?? primaryIface;
                  } else if (!fallbackIpv4 && !ifacePreferred) {
                    fallbackIpv4 = address;
                    fallbackIface = candidateIfName;
                  }
                }
                return;
              }
              const lower = address.toLowerCase();
              if (!lower.startsWith('fe80') && !primaryIpv6) {
                primaryIpv6 = address;
              } else if (!fallbackIpv6) {
                fallbackIpv6 = address;
              }
            });
          } catch (err) {
            const message = err ? `${err}` : 'unknown error';
            hilog.warn(LOG_DOMAIN, LOG_TAG, 'collectInterfaceAddresses properties error=%{public}s', message);
          }
        });
        refreshSucceeded = !!primaryIpv4 || !!primaryIpv6 || !!fallbackIpv6;
      } catch (err) {
        const message = err ? `${err}` : 'unknown error';
        hilog.warn(LOG_DOMAIN, LOG_TAG, 'collectInterfaceAddresses nets error=%{public}s', message);
      } finally {
        const elapsed = Date.now() - syncStart;
        if (elapsed > 300 && !this.disableSyncInterfaceLookup) {
          this.disableSyncInterfaceLookup = true;
          hilog.warn(
            LOG_DOMAIN,
            LOG_TAG,
            'collectInterfaceAddresses sync probe slow=%{public}dms; disabling sync path',
            elapsed
          );
        }
      }
    }

    if (syncLinkSnapshots.length > 0) {
      const label = !primaryIpv6 && !fallbackIpv6 ? 'without-ipv6' : 'with-ipv6';
      hilog.info(
        LOG_DOMAIN,
        LOG_TAG,
        'collectInterfaceAddresses link snapshots (%{public}s) %{public}s',
        label,
        JSON.stringify(syncLinkSnapshots.slice(0, 4))
      );
    }

    if (!primaryIpv6 && fallbackIpv6) {
      primaryIpv6 = fallbackIpv6;
    }
    if (!primaryIpv4) {
      if (fallbackIpv4) {
        primaryIpv4 = fallbackIpv4;
        primaryIface = fallbackIface;
      } else {
        primaryIpv4 = this.pickStoredInterfaceAddress('__nimActiveIPv4', (value: string) => value !== '0.0.0.0' && value !== '127.0.0.1');
      }
    }
    if (!primaryIpv6) {
      primaryIpv6 = this.pickStoredInterfaceAddress('__nimActiveIPv6', (value: string) => value !== '::');
    }

    if (!refreshSucceeded && ((primaryIpv4 && primaryIpv4.length > 0) || (primaryIpv6 && primaryIpv6.length > 0))) {
      refreshSucceeded = true;
    }

    const nextCache: PrimaryAddresses = {
      ipv4: this.interfaceCache.ipv4,
      ipv6: this.interfaceCache.ipv6,
      iface: this.interfaceCache.iface
    };
    if (primaryIpv4 && primaryIpv4.length > 0) {
      nextCache.ipv4 = primaryIpv4;
      nextCache.iface = primaryIface ?? nextCache.iface;
    }
    if (primaryIpv6 && primaryIpv6.length > 0) {
      nextCache.ipv6 = primaryIpv6;
    }
    if ((nextCache.ipv4 ?? '') !== (this.interfaceCache.ipv4 ?? '') ||
        (nextCache.ipv6 ?? '') !== (this.interfaceCache.ipv6 ?? '') ||
        (nextCache.iface ?? '') !== (this.interfaceCache.iface ?? '')) {
      this.interfaceCache = nextCache;
    }
    this.lastInterfaceRefresh = now;
    if (refreshSucceeded) {
      this.interfaceRefreshBackoffMs = 2000;
    } else {
      this.interfaceRefreshBackoffMs = Math.min(this.interfaceRefreshBackoffMs * 2, 30000);
    }

    if (primaryIpv4 && primaryIpv4.length > 0 && primaryIpv4 !== '0.0.0.0' && primaryIpv4 !== '127.0.0.1') {
      this.persistInterfaceArray('__nimActiveIPv4', [primaryIpv4]);
      if (primaryIface && primaryIface.length > 0) {
        this.persistInterfaceArray('__nimActiveInterface', [primaryIface]);
      }
    }
    if (primaryIpv6 && primaryIpv6.length > 0 && primaryIpv6 !== '::') {
      this.persistInterfaceArray('__nimActiveIPv6', [primaryIpv6]);
    }

    if ((!primaryIpv4 || primaryIpv4.length === 0) && (!primaryIpv6 || primaryIpv6.length === 0)) {
      if (!this.interfaceRefreshInFlight) {
        void this.requestInterfaceAddressesAsync(true);
      }
    }

    hilog.info(
      LOG_DOMAIN,
      LOG_TAG,
      'collectInterfaceAddresses result v4=%{public}s v6=%{public}s fallback=%{public}s backoff=%{public}d',
      primaryIpv4 ?? '<none>',
      primaryIpv6 ?? '<none>',
      fallbackIpv6 ?? '<none>',
      this.interfaceRefreshBackoffMs
    );

    return this.interfaceCache;
  }

  private async requestInterfaceAddressesAsync(force: boolean = false): Promise<void> {
    if (this.interfaceRefreshInFlight) {
      return;
    }
    const now = Date.now();
    if (!force && now - this.lastInterfaceRefresh < this.interfaceRefreshBackoffMs) {
      return;
    }
    if (typeof connection.getConnectionProperties !== 'function') {
      return;
    }
    this.interfaceRefreshInFlight = true;
    try {
      const handles: connection.NetHandle[] = [];
      const seenNetIds: Set<number> = new Set();
      if (typeof connection.getDefaultNet === 'function') {
        try {
          const handle = await connection.getDefaultNet();
          if (handle && handle.netId !== undefined) {
            const netId = Number(handle.netId);
            if (!Number.isNaN(netId) && !seenNetIds.has(netId)) {
              handles.push(handle);
              seenNetIds.add(netId);
            }
          } else if (handle) {
            handles.push(handle);
          }
        } catch (error) {
          const message = error ? `${error}` : 'unknown error';
          hilog.warn(LOG_DOMAIN, LOG_TAG, 'requestInterfaceAddressesAsync defaultNet error=%{public}s', message);
        }
      }
      if (handles.length === 0 && typeof connection.getAllNets === 'function') {
        try {
          const nets = await connection.getAllNets();
          nets.forEach((handle: connection.NetHandle) => {
            if (handle && handle.netId !== undefined) {
              const netId = Number(handle.netId);
              if (!Number.isNaN(netId) && !seenNetIds.has(netId)) {
                handles.push(handle);
                seenNetIds.add(netId);
              }
            } else if (handle) {
              handles.push(handle);
            }
          });
        } catch (error) {
          const message = error ? `${error}` : 'unknown error';
          hilog.warn(LOG_DOMAIN, LOG_TAG, 'requestInterfaceAddressesAsync allNets error=%{public}s', message);
        }
      }

      let primaryIpv4: string | undefined = undefined;
      let primaryIpv6: string | undefined = undefined;
      let fallbackIpv4: string | undefined = undefined;
      let fallbackIpv6: string | undefined = undefined;
      let primaryIface: string | undefined = this.interfaceCache.iface;
      let fallbackIface: string | undefined = undefined;
      const storedIfaceList = this.readStorageArray('__nimActiveInterface');
      const storedIface = storedIfaceList.length > 0 ? storedIfaceList[0] : undefined;
      const asyncLinkSnapshots: string[] = [];

      for (const handle of handles) {
        try {
          const props = await connection.getConnectionProperties(handle) as ExtendedConnectionProps;
          const links = Array.isArray(props.linkAddresses) ? props.linkAddresses : [];
          const ifaceName = (props as ExtendedConnectionProps).interfaceName;
          const candidateIfName = typeof ifaceName === 'string' && ifaceName.length > 0 ? ifaceName : undefined;
          const ifacePreferred = this.isPreferredInterface(candidateIfName);
          hilog.info(
            LOG_DOMAIN,
            LOG_TAG,
            'requestInterfaceAddressesAsync netId=%{public}d linkCount=%{public}d',
            handle && handle.netId !== undefined ? handle.netId : 0,
            links.length
          );
          for (const link of links) {
            const normalized = this.normalizeLinkAddress(link);
            if (normalized) {
              asyncLinkSnapshots.push(`${normalized.address}|${normalized.family}`);
            } else {
              asyncLinkSnapshots.push(this.safeSerialize(link));
              continue;
            }
            const address = normalized.address;
            const family = normalized.family;
            if (!address || address === '::') {
              continue;
            }
            if (family === 1 || (family === undefined && address.indexOf(':') < 0)) {
              if (address !== '0.0.0.0' && address !== '127.0.0.1') {
                if (!primaryIpv4 && (ifacePreferred || (!primaryIface && storedIface === candidateIfName))) {
                  primaryIpv4 = address;
                  primaryIface = candidateIfName ?? storedIface ?? primaryIface;
                } else if (!fallbackIpv4 && !ifacePreferred) {
                  fallbackIpv4 = address;
                  fallbackIface = candidateIfName;
                }
              }
              continue;
            }
            const lower = address.toLowerCase();
            if (!lower.startsWith('fe80') && !primaryIpv6) {
              primaryIpv6 = address;
            } else if (!fallbackIpv6) {
              fallbackIpv6 = address;
            }
          }
        } catch (error) {
          const message = error ? `${error}` : 'unknown error';
          hilog.warn(
            LOG_DOMAIN,
            LOG_TAG,
            'requestInterfaceAddressesAsync props error netId=%{public}s reason=%{public}s',
            JSON.stringify(handle),
            message
          );
        }
        if (primaryIpv4 && primaryIpv6) {
          break;
        }
      }

      if (asyncLinkSnapshots.length > 0) {
        const label = !primaryIpv6 && !fallbackIpv6 ? 'without-ipv6' : 'with-ipv6';
        hilog.info(
          LOG_DOMAIN,
          LOG_TAG,
          'requestInterfaceAddressesAsync link snapshots (%{public}s) %{public}s',
          label,
          JSON.stringify(asyncLinkSnapshots.slice(0, 4))
        );
      }

    hilog.info(
      LOG_DOMAIN,
      LOG_TAG,
      'requestInterfaceAddressesAsync synth result v4=%{public}s v6=%{public}s fallback=%{public}s handles=%{public}d',
      primaryIpv4 ?? '<none>',
      primaryIpv6 ?? '<none>',
      fallbackIpv6 ?? '<none>',
      handles.length
    );

    if (!primaryIpv6 && fallbackIpv6) {
      primaryIpv6 = fallbackIpv6;
    }
    if (!primaryIpv4) {
      if (fallbackIpv4) {
        primaryIpv4 = fallbackIpv4;
        primaryIface = fallbackIface;
      } else {
        primaryIpv4 = this.pickStoredInterfaceAddress('__nimActiveIPv4', (value: string) => value !== '0.0.0.0' && value !== '127.0.0.1');
      }
    }
    if (!primaryIpv6) {
      primaryIpv6 = this.pickStoredInterfaceAddress('__nimActiveIPv6', (value: string) => value !== '::');
    }

    if (primaryIpv4 || primaryIpv6) {
      const cacheChanged =
        (this.interfaceCache.ipv4 ?? '') !== (primaryIpv4 ?? '') ||
        (this.interfaceCache.ipv6 ?? '') !== (primaryIpv6 ?? '') ||
        (this.interfaceCache.iface ?? '') !== (primaryIface ?? '');
      if (cacheChanged) {
        this.interfaceCache = {
          ipv4: primaryIpv4,
          ipv6: primaryIpv6,
          iface: primaryIface
        };
      }
      this.lastInterfaceRefresh = Date.now();
      this.interfaceRefreshBackoffMs = 2000;
      if (primaryIpv4 && primaryIpv4.length > 0 && primaryIpv4 !== '0.0.0.0' && primaryIpv4 !== '127.0.0.1') {
        this.persistInterfaceArray('__nimActiveIPv4', [primaryIpv4]);
        if (primaryIface && primaryIface.length > 0) {
          this.persistInterfaceArray('__nimActiveInterface', [primaryIface]);
        }
        if (this.activeIpv4 !== primaryIpv4) {
          this.activeIpv4 = primaryIpv4;
          this.applyPreferredIpv4();
        }
        const updatedIpv4List = [primaryIpv4];
        if (!this.arraysEqual(this.ipv4List, updatedIpv4List)) {
          this.ipv4List = updatedIpv4List;
        }
      } else {
        const storedIpv4 = this.uniqueStrings(this.readStorageArray('__nimActiveIPv4').filter((value: string) => value !== '0.0.0.0' && value !== '127.0.0.1'));
        if (storedIpv4.length > 0 && !this.arraysEqual(this.ipv4List, storedIpv4)) {
          this.ipv4List = storedIpv4;
        }
      }
      if (primaryIpv6 && primaryIpv6.length > 0 && primaryIpv6 !== '::') {
        this.persistInterfaceArray('__nimActiveIPv6', [primaryIpv6]);
        const updatedIpv6List = [primaryIpv6];
        if (!this.arraysEqual(this.ipv6List, updatedIpv6List)) {
          this.ipv6List = updatedIpv6List;
        }
        hilog.info(LOG_DOMAIN, LOG_TAG, 'Active IPv6 updated %{public}s', primaryIpv6);
      } else {
        const storedIpv6 = this.uniqueStrings(this.readStorageArray('__nimActiveIPv6'));
        if (storedIpv6.length > 0 && !this.arraysEqual(this.ipv6List, storedIpv6)) {
          this.ipv6List = storedIpv6;
          hilog.info(LOG_DOMAIN, LOG_TAG, 'Reusing stored IPv6 %{public}s', JSON.stringify(storedIpv6));
        }
      }
    } else {
      hilog.warn(LOG_DOMAIN, LOG_TAG, 'requestInterfaceAddressesAsync found no usable address');
    }
    } catch (error) {
      const message = error ? `${error}` : 'unknown error';
      hilog.warn(LOG_DOMAIN, LOG_TAG, 'requestInterfaceAddressesAsync failed %{public}s', message);
    } finally {
      this.interfaceRefreshInFlight = false;
    }
  }

  private applyPreferredIpv4() {
    if (!this.node) {
      return;
    }
    const ipv4 = this.activeIpv4;
    if (!ipv4 || ipv4.length === 0 || ipv4 === '0.0.0.0') {
      return;
    }
    if (ipv4 === this.lastAppliedIpv4) {
      return;
    }
    const ok = this.node.setMdnsInterface(ipv4);
    if (ok) {
      this.lastAppliedIpv4 = ipv4;
      this.persistInterfaceArray('__nimActiveIPv4', [ipv4]);
    } else {
      console.warn(`setMdnsInterface failed: ${ipv4}`);
    }
  }

  private setDmPending(active: boolean) {
    if (!active) {
      this.dmPending = false;
      if (this.dmPendingTimer >= 0) {
        clearTimeout(this.dmPendingTimer);
        this.dmPendingTimer = -1;
      }
      return;
    }
    this.dmPending = true;
    if (this.dmPendingTimer >= 0) {
      clearTimeout(this.dmPendingTimer);
    }
    this.dmPendingTimer = setTimeout(() => {
      this.dmPending = false;
      this.dmPendingTimer = -1;
    }, 7000);
  }

  private async sendMessage() {
    if (!this.selectedPeer || !this.chatInput || !this.node) {
      return;
    }
    const ackCapable = this.node.supportsDirectAck();
    const messageId = `${Date.now()}`;
    const payload = JSON.stringify({
      op: 'dm',
      mid: messageId,
      body: this.chatInput,
      timestamp_ms: Date.now(),
      ackRequested: ackCapable
    });
    if (ackCapable) {
      this.setDmPending(true);
    } else {
      this.setDmPending(false);
    }
    this.recordMessage(this.selectedPeer, {
      messageId: messageId,
      fromSelf: true,
      body: this.chatInput,
      timestamp: Date.now(),
      acked: false
    });
    let ok = false;
    try {
      ok = await this.node.sendDirect(this.selectedPeer, payload);
    } catch (err) {
      const message = err ? `${err}` : 'unknown error';
      console.warn(`sendDirect async failed: ${message}`);
      ok = false;
    }
    if (!ok) {
      this.statusMessage = `DM send failed: ${this.node.lastDirectError()}`;
    } else {
      if (!ackCapable) {
        this.markMessageAck(this.selectedPeer, messageId);
      }
    }
    this.setDmPending(false);
    this.chatInput = '';
  }

  private publishFeedEntry() {
    if (!this.feedInput) {
      return;
    }
    const now = Date.now();
    const entry: LocalFeedEnvelope = {
      id: `${now}`,
      author: this.localPeer || 'unknown',
      body: this.feedInput,
      timestamp: now,
      timestamp_ms: now
    };
    if (this.attachDemo) {
      entry.media = [
        {
          id: `${now}-image`,
          type: 'image',
          label: 'Demo snapshot',
          mime: DEMO_IMAGE_MIME,
          encoding: 'base64',
          data: DEMO_IMAGE_BASE64
        },
        {
          id: `${now}-caption`,
          type: 'text',
          label: 'Caption',
          text: 'Sample multi-modal post generated on-device.'
        }
      ];
    }
    const ok = this.node.publishFeed(JSON.stringify(entry));
    if (!ok) {
      this.statusMessage = 'Feed publish failed';
    }
    const localItem = this.parseFeedItem(entry, this.localPeer || 'self', now);
    if (localItem) {
      this.feedItems = [localItem].concat(this.feedItems).slice(0, 128);
    }
    this.feedInput = '';
  }

  private publishLivestream() {
    if (!this.streamPayload) {
      return;
    }
    const key = this.streamKey || `${this.localPeer}-stream`;
    const ok = this.node.publishLivestream(key, this.streamPayload);
    if (!ok) {
      this.statusMessage = 'Livestream publish failed';
    }
    this.streamPayload = '';
  }

  private async connectPeer(peerId: string) {
    if (!this.node || !this.node.handle || !peerId || peerId.length === 0) {
      return;
    }
    this.statusMessage = `Dialing ${peerId}`;
    this.selectedPeer = peerId;
    this.updateMessageList(peerId);
    const peerState = this.peers.find(item => item.peerId === peerId);
    if (peerState && Array.isArray(peerState.addresses) && peerState.addresses.length > 0) {
      const addresses = this.uniqueStrings(peerState.addresses);
      for (const addr of addresses) {
        if (!addr || addr.length === 0) {
          continue;
        }
        await this.node.connectMultiaddrAsync(addr);
      }
    }
    const code = await this.node.connectPeerAsync(peerId);
    if (code !== 0) {
      const message = `连接 ${peerId} 失败 (code=${code})`;
      console.warn(message);
      this.statusMessage = message;
    }
  }

  build() {
    Column() {
      Text(`Status: ${this.statusMessage}`)
        .fontSize(16)
        .fontWeight(FontWeight.Bold)
        .padding({ bottom: 8 });

      Row() {
        ForEach(this.tabLabels, (label: string, index: number) => {
          Button(label)
            .margin({ right: 8 })
            .backgroundColor(this.selectedTab === index ? '#0A84FF' : '#1E1E1E')
            .onClick(() => {
              this.selectedTab = index;
            });
        }, (label: string, index: number) => `${index}-${label}`);
      }.padding({ bottom: 12 });

      if (this.selectedTab === 0) {
        this.renderOverview();
      } else if (this.selectedTab === 1) {
        this.renderChat();
      } else if (this.selectedTab === 2) {
        this.renderFeed();
      } else {
        this.renderLive();
      }
    }
    .padding(16)
    .width('100%')
    .height('100%');
  }

  @Builder
  private renderOverview() {
    Column() {
      Text(`Local peer: ${this.localPeer || 'unknown'}`);
      Column() {
        Text('IPv4');
        ForEach(this.getIPv4DisplayLines(), (line: string, index: number) => Text(line), (line: string, index: number) => `ipv4-${index}-${line}`);
      }.margin({ bottom: 8 });
      Column() {
        Text('IPv6');
        ForEach(this.getIPv6DisplayLines(), (line: string, index: number) => Text(line), (line: string, index: number) => `ipv6-${index}-${line}`);
      }.margin({ bottom: 12 });
      Row().height(1).backgroundColor('#3A3A3C').margin({ top: 12, bottom: 12 });
      Text('Discovered peers').fontSize(18).fontWeight(FontWeight.Bold);
      Scroll() {
        Column() {
          ForEach(this.peers, (item: PeerInfo, index: number) => {
            Column() {
              Text(item.peerId).fontWeight(FontWeight.Bold);
              ForEach(this.getAddressDisplayLines(item.addresses), (addr: string, addrIndex: number) => Text(addr), (addr: string, addrIndex: number) => `addr-${index}-${addrIndex}`);
              Row() {
                Button(item.connected ? 'Reconnect' : 'Connect')
                  .margin({ right: 8 })
                  .onClick(() => {
                    void this.connectPeer(item.peerId);
                  });
                Button('Chat')
                  .onClick(() => {
                    this.selectedPeer = item.peerId;
                    this.updateMessageList(item.peerId);
                    this.selectedTab = 1;
                    void this.node.connectPeerAsync(item.peerId);
                  });
              }.margin({ top: 6 });
            }.padding({ top: 8, bottom: 8, left: 4, right: 4 });
            if (index < this.peers.length - 1) {
              Row().height(1).backgroundColor('#3A3A3C').margin({ left: 4, right: 4 });
            }
          }, (item: PeerInfo, index: number) => `${index}-${item.peerId}`);
        };
      }.height('60%');
    };
  }

  @Builder
  private renderChat() {
    Column() {
      Row() {
        Text('Peers:').fontWeight(FontWeight.Bold);
        Scroll() {
          Row() {
            ForEach(this.peers, (item: PeerInfo) => {
              Button(item.peerId)
                .margin({ right: 6 })
                .backgroundColor(this.selectedPeer === item.peerId ? '#0A84FF' : '#1E1E1E')
                .onClick(() => {
                  this.selectedPeer = item.peerId;
                  this.updateMessageList(item.peerId);
                });
            }, (item: PeerInfo) => item.peerId);
          };
        }.width('80%').margin({ left: 8 });
      };
      Scroll() {
        Column() {
          ForEach(this.messageList, (msg: MessageItem) => {
            Column() {
              Text(msg.body)
                .textAlign(msg.fromSelf ? TextAlign.End : TextAlign.Start)
                .backgroundColor(msg.fromSelf ? '#0A84FF' : '#2C2C2E')
                .padding(8)
                .width('100%');
              Text(`${new Date(msg.timestamp).toLocaleTimeString()} · ${msg.acked ? 'Delivered' : 'Pending'}`)
                .fontSize(10)
                .textAlign(msg.fromSelf ? TextAlign.End : TextAlign.Start)
                .margin({ top: 4, bottom: 8 });
            }
          }, (msg: MessageItem) => msg.messageId + msg.timestamp);
        };
      }.height('60%').margin({ top: 8, bottom: 8 });
      TextArea({
        placeholder: 'Type message',
        text: this.chatInput
      })
        .height(80)
        .onChange(value => this.chatInput = value);
      Button('Send')
        .margin({ top: 8 })
        .enabled(!!this.selectedPeer && this.chatInput.length > 0)
        .onClick(() => this.sendMessage());
    };
  }

  @Builder
  private renderFeed() {
    Column() {
      TextArea({
        placeholder: 'Share update',
        text: this.feedInput
      })
        .height(100)
        .onChange(value => this.feedInput = value);
      Row() {
        Button(this.attachDemo ? 'Demo media: ON' : 'Attach demo media')
          .margin({ top: 8, right: 8 })
          .onClick(() => this.attachDemo = !this.attachDemo);
        Button('Publish')
          .margin({ top: 8 })
          .enabled(this.feedInput.length > 0)
          .onClick(() => this.publishFeedEntry());
      };
      Scroll() {
        Column() {
          ForEach(this.feedItems, (item: FeedItem) => {
            Column() {
              Text(`${item.author} · ${new Date(item.timestamp).toLocaleTimeString()}`)
                .fontWeight(FontWeight.Bold);
              Text(item.summary || 'No summary').margin({ top: 4, bottom: item.attachments.length > 0 ? 4 : 8 });
              if (item.attachments.length > 0) {
                ForEach(item.attachments, (att: FeedAttachment) => {
                  this.renderAttachment(att, this.getPixelMapForAttachment(att));
                }, (att: FeedAttachment) => `${item.id}-${att.id}`);
                Row().height(1).backgroundColor('#3A3A3C').margin({ top: 8 });
              } else {
                Row().height(1).backgroundColor('#3A3A3C');
              }
            }.padding(8);
          }, (item: FeedItem) => item.id);
        };
      }.margin({ top: 12 }).height('60%');
    };
  }

  @Builder
  private renderAttachment(att: FeedAttachment, pixel: image.PixelMap | null) {
    if (att.kind === 'image') {
      if (pixel) {
        Image(pixel)
          .width('100%')
          .height(180)
          .margin({ top: 6, bottom: 6 })
          .borderRadius(12);
      } else {
        Text(`[Image] ${att.label}`).margin({ top: 6, bottom: 6 });
      }
    } else if (att.kind === 'audio') {
      Text(`🔊 ${att.label}`).margin({ top: 6 });
      if (att.uri) {
        Text(att.uri).fontSize(12).fontColor('#0A84FF');
      }
    } else if (att.kind === 'video') {
      Text(`🎬 ${att.label}`).margin({ top: 6 });
      if (att.uri) {
        Text(att.uri).fontSize(12).fontColor('#0A84FF');
      }
    } else if (att.kind === 'link') {
      Text(`🔗 ${att.label}: ${att.uri ?? att.text ?? ''}`).margin({ top: 6 });
    } else if (att.kind === 'text') {
      Text(att.text ?? '').margin({ top: 6 });
    } else if (att.kind === 'data') {
      Text(`📎 ${att.label} (${att.data ? att.data.length : 0} bytes)`).margin({ top: 6 });
    } else {
      Text(`ℹ️ ${att.label}`).margin({ top: 6 });
    }
  }

  private getPixelMapForAttachment(att: FeedAttachment): image.PixelMap | null {
    if (!att.data) {
      return null;
    }
    const sanitized = this.sanitizeBase64(att.data);
    if (!sanitized) {
      return null;
    }
    const cacheKey = `${att.id}-${sanitized.length}`;
    const cached = this.imageCache.get(cacheKey);
    if (cached) {
      return cached;
    }
    try {
      const bytes = this.decodeSanitizedBase64(sanitized);
      if (bytes.length === 0) {
        return null;
      }
      const source = image.createImageSource(bytes.buffer);
      const pixelMap = source.createPixelMapSync();
      source.release();
      this.imageCache.set(cacheKey, pixelMap);
      return pixelMap;
    } catch (err) {
      return null;
    }
  }

  private sanitizeBase64(data: string): string {
    if (!data || data.indexOf(BASE64_TRIMMED_SENTINEL) >= 0) {
      return '';
    }
    let value = data.trim();
    if (value.startsWith('data:')) {
      const comma = value.indexOf(',');
      if (comma >= 0) {
        value = value.substring(comma + 1);
      }
    }
    value = value.replace(/\s+/g, '');
    for (let i = 0; i < value.length; i++) {
      if (BASE64_ALPHABET.indexOf(value.charAt(i)) < 0) {
        return '';
      }
    }
    return value;
  }

  private decodeSanitizedBase64(data: string): Uint8Array {
    const output: number[] = [];
    const length = data.length;
    for (let i = 0; i < length; i += 4) {
      const enc1 = BASE64_ALPHABET.indexOf(data.charAt(i));
      const enc2 = BASE64_ALPHABET.indexOf(data.charAt(i + 1));
      const enc3 = BASE64_ALPHABET.indexOf(data.charAt(i + 2));
      const enc4 = BASE64_ALPHABET.indexOf(data.charAt(i + 3));
      if (enc1 < 0 || enc2 < 0) {
        break;
      }
      const chr1 = (enc1 << 2) | (enc2 >> 4);
      output.push(chr1 & 0xff);
      if (enc3 >= 0 && data.charAt(i + 2) !== '=') {
        const chr2 = ((enc2 & 15) << 4) | (enc3 >> 2);
        output.push(chr2 & 0xff);
      }
      if (enc4 >= 0 && data.charAt(i + 3) !== '=') {
        const chr3 = ((enc3 & 3) << 6) | enc4;
        output.push(chr3 & 0xff);
      }
    }
    return new Uint8Array(output);
  }

  private releaseImages() {
    this.imageCache.forEach((bitmap: image.PixelMap) => {
      try {
        bitmap.release();
      } catch (err) {
        // ignore
      }
    });
    this.imageCache.clear();
  }

  @Builder
  private renderLive() {
    Column() {
      TextInput({ placeholder: 'Stream key', text: this.streamKey })
        .onChange(value => this.streamKey = value);
      TextArea({
        placeholder: 'Frame payload (text)',
        text: this.streamPayload
      })
        .height(100)
        .margin({ top: 8 })
        .onChange(value => this.streamPayload = value);
      Button('Publish frame')
        .margin({ top: 8 })
        .enabled(this.streamPayload.length > 0)
        .onClick(() => this.publishLivestream());
      Scroll() {
        Column() {
          ForEach(this.liveFrames, (frame: LiveFrame) => {
            Column() {
              Text(`${frame.streamKey} · frame ${frame.frameIndex}`)
                .fontWeight(FontWeight.Bold);
              Text(`${frame.payloadSize} bytes @ ${new Date(frame.timestamp).toLocaleTimeString()}`)
                .fontSize(12);
              Row().height(1).backgroundColor('#3A3A3C').margin({ top: 6, bottom: 6 });
            }
          }, (frame: LiveFrame) => `${frame.streamKey}-${frame.frameIndex}-${frame.timestamp}`);
        };
      }.margin({ top: 12 }).height('60%');
    };
  }

  private parseFeedEnvelope(payload: NodeEventPayload): FeedItem | null {
    if (!payload) {
      return null;
    }
    const fallbackAuthor: string = payload.peer_id ?? payload.author ?? 'unknown';
    const fallbackTimestamp: number = payload.timestamp_ms ?? payload.timestamp ?? Date.now();
    const candidate: string | FeedRecord | null = payload.payload ?? payload.content ?? null;
    const node = this.resolveFeedRecord(candidate);
    if (!node) {
      return null;
    }
    return this.parseFeedItem(node, fallbackAuthor, fallbackTimestamp);
  }

  private resolveFeedRecord(candidate: string | FeedRecord | null): FeedRecord | null {
    if (!candidate) {
      return null;
    }
    if (typeof candidate === 'string') {
      try {
        const parsed = JSON.parse(candidate) as Object;
        return this.coerceRecord(parsed);
      } catch (err) {
        return null;
      }
    }
    if (typeof candidate === 'object' && !Array.isArray(candidate)) {
      return this.coerceRecord(candidate);
    }
    return null;
  }

  private coerceRecord(value: Object | string | number | boolean | null | undefined): FeedRecord | null {
    if (!value || typeof value !== 'object') {
      return null;
    }
    if (Array.isArray(value)) {
      return null;
    }
    return value as FeedRecord;
  }

  private parseFeedItem(node: FeedRecord, fallbackAuthor: string, fallbackTimestamp: number): FeedItem | null {
    const idCandidate = this.getStringProperty(node as Object, 'id') || this.getStringProperty(node as Object, 'cid');
    if (!idCandidate) {
      return null;
    }
    const author = this.getStringProperty(node as Object, 'author') || fallbackAuthor || 'unknown';
    const timestampMs = this.getNumberProperty(node as Object, 'timestamp_ms', -1);
    const timestamp = timestampMs >= 0
      ? timestampMs
      : this.getNumberProperty(node as Object, 'timestamp', fallbackTimestamp);
    const summary = this.extractFeedSummary(node);
    const attachments = this.parseAttachments(node);
    return {
      id: idCandidate,
      author: author,
      timestamp: timestamp,
      summary: summary,
      attachments: attachments,
      raw: JSON.stringify(node)
    };
  }

  private extractFeedSummary(node: FeedRecord): string {
    const recordObject = node as Object;
    for (const key of FEED_SUMMARY_KEYS) {
      const value = this.readProperty(recordObject, key);
      if (typeof value === 'string' && value.length > 0) {
        return value;
      }
    }
    for (const key of FEED_ARRAY_KEYS) {
      const value = this.readProperty(recordObject, key);
      const description = this.describeAttachmentValue(value);
      if (description.length > 0) {
        return description;
      }
    }
    const fallback = this.getStringProperty(recordObject, 'id') || this.getStringProperty(recordObject, 'cid');
    return fallback || 'Feed item';
  }

  private describeAttachmentValue(value: Object | string | number | boolean | null | undefined): string {
    if (!value) {
      return '';
    }
    if (Array.isArray(value)) {
      const arrayValue = value as Array<Object | string | number | boolean | null | undefined>;
      const snippets: string[] = [];
      arrayValue.forEach(entry => {
        const text = this.describeAttachmentEntry(entry);
        if (text.length > 0 && snippets.length < 3) {
          snippets.push(text);
        }
      });
      return snippets.join(' • ');
    }
    return this.describeAttachmentEntry(value);
  }

  private describeAttachmentEntry(entry: Object | string | number | boolean | null | undefined): string {
    if (typeof entry === 'string') {
      return entry;
    }
    if (typeof entry === 'number' || typeof entry === 'boolean') {
      return `${entry}`;
    }
    const node = this.coerceAttachmentNode(entry);
    if (!node) {
      return '';
    }
    const label = node.label ?? node.title ?? node.name ?? node.type ?? node.kind ?? 'media';
    const text = node.text ?? node.caption ?? node.body ?? '';
    return text.length > 0 ? `${label}: ${text}` : label;
  }

  private parseAttachments(node: FeedRecord): FeedAttachment[] {
    const attachments: FeedAttachment[] = [];
    const seen: Set<string> = new Set();
    const recordObject = node as Object;
    FEED_ARRAY_KEYS.forEach(key => {
      const value = this.readProperty(recordObject, key);
      this.appendAttachmentFromValue(value, key, attachments, seen);
    });
    FEED_IMAGE_KEYS.forEach(key => {
      const value = this.readProperty(recordObject, key);
      this.appendAttachmentFromValue(value, key, attachments, seen);
    });
    return attachments;
  }

  private appendAttachmentFromValue(
    value: Object | string | number | boolean | null | undefined,
    key: string,
    attachments: FeedAttachment[],
    seen: Set<string>
  ) {
    if (!value) {
      return;
    }
    if (Array.isArray(value)) {
      const arrayValue = value as Array<Object | string | number | boolean | null | undefined>;
      arrayValue.forEach((entry, index) => {
        this.appendAttachmentEntry(entry, `${key}-${index}`, attachments, seen);
      });
      return;
    }
    this.appendAttachmentEntry(value, key, attachments, seen);
  }

  private appendAttachmentEntry(
    entry: Object | string | number | boolean | null | undefined,
    fallbackId: string,
    attachments: FeedAttachment[],
    seen: Set<string>
  ) {
    let attachment: FeedAttachment | null = null;
    if (typeof entry === 'string') {
      attachment = this.parseAttachmentPrimitive(fallbackId, entry);
    } else if (typeof entry === 'number' || typeof entry === 'boolean') {
      attachment = this.parseAttachmentPrimitive(fallbackId, `${entry}`);
    } else {
      const node = this.coerceAttachmentNode(entry);
      if (node) {
        attachment = this.parseAttachmentObject(node, fallbackId);
      }
    }
    if (!attachment) {
      return;
    }
    const cacheKey = `${attachment.kind}-${attachment.label}-${attachment.uri ?? ''}-${attachment.data?.length ?? 0}`;
    if (!seen.has(cacheKey)) {
      attachments.push(attachment);
      seen.add(cacheKey);
    }
  }

  private parseAttachmentObject(obj: FeedAttachmentNode, fallbackId: string): FeedAttachment | null {
    const typeValue: string = obj.type ?? obj.kind ?? obj.mediaType ?? '';
    const mime: string = obj.mime ?? obj.mediaType ?? obj.contentType ?? '';
    const label: string = obj.label ?? obj.title ?? obj.name ?? typeValue;
    const id: string = obj.id ?? fallbackId;
    const text: string = obj.text ?? obj.body ?? obj.caption ?? '';
    let uri: string | undefined;
    const uriKeys: string[] = ['url', 'uri', 'href', 'src'];
    for (const key of uriKeys) {
      const raw = this.readProperty(obj as Object, key);
      if (typeof raw === 'string' && raw.length > 0) {
        uri = raw;
        break;
      }
    }
    let data = '';
    const dataKeys: string[] = ['data', 'base64', 'payload', 'blob', 'bytes', 'value'];
    for (const key of dataKeys) {
      const rawData = this.readProperty(obj as Object, key);
      if (typeof rawData === 'string' && rawData.length > 0) {
        data = rawData;
        break;
      }
    }
    if (!data && text.startsWith('data:')) {
      data = text;
    }
    const sanitized = this.sanitizeBase64(data);
    const kind = this.determineAttachmentKind(typeValue, mime, uri, text, sanitized);
    if (kind === 'text' && !text) {
      return null;
    }
    return {
      id: id,
      kind: kind,
      label: label || kind.toUpperCase(),
      uri: uri,
      mime: mime || undefined,
      text: text || undefined,
      data: sanitized || undefined
    };
  }

  private coerceAttachmentNode(value: Object | string | number | boolean | null | undefined): FeedAttachmentNode | null {
    if (!value || typeof value !== 'object') {
      return null;
    }
    if (Array.isArray(value)) {
      return null;
    }
    return value as FeedAttachmentNode;
  }

  private readProperty(source: Object | null, key: string): Object | string | number | boolean | null | undefined {
    if (!source) {
      return undefined;
    }
    return Reflect.get(source as object, key) as Object | string | number | boolean | null | undefined;
  }

  private getStringProperty(source: Object | null, key: string): string {
    const raw = this.readProperty(source, key);
    if (typeof raw === 'string') {
      return raw;
    }
    if (typeof raw === 'number' || typeof raw === 'boolean') {
      return `${raw}`;
    }
    return '';
  }

  private getNumberProperty(source: Object | null, key: string, fallback: number): number {
    const raw = this.readProperty(source, key);
    if (typeof raw === 'number') {
      return raw;
    }
    if (typeof raw === 'string') {
      const parsed = Number(raw);
      return Number.isNaN(parsed) ? fallback : parsed;
    }
    return fallback;
  }

  private parseAttachmentPrimitive(label: string, value: string): FeedAttachment | null {
    if (!value) {
      return null;
    }
    const sanitized = this.sanitizeBase64(value);
    if (sanitized) {
      return {
        id: label,
        kind: 'image',
        label: label,
        data: sanitized
      };
    }
    return {
      id: label,
      kind: value.startsWith('http') ? 'link' : 'text',
      label: label,
      uri: value.startsWith('http') ? value : undefined,
      text: value.startsWith('http') ? undefined : value
    };
  }

  private determineAttachmentKind(
    typeValue: string,
    mime: string,
    uri?: string,
    text?: string,
    data?: string
  ): FeedAttachmentKind {
    const lowerType = typeValue.toLowerCase();
    const lowerMime = mime.toLowerCase();
    if (data && data.length > 0 && (lowerMime.startsWith('image') || lowerType.indexOf('image') >= 0)) {
      return 'image';
    }
    if (lowerType.indexOf('image') >= 0 || lowerMime.startsWith('image')) {
      return 'image';
    }
    if (lowerType.indexOf('audio') >= 0 || lowerMime.startsWith('audio')) {
      return 'audio';
    }
    if (lowerType.indexOf('video') >= 0 || lowerMime.startsWith('video')) {
      return 'video';
    }
    if (uri && uri.length > 0) {
      return 'link';
    }
    if (text && text.length > 0) {
      return 'text';
    }
    if (data && data.length > 0) {
      return 'data';
    }
    return 'unknown';
  }
}

import hilog from '@ohos.hilog';
interface NimBridgeStorage {
  Get(key: string): string[] | undefined;
}

declare const AppStorage: NimBridgeStorage;

const MODULE = 'libnimbridge.so';
const MODULE_NAME = 'com.example.unimaker.nimbridge';
const STAGE_CANDIDATES = [
  'entry/libnimbridge',
  'entry/libnimbridge.so',
  'entry/libs/arm64-v8a/libnimbridge.so',
  'entry/libs/arm64/libnimbridge.so',
  'entry/lib/libnimbridge',
  'entry/lib/libnimbridge.so',
  'libs/arm64-v8a/libnimbridge.so',
  'libs/arm64/libnimbridge.so',
  'entry/libp2pbridge',
  'entry/libp2pbridge.so',
  'entry/libs/arm64-v8a/libp2pbridge.so',
  'entry/libs/arm64/libp2pbridge.so',
  'entry/lib/libp2pbridge',
  'entry/lib/libp2pbridge.so',
  'libs/arm64-v8a/libp2pbridge.so',
  'libs/arm64/libp2pbridge.so',
  'libs/arm64/libp2pbridge.so'
] as const;
const REQUIRED_EXPORTS: Array<keyof NimBridgeModule> = [
  'initNode',
  'startNode',
  'pollEvents',
  'lanEndpoints',
  'localPeerId',
  'setMdnsInterface',
  'listenAddresses',
  'getDialableAddresses',
  'sendDirect',
  'connectPeer',
  'connectMultiaddr',
  'registerPeerHints',
  'addExternalAddress',
  'isPeerConnected'
];

interface NimBridgeModule {
  initNode(config: string): number;
  startNode(handle: number): number;
  initializeConnEvents(handle: number): boolean;
  mdnsProbe(handle: number): boolean;
  stopNode(handle: number): number;
  freeNode(handle: number): void;
  pollEvents(handle: number, limit: number): string | undefined;
  lanEndpoints(handle: number): string | undefined;
  lanEndpointsAsync?(handle: number): Promise<string | undefined>;
  localPeerId(handle: number): string | undefined;
  setMdnsInterface(handle: number, ipv4: string): boolean;
  listenAddresses(handle: number): string | undefined;
  getDialableAddresses(handle: number): string | undefined;
  sendDirect(handle: number, peerId: string, payload: string): boolean;
  sendDirectAsync?(handle: number, peerId: string, payload: string): Promise<boolean>;
  connectPeer(handle: number, peerId: string): number;
  connectPeerAsync?(handle: number, peerId: string): Promise<number>;
  connectMultiaddr(handle: number, multiaddr: string): boolean;
  connectMultiaddrAsync?(handle: number, multiaddr: string): Promise<boolean>;
  publishFeed(handle: number, payload: string): boolean;
  fetchFeedSnapshot(handle: number): string | undefined;
  upsertLivestream(handle: number, streamKey: string, configJson: string): boolean;
  publishLivestreamFrame(handle: number, streamKey: string, payload: string): boolean;
  lastDirectError(handle: number): string | undefined;
  lastInitError(): string | undefined;
  generateIdentity?(): string | undefined;
  identityFromSeed?(seed: string): string | undefined;
  registerPeerHints(handle: number, peerId: string, addressesJson: string, source?: string): boolean;
  addExternalAddress(handle: number, multiaddr: string): boolean;
  isPeerConnected(handle: number, peerId: string): boolean;
}

interface NapiRuntime {
  requireNapiModule?(name: string, isZlib?: boolean): NimBridgeModule;
  requireNapi?(name: string, isZlib?: boolean): NimBridgeModule;
  requireNative?(name: string, isZlib?: boolean): NimBridgeModule;
}

declare function requireNativeModule(name: string, isZlib?: boolean): NimBridgeModule;
declare function loadNativeModule(moduleName: string, isZlib?: boolean): NimBridgeModule;

let nativeBridge: NimBridgeModule | null = null;
let runtimeLogged: boolean = false;

function isBridgeModule(candidate: NimBridgeModule | null): candidate is NimBridgeModule {
  if (!candidate) {
    return false;
  }
  const value: Record<string, unknown> = candidate as unknown as Record<string, unknown>;
  return REQUIRED_EXPORTS.every((key: keyof NimBridgeModule) => typeof value[key] === 'function');
}

function loadNative(): NimBridgeModule {
  if (nativeBridge) {
    return nativeBridge;
  }
  const runtime = globalThis as unknown as NapiRuntime;
  const errors: string[] = [];
  if (!runtimeLogged) {
    const availableLoaders: string[] = [];
    if (runtime.requireNapiModule) {
      availableLoaders.push('requireNapiModule');
    }
    if (runtime.requireNapi) {
      availableLoaders.push('requireNapi');
    }
    if (runtime.requireNative) {
      availableLoaders.push('requireNative');
    }
    hilog.info(0xD0B0, 'nimlib', 'Runtime NAPI loaders available: %{public}s', JSON.stringify(availableLoaders));
    try {
      const globalKeys = Object.getOwnPropertyNames(globalThis as object).filter((key: string) => key.indexOf('require') >= 0 || key.indexOf('load') >= 0);
      hilog.info(0xD0B0, 'nimlib', 'Global loader-related keys: %{public}s', JSON.stringify(globalKeys));
    } catch (_) {
      // ignore reflection failures
    }
    runtimeLogged = true;
  }

  const attempt = (label: string, loader: () => NimBridgeModule): boolean => {
    try {
      const module = loader();
      if (module && isBridgeModule(module)) {
        nativeBridge = module;
        const keys = Object.keys(nativeBridge as object);
        hilog.info(0xD0B0, 'nimlib', 'Loaded nimbridge via %{public}s keys=%{public}s', label, JSON.stringify(keys));
        return true;
      }
      if (module) {
        const keys = Object.keys(module as object);
        errors.push(`${label}: missing exports keys=${JSON.stringify(keys)}`);
        hilog.warn(0xD0B0, 'nimlib', 'nimbridge module missing exports (%{public}s): %{public}s', label, JSON.stringify(keys));
      }
    } catch (err) {
      let message = 'unknown error';
      if (err) {
        if (typeof err === 'object') {
          try {
            message = JSON.stringify(err);
          } catch (_) {
            message = `${err}`;
          }
        } else {
          message = `${err}`;
        }
      }
      errors.push(`${label}: ${message}`);
      hilog.warn(0xD0B0, 'nimlib', 'nimbridge load failed (%{public}s): %{public}s', label, message);
    }
    return false;
  };

  const primaryCandidates: string[] = [
    MODULE_NAME,
    'nimbridge',
    'libnimbridge',
    'libnimbridge.so',
    'libp2pbridge',
    'libp2pbridge.so',
    'libs/arm64-v8a/libnimbridge.so',
    'libs/arm64/libnimbridge.so',
    'libs/arm64-v8a/libp2pbridge.so',
    'libs/arm64/libp2pbridge.so',
    MODULE,
    ...STAGE_CANDIDATES
  ];

  for (const name of primaryCandidates) {
    if (runtime.requireNapiModule) {
      if (attempt(`requireNapiModule(${name})`, () => runtime.requireNapiModule!(name))) {
        return nativeBridge!;
      }
    }
    if (runtime.requireNapi) {
      if (attempt(`requireNapi(${name})`, () => runtime.requireNapi!(name))) {
        return nativeBridge!;
      }
    }
    if (runtime.requireNative) {
      if (attempt(`requireNative(${name})`, () => runtime.requireNative!(name))) {
        return nativeBridge!;
      }
    }
    if (attempt(`requireNativeModule(${name})`, () => requireNativeModule(name))) {
      return nativeBridge!;
    }
    if (attempt(`loadNativeModule(${name})`, () => loadNativeModule(name) as NimBridgeModule)) {
      return nativeBridge!;
    }
  }

  const searchPaths: string[] = [];
  try {
    const stored = AppStorage.Get('__nimBridgeSearchPaths');
    if (Array.isArray(stored)) {
      searchPaths.push(...stored.filter(path => typeof path === 'string'));
    }
  } catch (_) {
    // ignore
  }
  if (searchPaths.length === 0) {
    searchPaths.push('/data/storage/el1/bundle/libs/arm64-v8a');
    searchPaths.push('/data/storage/el1/bundle/libs/arm64');
    searchPaths.push('/data/storage/el2/base/haps/entry/files/../lib');
  }
  const extraCandidates: string[] = [];
  if (searchPaths.length > 0) {
    searchPaths.forEach(root => {
      if (root && root.length > 0) {
        extraCandidates.push(`${root}/${MODULE}`);
        extraCandidates.push(`${root}/libnimbridge.so`);
        extraCandidates.push(`${root}/lib/libnimbridge.so`);
        extraCandidates.push(`${root}/libs/arm64-v8a/libnimbridge.so`);
        extraCandidates.push(`${root}/libp2pbridge.so`);
        extraCandidates.push(`${root}/lib/libp2pbridge.so`);
        extraCandidates.push(`${root}/libs/arm64-v8a/libp2pbridge.so`);
        extraCandidates.push(`${root}/${MODULE_NAME}`);
      }
    });
  }
  extraCandidates.push(MODULE);
  extraCandidates.push(MODULE_NAME);
  extraCandidates.push('libnimbridge.so');
  extraCandidates.push('lib/libnimbridge.so');
  extraCandidates.push('libs/arm64-v8a/libnimbridge.so');
  extraCandidates.push('libs/arm64/libnimbridge.so');
  extraCandidates.push('libp2pbridge.so');
  extraCandidates.push('lib/libp2pbridge.so');
  extraCandidates.push('libs/arm64-v8a/libp2pbridge.so');
  extraCandidates.push('libs/arm64/libp2pbridge.so');

  for (const candidate of extraCandidates) {
    if (runtime.requireNapiModule) {
      if (attempt(`requireNapiModule(${candidate})`, () => runtime.requireNapiModule!(candidate))) {
        return nativeBridge!;
      }
    }
    if (runtime.requireNapi && attempt(`requireNapi(${candidate})`, () => runtime.requireNapi!(candidate))) {
      return nativeBridge!;
    }
    if (runtime.requireNative && attempt(`requireNative(${candidate})`, () => runtime.requireNative!(candidate))) {
      return nativeBridge!;
    }
    if (attempt(`requireNativeModule(${candidate})`, () => requireNativeModule(candidate))) {
      return nativeBridge!;
    }
    if (attempt(`loadNativeModule(${candidate})`, () => loadNativeModule(candidate) as NimBridgeModule)) {
      return nativeBridge!;
    }
  }

  const summary = errors.length > 0 ? errors.join(' | ') : 'all loaders failed without explicit error';
  hilog.error(0xD0B0, 'nimlib', 'Unable to load nimbridge module: %{public}s', summary);
  throw new Error(`nimbridge load failed: ${summary}`);
}

function requireNapi(name: string): NimBridgeModule {
  const runtime: NapiRuntime = globalThis as NapiRuntime;
  if (runtime.requireNapi) {
    return runtime.requireNapi(name);
  }
  if (runtime.requireNative) {
    return runtime.requireNative(name);
  }
  return requireNativeModule(name);
}

function readStorageArray(key: string): string[] {
  try {
    const stored = AppStorage.Get(key);
    if (!stored) {
      return [];
    }
    const result: string[] = [];
    stored.forEach((value: string) => {
      if (typeof value === 'string' && value.length > 0) {
        result.push(value);
      }
    });
    return result;
  } catch (_) {
    return [];
  }
}

export class NimNodeHandle {
  constructor(public handle: number) {}
}

export class NimLibp2p {
  private node: NimNodeHandle | null = null;
  private directAsyncSupported: boolean | null = null;
  private loggedDirectAsyncFallback: boolean = false;

  private ensureDirectAsyncSupport(lib?: NimBridgeModule): boolean {
    if (this.directAsyncSupported !== null) {
      return this.directAsyncSupported;
    }
    const bridge = lib ?? loadNative();
    const supported = typeof bridge.sendDirectAsync === 'function';
    this.directAsyncSupported = supported;
    if (supported) {
      hilog.info(0xD0B0, 'nimlib', 'Native bridge provides sendDirectAsync');
    } else {
      hilog.warn(
        0xD0B0,
        'nimlib',
        'sendDirectAsync unavailable; falling back to synchronous sendDirect (acks disabled)'
      );
    }
    return supported;
  }

  identityFromSeed(seed: string): string | undefined {
    const lib = loadNative();
    if (typeof lib.identityFromSeed !== 'function') {
      hilog.warn(0xD0B0, 'nimlib', 'identityFromSeed not available on native bridge');
      return undefined;
    }
    return lib.identityFromSeed(seed);
  }

  generateIdentity(): string | undefined {
    const lib = loadNative();
    if (typeof lib.generateIdentity !== 'function') {
      hilog.warn(0xD0B0, 'nimlib', 'generateIdentity not available on native bridge');
      return undefined;
    }
    return lib.generateIdentity();
  }

  start(config: string): boolean {
    const lib = loadNative();
    hilog.info(0xD0B0, 'nimlib', 'Starting with config %{public}s', config);
    const initHandle: number = lib.initNode(config);
    if (!initHandle) {
      const reason = lib.lastInitError ? lib.lastInitError() ?? '' : '';
      hilog.error(0xD0B0, 'nimlib', 'initNode failed, reason=%{public}s', reason);
      return false;
    }
    this.node = new NimNodeHandle(initHandle);
    const ipv4Candidates = readStorageArray('__nimActiveIPv4');
    if (ipv4Candidates.length > 0) {
      const preferred = ipv4Candidates.find(value => value !== '0.0.0.0') ?? ipv4Candidates[0];
      if (preferred && preferred.length > 0) {
        const setResult = lib.setMdnsInterface(initHandle, preferred);
        hilog.info(
          0xD0B0,
          'nimlib',
          'setMdnsInterface ipv4=%{public}s result=%{public}s',
          preferred,
          String(setResult)
        );
      }
    }
    const startResult = lib.startNode(initHandle);
    if (startResult !== 0) {
      const reason = lib.lastInitError ? lib.lastInitError() ?? '' : '';
      hilog.error(0xD0B0, 'nimlib', 'startNode returned %{public}d reason=%{public}s', startResult, reason);
      lib.freeNode(initHandle);
      this.node = null;
      return false;
    }
    const connResult = lib.initializeConnEvents(initHandle);
    if (!connResult) {
      hilog.warn(0xD0B0, 'nimlib', 'initializeConnEvents returned false');
    }
    const mdnsOk = lib.mdnsProbe(initHandle);
    if (!mdnsOk) {
      hilog.warn(0xD0B0, 'nimlib', 'mdnsProbe returned false');
    }
    hilog.info(0xD0B0, 'nimlib', 'Nim node started successfully handle=%{public}d', initHandle);
    return true;
  }

  get handle(): number {
    return this.node ? this.node.handle : 0;
  }

  stop(): void {
    if (!this.node) {
      return;
    }
    const lib = loadNative();
    lib.stopNode(this.node.handle);
    lib.freeNode(this.node.handle);
    this.node = null;
  }

  setMdnsInterface(ipv4: string): boolean {
    if (!this.node || !ipv4 || ipv4.length === 0) {
      return false;
    }
    const lib = loadNative();
    return lib.setMdnsInterface(this.node.handle, ipv4);
  }

  poll(limit: number = 128): string {
    const lib = loadNative();
    return this.node ? lib.pollEvents(this.node.handle, limit) ?? '[]' : '[]';
  }

  lanEndpoints(): string {
    const lib = loadNative();
    return this.node ? lib.lanEndpoints(this.node.handle) ?? '{}' : '{}';
  }

  async lanEndpointsAsync(): Promise<string> {
    if (!this.node) {
      return '{}';
    }
    const lib = loadNative();
    const handle = this.node.handle;
    if (typeof lib.lanEndpointsAsync === 'function') {
      try {
        const result = await lib.lanEndpointsAsync(handle);
        return result ?? '{}';
      } catch (err) {
        const message = err ? `${err}` : 'unknown error';
        hilog.warn(0xD0B0, 'nimlib', 'lanEndpointsAsync failed: %{public}s', message);
      }
    }
    const fallback = lib.lanEndpoints(handle);
    return fallback ?? '{}';
  }

  listenAddresses(): string {
    const lib = loadNative();
    return this.node ? lib.listenAddresses(this.node.handle) ?? '[]' : '[]';
  }

  dialableAddresses(): string {
    const lib = loadNative();
    return this.node ? lib.getDialableAddresses(this.node.handle) ?? '[]' : '[]';
  }

  async sendDirect(peerId: string, payload: string): Promise<boolean> {
    if (!this.node) {
      return false;
    }
    const lib = loadNative();
    const supportsAsync = this.ensureDirectAsyncSupport(lib);
    if (supportsAsync && typeof lib.sendDirectAsync === 'function') {
      try {
        const result = await lib.sendDirectAsync(this.node.handle, peerId, payload);
        return !!result;
      } catch (err) {
        const message = err ? `${err}` : 'unknown error';
        hilog.warn(0xD0B0, 'nimlib', 'sendDirectAsync failed: %{public}s', message);
        return false;
      }
    }
    if (!this.loggedDirectAsyncFallback) {
      this.loggedDirectAsyncFallback = true;
      hilog.warn(0xD0B0, 'nimlib', 'sendDirectAsync not supported; using blocking sendDirect');
    }
    return lib.sendDirect(this.node.handle, peerId, payload);
  }

  connectPeer(peerId: string): void {
    if (!this.node) {
      return;
    }
    const lib = loadNative();
    lib.connectPeer(this.node.handle, peerId);
  }

  connectMultiaddr(multiaddr: string): boolean {
    if (!this.node || !multiaddr || multiaddr.length === 0) {
      return false;
    }
    const lib = loadNative();
    return lib.connectMultiaddr(this.node.handle, multiaddr);
  }

  async connectPeerAsync(peerId: string): Promise<number> {
    if (!this.node || !peerId || peerId.length === 0) {
      return -1;
    }
    const lib = loadNative();
    if (typeof lib.connectPeerAsync === 'function') {
      try {
        const result = await lib.connectPeerAsync(this.node.handle, peerId);
        return typeof result === 'number' ? result : -1;
      } catch (err) {
        const message = err ? `${err}` : 'unknown error';
        hilog.warn(0xD0B0, 'nimlib', 'connectPeerAsync bridge failed: %{public}s', message);
        return -1;
      }
    }
    return lib.connectPeer(this.node.handle, peerId);
  }

  async connectMultiaddrAsync(multiaddr: string): Promise<boolean> {
    if (!this.node || !multiaddr || multiaddr.length === 0) {
      return false;
    }
    const lib = loadNative();
    if (typeof lib.connectMultiaddrAsync === 'function') {
      try {
        const result = await lib.connectMultiaddrAsync(this.node.handle, multiaddr);
        return !!result;
      } catch (err) {
        const message = err ? `${err}` : 'unknown error';
        hilog.warn(0xD0B0, 'nimlib', 'connectMultiaddrAsync bridge failed: %{public}s', message);
        return false;
      }
    }
    return lib.connectMultiaddr(this.node.handle, multiaddr);
  }

  registerPeerHints(peerId: string, addresses: string[], source: string = ''): boolean {
    if (!this.node || !peerId || peerId.length === 0 || !Array.isArray(addresses) || addresses.length === 0) {
      return false;
    }
    const lib = loadNative();
    const payload = JSON.stringify(addresses);
    return lib.registerPeerHints(this.node.handle, peerId, payload, source);
  }

  addExternalAddress(multiaddr: string): boolean {
    if (!this.node || !multiaddr || multiaddr.length === 0) {
      return false;
    }
    const lib = loadNative();
    return lib.addExternalAddress(this.node.handle, multiaddr);
  }

  isPeerConnected(peerId: string): boolean {
    if (!this.node || !peerId || peerId.length === 0) {
      return false;
    }
    const lib = loadNative();
    return lib.isPeerConnected(this.node.handle, peerId);
  }

  publishFeed(payload: string): boolean {
    if (!this.node) {
      return false;
    }
    const lib = loadNative();
    return lib.publishFeed(this.node.handle, payload);
  }

  fetchFeed(): string {
    if (!this.node) {
      return '{}';
    }
    const lib = loadNative();
    return lib.fetchFeedSnapshot(this.node.handle) ?? '{}';
  }

  publishLivestream(streamKey: string, payload: string): boolean {
    if (!this.node) {
      return false;
    }
    const lib = loadNative();
    const meta = JSON.stringify({ description: 'demo', created_ms: Date.now() });
    lib.upsertLivestream(this.node.handle, streamKey, meta);
    return lib.publishLivestreamFrame(this.node.handle, streamKey, payload);
  }

  lastDirectError(): string {
    if (!this.node) {
      return '';
    }
    const lib = loadNative();
    return lib.lastDirectError(this.node.handle) ?? '';
  }

  supportsDirectAck(): boolean {
    if (!this.node) {
      return false;
    }
    return this.ensureDirectAsyncSupport();
  }

  lastInitError(): string {
    const lib = loadNative();
    return lib.lastInitError() ?? '';
  }
}

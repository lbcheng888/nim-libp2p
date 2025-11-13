import { AbilityConstant, ConfigurationConstant, UIAbility, Want } from '@kit.AbilityKit';
import { hilog } from '@kit.PerformanceAnalysisKit';
import { window } from '@kit.ArkUI';
import abilityAccessCtrl, { Permissions } from '@ohos.abilityAccessCtrl';
import connection from '@ohos.net.connection';
import { BusinessError } from '@ohos.base';

interface NimBridgeStorage {
  SetOrCreate(key: string, value: string[]): void;
}

declare const AppStorage: NimBridgeStorage;

type ErrorLike = BusinessError | Error | string | number | boolean | object | null | undefined;

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

const DOMAIN = 0x0000;
const PERMISSIONS: Array<Permissions> = [
  'ohos.permission.INTERNET',
  'ohos.permission.GET_NETWORK_INFO',
  'ohos.permission.LOCATION'
];

interface PermissionResultLite {
  authResults?: Array<number>;
}

export default class EntryAbility extends UIAbility {
  private netWatcher: connection.NetConnection | null = null;
  private readonly netWatcherCallback: (error?: BusinessError) => void = (error?: BusinessError) => {
    if (error) {
      const message = this.describeError(error);
      hilog.error(DOMAIN, 'testTag', 'netWatcher register callback error %{public}s', message);
    } else {
      hilog.info(DOMAIN, 'testTag', '%{public}s', 'netWatcher registered');
    }
  };

  onCreate(want: Want, launchParam: AbilityConstant.LaunchParam): void {
    try {
      this.context.getApplicationContext().setColorMode(ConfigurationConstant.ColorMode.COLOR_MODE_NOT_SET);
    } catch (err) {
      hilog.error(DOMAIN, 'testTag', 'Failed to set colorMode. Cause: %{public}s', JSON.stringify(err));
    }
    hilog.info(DOMAIN, 'testTag', '%{public}s', 'Ability onCreate');

    const codeDir = this.context.bundleCodeDir;
    const libRoot = `${codeDir}/libs`;
    const searchPaths: string[] = [
      `${libRoot}/arm64-v8a`,
      `${libRoot}/arm64`,
      `${libRoot}/arm64/lib`,
      `${libRoot}/module/arm64-v8a`,
      `${libRoot}/module/lib64`,
      `${this.context.filesDir}/../lib`
    ];
    AppStorage.SetOrCreate('__nimBridgeSearchPaths', searchPaths);
    hilog.info(DOMAIN, 'testTag', 'Bridge search paths: %{public}s', JSON.stringify(searchPaths));

    this.ensurePermissions()
      .then((granted: boolean) => {
        hilog.info(DOMAIN, 'testTag', 'Permission request result %{public}s', JSON.stringify(granted));
        if (granted) {
          this.bindDefaultNetwork().catch((error: ErrorLike) => {
            const message = this.describeError(error);
            hilog.error(DOMAIN, 'testTag', 'bindDefaultNetwork failed %{public}s', message);
          });
          this.watchWifiNetwork();
        }
      })
      .catch((error: BusinessError) => {
        const message = this.describeError(error);
        hilog.error(DOMAIN, 'testTag', 'ensurePermissions threw: %{public}s', message);
      });
  }

  onDestroy(): void {
    hilog.info(DOMAIN, 'testTag', '%{public}s', 'Ability onDestroy');
    this.teardownNetworkWatcher();
  }

  onWindowStageCreate(windowStage: window.WindowStage): void {
    hilog.info(DOMAIN, 'testTag', '%{public}s', 'Ability onWindowStageCreate');

    windowStage.loadContent('pages/Index', (err) => {
      if (err.code) {
        hilog.error(DOMAIN, 'testTag', 'Failed to load the content. Cause: %{public}s', JSON.stringify(err));
        return;
      }
      hilog.info(DOMAIN, 'testTag', 'Succeeded in loading the content.');
    });
  }

  onWindowStageDestroy(): void {
    hilog.info(DOMAIN, 'testTag', '%{public}s', 'Ability onWindowStageDestroy');
  }

  onForeground(): void {
    hilog.info(DOMAIN, 'testTag', '%{public}s', 'Ability onForeground');
  }

  onBackground(): void {
    hilog.info(DOMAIN, 'testTag', '%{public}s', 'Ability onBackground');
  }

  private async ensurePermissions(): Promise<boolean> {
    const tokenId = this.context.abilityInfo?.applicationInfo?.accessTokenId;
    if (tokenId === undefined) {
      hilog.warn(DOMAIN, 'testTag', '%{public}s', 'accessTokenId unavailable');
      return false;
    }

    const atManager = abilityAccessCtrl.createAtManager();
    const missing: Array<Permissions> = [];

    for (const permissionName of PERMISSIONS) {
      const status = await atManager
        .checkAccessToken(tokenId, permissionName)
        .catch((error: BusinessError) => {
          const message = this.describeError(error);
          hilog.warn(DOMAIN, 'testTag', 'checkAccessToken failed %{public}s %{public}s', permissionName, message);
          return abilityAccessCtrl.GrantStatus.PERMISSION_DENIED;
        });
      if (status !== undefined && status !== abilityAccessCtrl.GrantStatus.PERMISSION_GRANTED) {
        missing.push(permissionName);
      }
    }

    if (missing.length === 0) {
      return true;
    }

    return atManager
      .requestPermissionsFromUser(this.context, missing)
      .then((result: PermissionResultLite) => {
        const authResults: Array<number> = result.authResults ?? [];
        const denied = authResults.some(value => value !== abilityAccessCtrl.GrantStatus.PERMISSION_GRANTED);
        if (denied) {
          hilog.warn(DOMAIN, 'testTag', 'requestPermissionsFromUser denied %{public}s', JSON.stringify(result));
          return false;
        }
        return true;
      })
      .catch((error: BusinessError) => {
        const message = this.describeError(error);
        hilog.error(DOMAIN, 'testTag', 'requestPermissionsFromUser error %{public}s', message);
        return false;
      });
  }

  private async bindDefaultNetwork(): Promise<void> {
    try {
      const handle = await this.queryDefaultNetwork();
      if (!handle || handle.netId === 0) {
        hilog.warn(DOMAIN, 'testTag', '%{public}s', 'No active default network handle available');
        return;
      }
      await connection.setAppNet(handle);
      hilog.info(DOMAIN, 'testTag', 'setAppNet succeeded netId=%{public}d', handle.netId);
      this.cacheNetHandle(handle);
      await this.cacheNetProperties(handle);
    } catch (error) {
      const message = this.describeError(error);
      hilog.error(DOMAIN, 'testTag', 'bindDefaultNetwork error %{public}s', message);
    }
  }

  private async queryDefaultNetwork(): Promise<connection.NetHandle | null> {
    try {
      const handle = await connection.getDefaultNet();
      if (handle && handle.netId !== undefined && handle.netId !== 0) {
        return handle;
      }
    } catch (error) {
      const message = this.describeError(error);
      hilog.warn(DOMAIN, 'testTag', 'getDefaultNet error %{public}s', message);
    }
    try {
      const nets = await connection.getAllNets();
      for (const handle of nets) {
        if (handle && handle.netId !== undefined && handle.netId !== 0) {
          return handle;
        }
      }
    } catch (error) {
      const message = this.describeError(error);
      hilog.warn(DOMAIN, 'testTag', 'getAllNets error %{public}s', message);
    }
    return null;
  }

  private watchWifiNetwork(): void {
    if (this.netWatcher !== null) {
      return;
    }
    try {
      const spec: connection.NetSpecifier = {
        netCapabilities: {
          bearerTypes: [
            connection.NetBearType.BEARER_WIFI,
            connection.NetBearType.BEARER_ETHERNET,
            connection.NetBearType.BEARER_CELLULAR
          ],
          networkCap: [
            connection.NetCap.NET_CAPABILITY_INTERNET,
            connection.NetCap.NET_CAPABILITY_NOT_VPN
          ]
        },
        bearerPrivateIdentifier: 'wifi'
      };
      const watcher = connection.createNetConnection(spec, 0);
      watcher.on('netAvailable', (handle: connection.NetHandle) => {
        hilog.info(DOMAIN, 'testTag', 'netAvailable netId=%{public}d', handle.netId);
        connection.setAppNet(handle)
          .then(() => hilog.info(DOMAIN, 'testTag', 'setAppNet (watcher) ok netId=%{public}d', handle.netId))
          .catch((error: ErrorLike) => {
            const message = this.describeError(error);
            hilog.error(DOMAIN, 'testTag', 'setAppNet (watcher) failed %{public}s', message);
          });
        this.cacheNetHandle(handle);
        this.cacheNetProperties(handle).catch((error: ErrorLike) => {
          const message = this.describeError(error);
          hilog.error(DOMAIN, 'testTag', 'cacheNetProperties failed %{public}s', message);
        });
      });
      watcher.on('netLost', (handle: connection.NetHandle) => {
        hilog.warn(DOMAIN, 'testTag', 'netLost netId=%{public}d', handle.netId);
      });
      watcher.on('netUnavailable', () => {
        hilog.warn(DOMAIN, 'testTag', '%{public}s', 'netUnavailable triggered');
      });
      watcher.register(this.netWatcherCallback);
      this.netWatcher = watcher;
    } catch (error) {
      const message = this.describeError(error);
      hilog.error(DOMAIN, 'testTag', 'watchWifiNetwork error %{public}s', message);
    }
  }

  private cacheNetHandle(handle: connection.NetHandle): void {
    try {
      AppStorage.SetOrCreate('__nimActiveNetId', [String(handle.netId)]);
    } catch (_) {
      // ignore storage errors
    }
  }

  private async cacheNetProperties(handle: connection.NetHandle): Promise<void> {
    try {
      const properties = await connection.getConnectionProperties(handle) as ConnectionPropertiesLike;
      const ipv4: string[] = [];
      const ipv6: string[] = [];
      const links: LinkAddressLike[] = Array.isArray(properties.linkAddresses) ? properties.linkAddresses : [];
      for (const link of links) {
        const normalized = this.normalizeLinkAddress(link);
        if (!normalized) {
          continue;
        }
        const address = normalized.address;
        const family = normalized.family;
        if (family === 1 || (family === undefined && address.indexOf(':') < 0)) {
          if (address !== '0.0.0.0' && address !== '127.0.0.1' && ipv4.indexOf(address) < 0) {
            ipv4.push(address);
          }
        } else if (family === 2 || address.indexOf(':') >= 0) {
          if (address !== '::' && ipv6.indexOf(address) < 0) {
            ipv6.push(address);
          }
        }
      }
      if (properties.interfaceName && properties.interfaceName.length > 0) {
        this.storeStringArray('__nimActiveInterface', [properties.interfaceName]);
      }
      if (ipv4.length > 0) {
        this.storeStringArray('__nimActiveIPv4', ipv4);
      }
      if (ipv6.length > 0) {
        this.storeStringArray('__nimActiveIPv6', ipv6);
      }
      hilog.info(
        DOMAIN,
        'testTag',
        'Network properties iface=%{public}s ipv4=%{public}s ipv6=%{public}s',
        properties.interfaceName,
        JSON.stringify(ipv4),
        JSON.stringify(ipv6)
      );
    } catch (error) {
      const message = this.describeError(error);
      hilog.error(DOMAIN, 'testTag', 'cacheNetProperties error %{public}s', message);
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

  private unwrapNetAddress(candidate: NetAddressLike): string {
    if (candidate === undefined || candidate === null) {
      return '';
    }
    if (typeof candidate === 'string') {
      return candidate;
    }
    const node = candidate as LinkAddressNode;
    const direct = node.address;
    if (typeof direct === 'string') {
      return direct;
    }
    if (direct !== undefined) {
      const nested = this.unwrapNetAddress(direct);
      if (nested.length > 0) {
        return nested;
      }
    }
    const alt = node.value;
    if (typeof alt === 'string') {
      return alt;
    }
    if (alt !== undefined) {
      const nestedAlt = this.unwrapNetAddress(alt);
      if (nestedAlt.length > 0) {
        return nestedAlt;
      }
    }
    return '';
  }

  private readNetFamily(candidate: NetAddressLike): number | undefined {
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
    const node = candidate as LinkAddressNode;
    const direct = node.family;
    if (typeof direct === 'number') {
      return direct;
    }
    if (node.address !== undefined) {
      const nested = this.readNetFamily(node.address);
      if (nested !== undefined) {
        return nested;
      }
    }
    if (node.value !== undefined) {
      return this.readNetFamily(node.value);
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

  private storeStringArray(key: string, values: string[]): void {
    const sanitized = values
      .map(value => this.sanitizeIpAddress(value))
      .filter(value => value.length > 0);
    if (sanitized.length === 0) {
      return;
    }
    try {
      AppStorage.SetOrCreate(key, sanitized);
    } catch (_) {
      // ignore storage errors
    }
  }

  private teardownNetworkWatcher(): void {
    if (!this.netWatcher) {
      return;
    }
    try {
      this.netWatcher.unregister(this.netWatcherCallback);
    } catch (error) {
      const message = this.describeError(error as ErrorLike);
      hilog.warn(DOMAIN, 'testTag', 'netWatcher unregister error %{public}s', message);
    }
    this.netWatcher = null;
  }

  private describeError(error: ErrorLike): string {
    if (typeof error === 'string') {
      return error;
    }
    if (typeof error === 'number' || typeof error === 'boolean') {
      return String(error);
    }
    if (error && typeof error === 'object') {
      try {
        return JSON.stringify(error);
      } catch (_) {
        return 'object error';
      }
    }
    return 'unknown error';
  }
}

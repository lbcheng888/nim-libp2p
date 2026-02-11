#include <napi/native_api.h>

#ifndef EXTERN_C_START
#ifdef __cplusplus
#define EXTERN_C_START extern "C" {
#define EXTERN_C_END }
#else
#define EXTERN_C_START
#define EXTERN_C_END
#endif
#endif

#include <dlfcn.h>
#include <mutex>
#include <string>
#include <vector>
#include <optional>
#include <cstdarg>
#include <cstdio>
#include <cctype>
#include <cstring>
#include <atomic>
#include <cstdlib>
#include <unistd.h>
#include <hilog/log.h>

namespace {

constexpr unsigned int BRIDGE_LOG_DOMAIN = 0xC020;
constexpr const char *BRIDGE_LOG_TAG = "nim-bridge";

constexpr int32_t kNimResultOk = 0;
constexpr int32_t kNimResultInvalidArgument = 3;

bool g_opensslEnvConfigured = false;
std::string g_lastLibPath;

extern "C" __attribute__((weak)) int OH_LOG_Print(LogType logType, LogLevel level, unsigned int domain, const char *tag, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    fprintf(stderr, "[nim-bridge][%u][%s] ", domain, tag ? tag : "nil");
    if (fmt != nullptr) {
        vfprintf(stderr, fmt, args);
    }
    fprintf(stderr, "\n");
    va_end(args);
    return 0;
}

struct NimBridge {
    void *moduleHandle = nullptr;
    bool runtimeInitialized = false;

    using NimMainFn = void (*)();
    using ThreadAttachFn = void (*)();
    using ThreadDetachFn = void (*)();
    using NodeInitFn = void *(*)(const char *);
    using NodeStartFn = int (*)(void *);
    using NodeStopFn = int (*)(void *);
    using NodeFreeFn = void (*)(void *);
    using NodeIsStartedFn = bool (*)(void *);
    using CStringFn = char *(*)(void *);
    using CStringHandleIntFn = char *(*)(void *, int);
    using CStringPeerFn = char *(*)(void *, const char *);
    using CStringPeerStringFn = char *(*)(void *, const char *, const char *);
    using LastErrorFn = char *(*)();
    using StringFreeFn = void (*)(char *);
    using BoolHandleFn = bool (*)(void *);
    using BoolHandleBoolFn = bool (*)(void *, bool);
    using BoolHandleIntFn = bool (*)(void *, int);
    using BoolPeerFn = bool (*)(void *, const char *);
    using BoolPeerStringFn = bool (*)(void *, const char *, const char *);
    using BoolPeerMessageFn = bool (*)(void *, const char *, const char *, const char *, const char *, bool, uint32_t);
    using BoolPeerAckFn = bool (*)(void *, const char *, const char *, bool, const char *);
    using BoolPeerBytesFn = bool (*)(void *, const char *, const unsigned char *, size_t);
    using BoolPeerJsonIntFn = bool (*)(void *, const char *, const char *, uint32_t);
    using BoolPeerIntFn = bool (*)(void *, const char *, uint32_t);
    using GenerateIdentityFn = char *(*)();
    using IdentityFromSeedFn = char *(*)(const unsigned char *, size_t);
    using RegisterPeerHintsFn = bool (*)(void *, const char *, const char *, const char *);
    using AddExternalAddressFn = bool (*)(void *, const char *);
    using PublishFn = int (*)(void *, const char *, const unsigned char *, size_t, size_t *);
    using IntPeerFn = int (*)(void *, const char *);
    using IntHandleFn = int (*)(void *);
    using PubsubCallbackFn = void (*)(const char *, const unsigned char *, size_t, void *);
    using PubsubSubscribeFn = void *(*)(void *, const char *, PubsubCallbackFn, void *);
    using PubsubUnsubscribeFn = int (*)(void *, void *);

    NimMainFn nimMain = nullptr;
    ThreadAttachFn nimThreadAttach = nullptr;
    ThreadDetachFn nimThreadDetach = nullptr;
    NodeInitFn nodeInit = nullptr;
    NodeStartFn nodeStart = nullptr;
    NodeStopFn nodeStop = nullptr;
    NodeFreeFn nodeFree = nullptr;
    NodeIsStartedFn nodeIsStarted = nullptr;
    CStringFn getListenAddresses = nullptr;
    CStringFn getDialableAddresses = nullptr;
    CStringFn getDiagnostics = nullptr;
    CStringFn getBootstrapStatus = nullptr;
    CStringFn getConnectedPeers = nullptr;
    CStringFn getConnectedPeersInfo = nullptr;
    CStringPeerFn getPeerMultiaddrs = nullptr;
    CStringFn getLocalPeerId = nullptr;
    CStringFn getLanEndpoints = nullptr;
    CStringFn fetchFeedSnapshot = nullptr;
    CStringFn syncPeerstoreState = nullptr;
    CStringFn loadStoredPeers = nullptr;
    CStringFn getLastDirectError = nullptr;
    BoolHandleFn refreshPeerConnections = nullptr;
    BoolHandleFn initializeConnEvents = nullptr;
    IntPeerFn connectPeer = nullptr;
    IntPeerFn disconnectPeer = nullptr;
    IntPeerFn connectMultiaddr = nullptr;
    BoolPeerFn keepAlivePin = nullptr;
    BoolPeerFn keepAliveUnpin = nullptr;
    BoolPeerFn isPeerConnected = nullptr;
    BoolPeerMessageFn sendDirectText = nullptr;
    BoolPeerBytesFn sendDmPayload = nullptr;
    BoolPeerMessageFn sendChatControl = nullptr;
    BoolPeerAckFn sendChatAck = nullptr;
    BoolPeerJsonIntFn sendWithAck = nullptr;
    BoolPeerIntFn waitSecureChannel = nullptr;
    BoolPeerStringFn upsertLivestreamCfg = nullptr;
    BoolPeerBytesFn publishLivestreamFrame = nullptr;
    PublishFn publish = nullptr;
    BoolHandleFn boostConnectivity = nullptr;
    BoolHandleFn reconnectBootstrap = nullptr;
    BoolPeerFn feedSubscribePeer = nullptr;
    BoolPeerFn feedUnsubscribePeer = nullptr;
    BoolPeerFn feedPublishEntry = nullptr;
    BoolHandleIntFn mdnsSetInterval = nullptr;
    BoolHandleFn mdnsProbe = nullptr;
    BoolHandleBoolFn mdnsSetEnabled = nullptr;
    GenerateIdentityFn generateIdentity = nullptr;
    IdentityFromSeedFn identityFromSeed = nullptr;
    RegisterPeerHintsFn registerPeerHints = nullptr;
    AddExternalAddressFn mdnsSetInterface = nullptr;
    CStringFn mdnsDebug = nullptr;
    AddExternalAddressFn addExternalAddress = nullptr;
    PubsubSubscribeFn pubsubSubscribe = nullptr;
    PubsubUnsubscribeFn pubsubUnsubscribe = nullptr;
    LastErrorFn getLastError = nullptr;
    StringFreeFn stringFree = nullptr;
    CStringHandleIntFn pollEvents = nullptr;

    std::mutex mutex;
} g_nim;

constexpr const char *kLibCandidates[] = {
    "libnimlibp2p.so",
    "libnimlibp2p.z.so",
    "./libnimlibp2p.so",
    "./libnimlibp2p.z.so",
    "./libs/arm64-v8a/libnimlibp2p.so",
    "./libs/arm64-v8a/libnimlibp2p.z.so",
    "./libs/arm64/libnimlibp2p.so",
    "./libs/arm64/libnimlibp2p.z.so",
    "./libs/arm64/lib/libnimlibp2p.so",
    "./libs/arm64/lib/libnimlibp2p.z.so",
    "/data/storage/el1/bundle/libs/arm64/lib/libnimlibp2p.so",
    "/data/storage/el1/bundle/libs/arm64/lib/libnimlibp2p.z.so",
    "/system/lib/libnimlibp2p.so",
    "/system/lib/libnimlibp2p.z.so",
    "/system/lib64/libnimlibp2p.so",
    "/system/lib64/libnimlibp2p.z.so",
    "/vendor/lib64/libnimlibp2p.so",
    "/vendor/lib64/libnimlibp2p.z.so",
    "/vendor/lib/libnimlibp2p.so",
    "/vendor/lib/libnimlibp2p.z.so",
    "/data/storage/el2/base/files/libp2p/libnimlibp2p.so",
    "/data/storage/el2/base/files/libp2p/libnimlibp2p.z.so"
};

template <typename T>
bool loadSymbol(void *handle, const char *symbol, T &out) {
    out = reinterpret_cast<T>(dlsym(handle, symbol));
    if (!out) {
        fprintf(stderr, "[nim-bridge] Failed to resolve symbol %s\n", symbol);
        OH_LOG_Print(LOG_APP, LOG_ERROR, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "loadSymbol missing %{public}s", symbol);
        return false;
    }
    return true;
}

template <typename T>
bool loadSymbolOptional(void *handle, const char *symbol, T &out) {
    out = reinterpret_cast<T>(dlsym(handle, symbol));
    if (!out) {
        fprintf(stderr, "[nim-bridge] Optional symbol %s not found\n", symbol);
        OH_LOG_Print(LOG_APP, LOG_WARN, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "optional symbol %{public}s absent", symbol);
    }
    return true;
}

bool directoryExists(const std::string &path) {
    return !path.empty() && access(path.c_str(), F_OK) == 0;
}

void tryConfigureOpensslEnvFromDir(const std::string &dir) {
    if (dir.empty()) {
        return;
    }
    std::string modules = dir + "/ossl-modules";
    std::string engines = dir + "/engines-4";
    bool configured = false;
    if (directoryExists(modules)) {
        setenv("OPENSSL_MODULES", modules.c_str(), 1);
        OH_LOG_Print(LOG_APP, LOG_INFO, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "Configured OPENSSL_MODULES %{public}s", modules.c_str());
        configured = true;
    }
    if (directoryExists(engines)) {
        setenv("OPENSSL_ENGINES", engines.c_str(), 1);
        OH_LOG_Print(LOG_APP, LOG_INFO, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "Configured OPENSSL_ENGINES %{public}s", engines.c_str());
        configured = true;
    }
    if (configured) {
        g_opensslEnvConfigured = true;
    }
}

void configureOpenSslEnvironment(const std::string &libPath) {
    if (g_opensslEnvConfigured) {
        return;
    }
    if (libPath.empty()) {
        return;
    }
    auto pos = libPath.find_last_of('/');
    if (pos == std::string::npos) {
        return;
    }
    std::string dir = libPath.substr(0, pos);
    tryConfigureOpensslEnvFromDir(dir);
    if (!g_opensslEnvConfigured) {
        auto parentPos = dir.find_last_of('/');
        if (parentPos != std::string::npos) {
            tryConfigureOpensslEnvFromDir(dir.substr(0, parentPos));
        }
    }
}

bool ensureLibraryLoadedLocked()
{
    if (g_nim.moduleHandle != nullptr) {
        return true;
    }
    void *handle = nullptr;
    for (const auto *candidate : kLibCandidates) {
        dlerror();
        handle = dlopen(candidate, RTLD_NOW);
        if (handle != nullptr) {
            fprintf(stderr, "[nim-bridge] dlopen(%s) succeeded\n", candidate);
            g_lastLibPath = candidate;
            break;
        }
        const char *err = dlerror();
        if (err == nullptr) {
            err = "unknown";
        }
        fprintf(stderr, "[nim-bridge] dlopen(%s) failed: %s\n", candidate, err);
        OH_LOG_Print(LOG_APP, LOG_WARN, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "dlopen(%{public}s) failed: %{public}s", candidate, err);
    }
    if (!handle) {
        OH_LOG_Print(LOG_APP, LOG_ERROR, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "dlopen libnimlibp2p failed after trying all candidates");
        return false;
    }
    if (!loadSymbol(handle, "NimMain", g_nim.nimMain) ||
        !loadSymbol(handle, "nim_thread_attach", g_nim.nimThreadAttach) ||
        !loadSymbol(handle, "nim_thread_detach", g_nim.nimThreadDetach) ||
        !loadSymbol(handle, "libp2p_node_init", g_nim.nodeInit) ||
        !loadSymbol(handle, "libp2p_node_start", g_nim.nodeStart) ||
        !loadSymbol(handle, "libp2p_node_stop", g_nim.nodeStop) ||
        !loadSymbol(handle, "libp2p_node_free", g_nim.nodeFree) ||
        !loadSymbol(handle, "libp2p_node_is_started", g_nim.nodeIsStarted) ||
        !loadSymbol(handle, "libp2p_get_listen_addresses", g_nim.getListenAddresses) ||
        !loadSymbol(handle, "libp2p_get_dialable_addresses", g_nim.getDialableAddresses) ||
        !loadSymbol(handle, "libp2p_get_local_peer_id", g_nim.getLocalPeerId) ||
        !loadSymbol(handle, "libp2p_get_diagnostics_json", g_nim.getDiagnostics) ||
        !loadSymbol(handle, "libp2p_get_bootstrap_status", g_nim.getBootstrapStatus) ||
        !loadSymbol(handle, "libp2p_get_connected_peers_json", g_nim.getConnectedPeers) ||
        !loadSymbol(handle, "libp2p_connected_peers_info", g_nim.getConnectedPeersInfo) ||
        !loadSymbol(handle, "libp2p_get_peer_multiaddrs_json", g_nim.getPeerMultiaddrs) ||
        !loadSymbol(handle, "libp2p_get_lan_endpoints_json", g_nim.getLanEndpoints) ||
        !loadSymbol(handle, "libp2p_fetch_feed_snapshot", g_nim.fetchFeedSnapshot) ||
        !loadSymbol(handle, "libp2p_feed_subscribe_peer", g_nim.feedSubscribePeer) ||
        !loadSymbol(handle, "libp2p_feed_unsubscribe_peer", g_nim.feedUnsubscribePeer) ||
        !loadSymbol(handle, "libp2p_feed_publish_entry", g_nim.feedPublishEntry) ||
        !loadSymbol(handle, "libp2p_sync_peerstore_state", g_nim.syncPeerstoreState) ||
        !loadSymbol(handle, "libp2p_load_stored_peers", g_nim.loadStoredPeers) ||
        !loadSymbol(handle, "libp2p_get_last_direct_error", g_nim.getLastDirectError) ||
        !loadSymbol(handle, "libp2p_refresh_peer_connections", g_nim.refreshPeerConnections) ||
        !loadSymbol(handle, "libp2p_initialize_conn_events", g_nim.initializeConnEvents) ||
        !loadSymbol(handle, "libp2p_connect_peer", g_nim.connectPeer) ||
        !loadSymbol(handle, "libp2p_disconnect_peer", g_nim.disconnectPeer) ||
        !loadSymbol(handle, "libp2p_connect_multiaddr", g_nim.connectMultiaddr) ||
        !loadSymbol(handle, "libp2p_keepalive_pin", g_nim.keepAlivePin) ||
        !loadSymbol(handle, "libp2p_keepalive_unpin", g_nim.keepAliveUnpin) ||
        !loadSymbol(handle, "libp2p_is_peer_connected", g_nim.isPeerConnected) ||
        !loadSymbol(handle, "libp2p_send_direct_text", g_nim.sendDirectText) ||
        !loadSymbol(handle, "libp2p_send_dm_payload", g_nim.sendDmPayload) ||
        !loadSymbol(handle, "libp2p_send_chat_control", g_nim.sendChatControl) ||
        !loadSymbol(handle, "libp2p_send_chat_ack_ffi", g_nim.sendChatAck) ||
        !loadSymbol(handle, "libp2p_send_with_ack", g_nim.sendWithAck) ||
        !loadSymbol(handle, "libp2p_wait_secure_channel_ffi", g_nim.waitSecureChannel) ||
        !loadSymbol(handle, "libp2p_upsert_livestream_cfg", g_nim.upsertLivestreamCfg) ||
        !loadSymbol(handle, "libp2p_publish_livestream_frame_ffi", g_nim.publishLivestreamFrame) ||
        !loadSymbol(handle, "libp2p_pubsub_publish", g_nim.publish) ||
        !loadSymbol(handle, "libp2p_pubsub_subscribe", g_nim.pubsubSubscribe) ||
        !loadSymbol(handle, "libp2p_pubsub_unsubscribe", g_nim.pubsubUnsubscribe) ||
        !loadSymbol(handle, "libp2p_boost_connectivity", g_nim.boostConnectivity) ||
        !loadSymbol(handle, "libp2p_reconnect_bootstrap", g_nim.reconnectBootstrap) ||
        !loadSymbol(handle, "libp2p_mdns_set_interval", g_nim.mdnsSetInterval) ||
        !loadSymbol(handle, "libp2p_mdns_probe", g_nim.mdnsProbe) ||
        !loadSymbol(handle, "libp2p_get_last_error", g_nim.getLastError) ||
        !loadSymbol(handle, "libp2p_string_free", g_nim.stringFree) ||
        !loadSymbol(handle, "libp2p_mdns_set_enabled", g_nim.mdnsSetEnabled) ||
        !loadSymbol(handle, "libp2p_mdns_set_interface", g_nim.mdnsSetInterface) ||
        !loadSymbolOptional(handle, "libp2p_mdns_debug", g_nim.mdnsDebug) ||
        !loadSymbol(handle, "libp2p_generate_identity_json", g_nim.generateIdentity) ||
        !loadSymbol(handle, "libp2p_identity_from_seed", g_nim.identityFromSeed) ||
        !loadSymbol(handle, "libp2p_register_peer_hints", g_nim.registerPeerHints) ||
        !loadSymbol(handle, "libp2p_add_external_address", g_nim.addExternalAddress) ||
        !loadSymbol(handle, "libp2p_poll_events", g_nim.pollEvents)) {
        dlclose(handle);
        return false;
    }

    if (!g_opensslEnvConfigured) {
        if (!g_lastLibPath.empty() && g_lastLibPath.find('/') != std::string::npos) {
            configureOpenSslEnvironment(g_lastLibPath);
        } else if (g_nim.nodeInit != nullptr) {
            Dl_info info{};
            if (dladdr(reinterpret_cast<void *>(g_nim.nodeInit), &info) != 0 && info.dli_fname != nullptr) {
                configureOpenSslEnvironment(info.dli_fname);
            }
        }
    }

    g_nim.moduleHandle = handle;
    return true;
}

bool ensureLibraryLoaded()
{
    std::lock_guard<std::mutex> guard(g_nim.mutex);
    if (!ensureLibraryLoadedLocked()) {
        return false;
    }
    if (!g_nim.runtimeInitialized && g_nim.nimMain != nullptr) {
        g_nim.nimMain();
        g_nim.runtimeInitialized = true;
    }
    return true;
}

struct ThreadScope {
    ThreadScope() {
        if (g_nim.nimThreadAttach) {
            g_nim.nimThreadAttach();
        }
    }
    ~ThreadScope() {
        if (g_nim.nimThreadDetach) {
            g_nim.nimThreadDetach();
        }
    }
};

std::string napiValueToString(napi_env env, napi_value value)
{
    size_t length = 0;
    napi_get_value_string_utf8(env, value, nullptr, 0, &length);
    std::string result;
    result.resize(length);
    if (length > 0) {
        napi_get_value_string_utf8(env, value, result.data(), length + 1, &length);
    }
    return result;
}

void napiThrowError(napi_env env, const std::string &message)
{
    napi_throw_error(env, nullptr, message.c_str());
}

void napiSetBool(napi_env env, bool ok, napi_value *result)
{
    napi_get_boolean(env, ok, result);
}

std::string escapeJsonString(const std::string &input)
{
    std::string output;
    output.reserve(input.size() + 8);
    for (unsigned char c : input) {
        switch (c) {
        case '\"':
            output += "\\\"";
            break;
        case '\\':
            output += "\\\\";
            break;
        case '\b':
            output += "\\b";
            break;
        case '\f':
            output += "\\f";
            break;
        case '\n':
            output += "\\n";
            break;
        case '\r':
            output += "\\r";
            break;
        case '\t':
            output += "\\t";
            break;
        default:
            if (c < 0x20) {
                char buf[7];
                std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                output += buf;
            } else {
                output += static_cast<char>(c);
            }
        }
    }
    return output;
}

std::string jsonEncodeStringArray(const std::vector<std::string> &items)
{
    std::string json = "[";
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) {
            json += ",";
        }
        json += "\"";
        json += escapeJsonString(items[i]);
        json += "\"";
    }
    json += "]";
    return json;
}

std::optional<std::vector<std::string>> napiArrayToStringVector(napi_env env, napi_value value)
{
    bool isArray = false;
    napi_status status = napi_is_array(env, value, &isArray);
    if (status != napi_ok || !isArray) {
        return std::nullopt;
    }
    uint32_t length = 0;
    if (napi_get_array_length(env, value, &length) != napi_ok) {
        return std::nullopt;
    }
    std::vector<std::string> result;
    result.reserve(length);
    for (uint32_t i = 0; i < length; ++i) {
        napi_value element;
        if (napi_get_element(env, value, i, &element) != napi_ok) {
            continue;
        }
        napi_valuetype type;
        if (napi_typeof(env, element, &type) != napi_ok) {
            continue;
        }
        if (type != napi_string) {
            continue;
        }
        result.emplace_back(napiValueToString(env, element));
    }
    return result;
}

std::optional<std::string> copyAndRelease(char *value)
{
    if (value == nullptr) {
        return std::nullopt;
    }
    std::string result(value);
    if (g_nim.stringFree) {
        g_nim.stringFree(value);
    }
    return result;
}

std::string safeCopyCstr(const char *value)
{
    if (value == nullptr) {
        return std::string();
    }
    return std::string(value);
}

struct PubsubMessage {
    std::string topic;
    std::vector<uint8_t> payload;
};

struct PubsubSubscription {
    std::atomic<uint32_t> refCount {1};
    void *nodeHandle = nullptr;
    void *nativeSubscription = nullptr;
    napi_threadsafe_function tsfn = nullptr;
    std::string topic;
    std::atomic<bool> unsubscribed {false};
    std::atomic<bool> tsfnReleased {false};
    std::atomic<bool> externalReleased {false};
};

void retainSubscription(PubsubSubscription *subscription)
{
    if (subscription != nullptr) {
        subscription->refCount.fetch_add(1, std::memory_order_relaxed);
    }
}

void releaseSubscription(PubsubSubscription *subscription)
{
    if (!subscription) {
        return;
    }
    uint32_t previous = subscription->refCount.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 1) {
        delete subscription;
    }
}

void pubsubCallJs(napi_env env, napi_value jsCallback, void *context, void *data)
{
    (void)context;
    if (!data) {
        return;
    }
    auto *message = static_cast<PubsubMessage *>(data);
    if (env == nullptr || jsCallback == nullptr) {
        delete message;
        return;
    }

    napi_value topicValue = nullptr;
    napi_status status = napi_create_string_utf8(env, message->topic.c_str(), message->topic.length(), &topicValue);
    if (status != napi_ok) {
        delete message;
        return;
    }

    napi_value arrayBuffer = nullptr;
    void *bufferData = nullptr;
    status = napi_create_arraybuffer(env, message->payload.size(), &bufferData, &arrayBuffer);
    if (status != napi_ok) {
        delete message;
        return;
    }
    if (!message->payload.empty() && bufferData != nullptr) {
        std::memcpy(bufferData, message->payload.data(), message->payload.size());
    }

    napi_value typedArray = nullptr;
    status = napi_create_typedarray(env, napi_uint8_array, message->payload.size(), arrayBuffer, 0, &typedArray);
    if (status != napi_ok) {
        delete message;
        return;
    }

    napi_value args[2] = { topicValue, typedArray };
    napi_value result;
    status = napi_call_function(env, nullptr, jsCallback, 2, args, &result);
    if (status != napi_ok) {
        fprintf(stderr, "[nim-bridge] Failed to deliver pubsub message to JS callback (status=%d)\n", static_cast<int>(status));
    }
    delete message;
}

void pubsubTsfnFinalize(napi_env env, void *finalizeData, void *finalizeHint)
{
    (void)env;
    (void)finalizeHint;
    auto *subscription = static_cast<PubsubSubscription *>(finalizeData);
    releaseSubscription(subscription);
}

void performUnsubscribe(PubsubSubscription *subscription, napi_threadsafe_function_release_mode mode)
{
    if (!subscription) {
        return;
    }
    bool wasUnsubscribed = subscription->unsubscribed.exchange(true, std::memory_order_acq_rel);
    if (!wasUnsubscribed) {
        ThreadScope scope;
        if (g_nim.pubsubUnsubscribe && subscription->nodeHandle && subscription->nativeSubscription) {
            g_nim.pubsubUnsubscribe(subscription->nodeHandle, subscription->nativeSubscription);
        }
        subscription->nativeSubscription = nullptr;
    }
    bool wasReleased = subscription->tsfnReleased.exchange(true, std::memory_order_acq_rel);
    napi_threadsafe_function tsfn = subscription->tsfn;
    if (!wasReleased && tsfn != nullptr) {
        subscription->tsfn = nullptr;
        napi_release_threadsafe_function(tsfn, mode);
    }
}

bool enqueuePubsubMessage(PubsubSubscription *subscription, const char *topic, const unsigned char *payload, size_t payloadLen)
{
    if (!subscription || subscription->tsfn == nullptr || subscription->unsubscribed) {
        return false;
    }
    auto *message = new PubsubMessage();
    message->topic = topic ? topic : "";
    if (payload != nullptr && payloadLen > 0) {
        message->payload.assign(payload, payload + payloadLen);
    }
    napi_status status = napi_call_threadsafe_function(subscription->tsfn, message, napi_tsfn_nonblocking);
    if (status != napi_ok) {
        delete message;
        return false;
    }
    return true;
}

void pubsubNativeCallback(const char *topic, const unsigned char *payload, size_t payloadLen, void *userData)
{
    ThreadScope scope;
    auto *subscription = static_cast<PubsubSubscription *>(userData);
    if (!subscription) {
        return;
    }
    if (!enqueuePubsubMessage(subscription, topic, payload, payloadLen)) {
        fprintf(stderr, "[nim-bridge] Dropped pubsub message for topic %s\n", topic ? topic : "");
    }
}

void subscriptionExternalFinalizer(napi_env env, void *data, void *hint)
{
    (void)env;
    (void)hint;
    auto *subscription = static_cast<PubsubSubscription *>(data);
    if (!subscription) {
        return;
    }
    bool wasReleased = subscription->externalReleased.exchange(true, std::memory_order_acq_rel);
    if (wasReleased) {
        return;
    }
    performUnsubscribe(subscription, napi_tsfn_abort);
    releaseSubscription(subscription);
}

napi_value wrapString(napi_env env, char *cstr)
{
    auto str = copyAndRelease(cstr);
    if (!str.has_value()) {
        napi_value result;
        napi_get_null(env, &result);
        return result;
    }
    napi_value result;
    napi_create_string_utf8(env, str->c_str(), str->length(), &result);
    return result;
}

std::optional<std::vector<uint8_t>> napiValueToByteVector(napi_env env, napi_value value)
{
    bool isArrayBuffer = false;
    if (napi_is_arraybuffer(env, value, &isArrayBuffer) == napi_ok && isArrayBuffer) {
        void *data = nullptr;
        size_t length = 0;
        if (napi_get_arraybuffer_info(env, value, &data, &length) != napi_ok || data == nullptr || length == 0) {
            return std::nullopt;
        }
        const auto *bytes = static_cast<const uint8_t *>(data);
        return std::vector<uint8_t>(bytes, bytes + length);
    }
    bool isTypedArray = false;
    if (napi_is_typedarray(env, value, &isTypedArray) == napi_ok && isTypedArray) {
        napi_typedarray_type type;
        size_t length = 0;
        void *data = nullptr;
        napi_value arrayBuffer;
        size_t byteOffset = 0;
        if (napi_get_typedarray_info(env, value, &type, &length, &data, &arrayBuffer, &byteOffset) != napi_ok ||
            data == nullptr || length == 0) {
            return std::nullopt;
        }
        const auto *bytes = static_cast<const uint8_t *>(data);
        size_t elementSize = 1;
        switch (type) {
            case napi_uint8_array:
            case napi_int8_array:
            case napi_uint8_clamped_array:
                elementSize = 1;
                break;
            case napi_uint16_array:
            case napi_int16_array:
                elementSize = 2;
                break;
            case napi_uint32_array:
            case napi_int32_array:
            case napi_float32_array:
                elementSize = 4;
                break;
            case napi_float64_array:
                elementSize = 8;
                break;
            default:
                elementSize = 1;
                break;
        }
        size_t byteLength = length * elementSize;
        return std::vector<uint8_t>(bytes, bytes + byteLength);
    }
    return std::nullopt;
}

void nodeHandleFinalizer(napi_env env, void *finalizeData, void *finalizeHint)
{
    if (g_nim.nodeFree && finalizeData != nullptr) {
        ThreadScope scope;
        g_nim.nodeFree(finalizeData);
    }
}

void *getNodeHandle(napi_env env, napi_value value)
{
    void *handle = nullptr;
    if (napi_get_value_external(env, value, &handle) != napi_ok) {
        handle = nullptr;
    }
    return handle;
}

napi_value napiNodeInit(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);

    if (!ensureLibraryLoaded()) {
        napiThrowError(env, "Failed to load libnimlibp2p.so");
        return nullptr;
    }

    ThreadScope scope;

    const char *configPtr = nullptr;
    std::string config;
    if (argc >= 1) {
        napi_valuetype type;
        napi_typeof(env, args[0], &type);
        if (type == napi_string) {
            config = napiValueToString(env, args[0]);
            configPtr = config.c_str();
        }
    }

    void *handle = g_nim.nodeInit ? g_nim.nodeInit(configPtr) : nullptr;
    if (handle == nullptr) {
        std::string err = "[nim-bridge] libp2p_node_init failed";
        if (g_nim.getLastError) {
            char *msg = g_nim.getLastError();
            auto str = copyAndRelease(msg);
            if (str.has_value()) {
                err += ": " + str.value();
            }
        }
        napiThrowError(env, err);
        return nullptr;
    }

    napi_value external;
    napi_status status = napi_create_external(env, handle, nodeHandleFinalizer, nullptr, &external);
    if (status != napi_ok) {
        g_nim.nodeFree(handle);
        napiThrowError(env, "Failed to wrap node handle");
        return nullptr;
    }
    OH_LOG_Print(LOG_APP, LOG_INFO, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG, "napiNodeInit created handle=%{public}p", handle);
    return external;
}

napi_value napiNodeStart(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "nodeStart requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    OH_LOG_Print(LOG_APP, LOG_INFO, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG, "napiNodeStart invoked handle=%{public}p", handle);
    napi_value result;
    int32_t rc = kNimResultInvalidArgument;
    if (g_nim.nodeStart && handle) {
        rc = static_cast<int32_t>(g_nim.nodeStart(handle));
        if (rc != kNimResultOk) {
            fprintf(stderr, "[nim-bridge] libp2p_node_start failed with code %d\n", rc);
            if (g_nim.getLastError) {
                char *msg = g_nim.getLastError();
                if (msg != nullptr) {
                    OH_LOG_Print(LOG_APP, LOG_ERROR, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG, "libp2p_node_start failed: %{public}s", msg);
                    fprintf(stderr, "[nim-bridge] lastError: %s\n", msg);
                    if (g_nim.stringFree) {
                        g_nim.stringFree(msg);
                    }
                } else {
                    OH_LOG_Print(LOG_APP, LOG_ERROR, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG, "libp2p_node_start failed with code %{public}d", rc);
                }
            } else {
                OH_LOG_Print(LOG_APP, LOG_ERROR, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG, "libp2p_node_start failed with code %{public}d", rc);
            }
        }
    }
    napi_create_int32(env, rc, &result);
    return result;
}

napi_value napiNodeStop(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "nodeStop requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    napi_value result;
    int32_t rc = kNimResultInvalidArgument;
    if (g_nim.nodeStop && handle) {
        rc = static_cast<int32_t>(g_nim.nodeStop(handle));
    }
    napi_create_int32(env, rc, &result);
    return result;
}

napi_value napiNodeFree(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "freeNode requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (g_nim.nodeFree && handle) {
        g_nim.nodeFree(handle);
    }
    napi_value undefinedValue;
    napi_get_undefined(env, &undefinedValue);
    return undefinedValue;
}

napi_value napiNodeIsStarted(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "nodeIsStarted requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    napi_value result;
    bool ok = g_nim.nodeIsStarted && handle && g_nim.nodeIsStarted(handle);
    napi_get_boolean(env, ok, &result);
    return result;
}

napi_value napiGetListenAddresses(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getListenAddresses requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
  if (!handle || !g_nim.getListenAddresses) {
    napi_value nullValue;
    napi_get_null(env, &nullValue);
    return nullValue;
  }
  char *raw = g_nim.getListenAddresses(handle);
  std::string safe = safeCopyCstr(raw);
  napi_value result;
  napi_create_string_utf8(env, safe.c_str(), safe.length(), &result);
  if (raw && g_nim.stringFree) {
    g_nim.stringFree(raw);
  }
  return result;
}

napi_value napiGetDialableAddresses(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getDialableAddresses requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getDialableAddresses) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getDialableAddresses(handle));
}

napi_value napiGetLocalPeerId(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "localPeerId requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getLocalPeerId) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getLocalPeerId(handle));
}

napi_value napiGetDiagnostics(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getDiagnostics requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getDiagnostics) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getDiagnostics(handle));
}

napi_value napiGetBootstrapStatus(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getBootstrapStatus requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getBootstrapStatus) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getBootstrapStatus(handle));
}

napi_value napiGetConnectedPeers(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getConnectedPeers requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getConnectedPeers) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getConnectedPeers(handle));
}

napi_value napiGetConnectedPeersInfo(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getConnectedPeersInfo requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getConnectedPeersInfo) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getConnectedPeersInfo(handle));
}

napi_value napiGetPeerMultiaddrs(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "getPeerMultiaddrs(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    if (!handle || !g_nim.getPeerMultiaddrs || peerId.empty()) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getPeerMultiaddrs(handle, peerId.c_str()));
}

napi_value napiGetLanEndpoints(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getLanEndpoints requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getLanEndpoints) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getLanEndpoints(handle));
}

napi_value napiGetLanEndpointsAsync(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "lanEndpointsAsync requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getLanEndpoints) {
        napi_value promise;
        napi_deferred deferred;
        napi_create_promise(env, &deferred, &promise);
        napi_value empty;
        napi_create_string_utf8(env, "{}", NAPI_AUTO_LENGTH, &empty);
        napi_resolve_deferred(env, deferred, empty);
        return promise;
    }

    struct AsyncLanEndpointsWork {
        void *handle;
        napi_deferred deferred;
        napi_async_work work;
        std::string result;
    };

    auto *workData = new AsyncLanEndpointsWork();
    workData->handle = handle;
    workData->deferred = nullptr;
    workData->work = nullptr;
    workData->result = "{}";

    napi_value promise;
    napi_create_promise(env, &workData->deferred, &promise);

    napi_value resourceName;
    napi_create_string_utf8(env, "lanEndpointsAsync", NAPI_AUTO_LENGTH, &resourceName);

    auto execute = [](napi_env envExec, void *data) {
        auto *ctx = static_cast<AsyncLanEndpointsWork *>(data);
        ThreadScope scope;
        if (!ctx->handle || !g_nim.getLanEndpoints) {
            ctx->result = "{}";
            return;
        }
        char *raw = g_nim.getLanEndpoints(ctx->handle);
        if (raw != nullptr) {
            ctx->result.assign(raw);
            if (g_nim.stringFree) {
                g_nim.stringFree(raw);
            }
        } else {
            ctx->result = "{}";
        }
    };

    auto complete = [](napi_env envComp, napi_status status, void *data) {
        auto *ctx = static_cast<AsyncLanEndpointsWork *>(data);
        napi_value output;
        if (status == napi_ok) {
            napi_create_string_utf8(envComp, ctx->result.c_str(), NAPI_AUTO_LENGTH, &output);
            napi_resolve_deferred(envComp, ctx->deferred, output);
        } else {
            napi_value error;
            napi_create_string_utf8(envComp, "lanEndpointsAsync failed", NAPI_AUTO_LENGTH, &error);
            napi_reject_deferred(envComp, ctx->deferred, error);
        }
        napi_delete_async_work(envComp, ctx->work);
        delete ctx;
    };

    napi_create_async_work(env, nullptr, resourceName, execute, complete, workData, &workData->work);
    napi_queue_async_work(env, workData->work);
    return promise;
}

napi_value napiFetchFeedSnapshot(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "fetchFeedSnapshot requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.fetchFeedSnapshot) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.fetchFeedSnapshot(handle));
}

napi_value napiSubscribeFeedPeer(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "subscribeFeedPeer(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    bool ok = g_nim.feedSubscribePeer && handle && !peerId.empty() &&
              g_nim.feedSubscribePeer(handle, peerId.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiUnsubscribeFeedPeer(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "unsubscribeFeedPeer(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    bool ok = g_nim.feedUnsubscribePeer && handle && !peerId.empty() &&
              g_nim.feedUnsubscribePeer(handle, peerId.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiPublishFeedEntry(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "publishFeedEntry(handle, payload) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string payload = napiValueToString(env, args[1]);
    bool ok = g_nim.feedPublishEntry && handle && !payload.empty() &&
              g_nim.feedPublishEntry(handle, payload.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSyncPeerstoreState(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "syncPeerstoreState requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.syncPeerstoreState) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.syncPeerstoreState(handle));
}

napi_value napiLoadStoredPeers(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "loadStoredPeers requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.loadStoredPeers) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.loadStoredPeers(handle));
}

napi_value napiGetLastDirectError(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "getLastDirectError requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    if (!handle || !g_nim.getLastDirectError) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    return wrapString(env, g_nim.getLastDirectError(handle));
}

napi_value napiRefreshPeerConnections(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "refreshPeerConnections requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    bool ok = g_nim.refreshPeerConnections && handle && g_nim.refreshPeerConnections(handle);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiInitializeConnEvents(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "initializeConnEvents requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    napi_value result;
    bool ok = g_nim.initializeConnEvents && handle && g_nim.initializeConnEvents(handle);
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiConnectPeer(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "connectPeer(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    bool ok = g_nim.connectPeer && handle && !peerId.empty() && (g_nim.connectPeer(handle, peerId.c_str()) == 0);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiDisconnectPeer(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "disconnectPeer(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    bool ok = g_nim.disconnectPeer && handle && !peerId.empty() &&
              (g_nim.disconnectPeer(handle, peerId.c_str()) == 0);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiConnectMultiaddr(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "connectMultiaddr(handle, multiaddr) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string addr = napiValueToString(env, args[1]);
    bool ok = g_nim.connectMultiaddr && handle && !addr.empty() &&
              (g_nim.connectMultiaddr(handle, addr.c_str()) == 0);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiConnectPeerAsync(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "connectPeerAsync(handle, peerId) required");
        return nullptr;
    }
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    if (!handle || peerId.empty()) {
        napiThrowError(env, "connectPeerAsync requires valid handle and peerId");
        return nullptr;
    }

    struct AsyncConnectPeerWork {
        void *handle;
        std::string peerId;
        napi_deferred deferred;
        napi_async_work work;
        int result;
    };

    auto *workData = new AsyncConnectPeerWork();
    workData->handle = handle;
    workData->peerId = peerId;
    workData->deferred = nullptr;
    workData->work = nullptr;
    workData->result = -1;

    napi_value promise;
    napi_create_promise(env, &workData->deferred, &promise);

    napi_value resourceName;
    napi_create_string_utf8(env, "connectPeerAsync", NAPI_AUTO_LENGTH, &resourceName);

    auto execute = [](napi_env envExec, void *data) {
        auto *ctx = static_cast<AsyncConnectPeerWork *>(data);
        ThreadScope scope;
        if (!ctx->handle || !g_nim.connectPeer) {
            ctx->result = -1;
            return;
        }
        ctx->result = g_nim.connectPeer(ctx->handle, ctx->peerId.c_str());
    };

    auto complete = [](napi_env envComp, napi_status status, void *data) {
        auto *ctx = static_cast<AsyncConnectPeerWork *>(data);
        if (status == napi_ok) {
            napi_value output;
            napi_create_int32(envComp, ctx->result, &output);
            napi_resolve_deferred(envComp, ctx->deferred, output);
        } else {
            napi_value error;
            napi_create_string_utf8(envComp, "connectPeerAsync failed", NAPI_AUTO_LENGTH, &error);
            napi_reject_deferred(envComp, ctx->deferred, error);
        }
        napi_delete_async_work(envComp, ctx->work);
        delete ctx;
    };

    napi_create_async_work(env, nullptr, resourceName, execute, complete, workData, &workData->work);
    napi_queue_async_work(env, workData->work);
    return promise;
}

napi_value napiConnectMultiaddrAsync(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "connectMultiaddrAsync(handle, multiaddr) required");
        return nullptr;
    }
    void *handle = getNodeHandle(env, args[0]);
    std::string addr = napiValueToString(env, args[1]);
    if (!handle || addr.empty()) {
        napiThrowError(env, "connectMultiaddrAsync requires valid handle and multiaddr");
        return nullptr;
    }

    struct AsyncConnectMultiaddrWork {
        void *handle;
        std::string addr;
        napi_deferred deferred;
        napi_async_work work;
        bool result;
    };

    auto *workData = new AsyncConnectMultiaddrWork();
    workData->handle = handle;
    workData->addr = addr;
    workData->deferred = nullptr;
    workData->work = nullptr;
    workData->result = false;

    napi_value promise;
    napi_create_promise(env, &workData->deferred, &promise);

    napi_value resourceName;
    napi_create_string_utf8(env, "connectMultiaddrAsync", NAPI_AUTO_LENGTH, &resourceName);

    auto execute = [](napi_env envExec, void *data) {
        auto *ctx = static_cast<AsyncConnectMultiaddrWork *>(data);
        ThreadScope scope;
        if (!ctx->handle || !g_nim.connectMultiaddr) {
            ctx->result = false;
            return;
        }
        ctx->result = g_nim.connectMultiaddr(ctx->handle, ctx->addr.c_str());
    };

    auto complete = [](napi_env envComp, napi_status status, void *data) {
        auto *ctx = static_cast<AsyncConnectMultiaddrWork *>(data);
        if (status == napi_ok) {
            napi_value output;
            napi_get_boolean(envComp, ctx->result, &output);
            napi_resolve_deferred(envComp, ctx->deferred, output);
        } else {
            napi_value error;
            napi_create_string_utf8(envComp, "connectMultiaddrAsync failed", NAPI_AUTO_LENGTH, &error);
            napi_reject_deferred(envComp, ctx->deferred, error);
        }
        napi_delete_async_work(envComp, ctx->work);
        delete ctx;
    };

    napi_create_async_work(env, nullptr, resourceName, execute, complete, workData, &workData->work);
    napi_queue_async_work(env, workData->work);
    return promise;
}

napi_value napiKeepAlivePin(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "keepAlivePin(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    bool ok = g_nim.keepAlivePin && handle && !peerId.empty() && g_nim.keepAlivePin(handle, peerId.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiKeepAliveUnpin(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "keepAliveUnpin(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    bool ok = g_nim.keepAliveUnpin && handle && !peerId.empty() && g_nim.keepAliveUnpin(handle, peerId.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiIsPeerConnected(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "isPeerConnected(handle, peerId) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    bool ok = g_nim.isPeerConnected && handle && !peerId.empty() && g_nim.isPeerConnected(handle, peerId.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSendDirectText(napi_env env, napi_callback_info info)
{
    size_t argc = 7;
    napi_value args[7];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 4) {
        napiThrowError(env, "sendDirectText(handle, peerId, messageId, text[, replyTo, requestAck, timeoutMs]) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    std::string messageId = napiValueToString(env, args[2]);
    std::string text = napiValueToString(env, args[3]);
    std::string replyTo;
    bool requestAck = false;
    uint32_t timeoutMs = 5000;
    if (argc >= 5) {
        napi_valuetype type;
        napi_typeof(env, args[4], &type);
        if (type == napi_string) {
            replyTo = napiValueToString(env, args[4]);
        }
    }
    if (argc >= 6) {
        napi_get_value_bool(env, args[5], &requestAck);
    }
    if (argc >= 7) {
        double timeoutDouble = 0;
        if (napi_get_value_double(env, args[6], &timeoutDouble) == napi_ok && timeoutDouble > 0) {
            timeoutMs = static_cast<uint32_t>(timeoutDouble);
        }
    }
    const char *replyPtr = replyTo.empty() ? nullptr : replyTo.c_str();
    bool ok = g_nim.sendDirectText && handle && !peerId.empty() && !messageId.empty() &&
              g_nim.sendDirectText(handle, peerId.c_str(), messageId.c_str(), text.c_str(), replyPtr, requestAck, timeoutMs);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSendDirect(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "sendDirect(handle, peerId, payload) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    std::string payload = napiValueToString(env, args[2]);
    bool ok = false;
    if (g_nim.sendDmPayload && handle && !peerId.empty() && !payload.empty()) {
        ok = g_nim.sendDmPayload(
            handle,
            peerId.c_str(),
            reinterpret_cast<const unsigned char *>(payload.data()),
            payload.size());
    }
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSendDirectAsync(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "sendDirectAsync(handle, peerId, payload) required");
        return nullptr;
    }
    if (!ensureLibraryLoaded() || !g_nim.sendDmPayload) {
        napi_value promise;
        napi_deferred deferred;
        napi_create_promise(env, &deferred, &promise);
        napi_value failure;
        napi_get_boolean(env, false, &failure);
        napi_resolve_deferred(env, deferred, failure);
        return promise;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    std::string payload = napiValueToString(env, args[2]);
    if (!handle || peerId.empty() || payload.empty()) {
        napi_value promise;
        napi_deferred deferred;
        napi_create_promise(env, &deferred, &promise);
        napi_value failure;
        napi_get_boolean(env, false, &failure);
        napi_resolve_deferred(env, deferred, failure);
        return promise;
    }

    struct SendDirectAsyncContext {
        void *handle;
        std::string peerId;
        std::string payload;
        napi_deferred deferred;
        napi_async_work work;
        bool result;
    };

    auto *ctx = new SendDirectAsyncContext();
    ctx->handle = handle;
    ctx->peerId = std::move(peerId);
    ctx->payload = std::move(payload);
    ctx->deferred = nullptr;
    ctx->work = nullptr;
    ctx->result = false;

    napi_value promise;
    napi_create_promise(env, &ctx->deferred, &promise);

    napi_value resourceName;
    napi_create_string_utf8(env, "sendDirectAsync", NAPI_AUTO_LENGTH, &resourceName);

    auto execute = [](napi_env envExec, void *data) {
        auto *context = static_cast<SendDirectAsyncContext *>(data);
        if (!ensureLibraryLoaded() || !g_nim.sendDmPayload) {
            context->result = false;
            return;
        }
        ThreadScope scope;
        context->result = g_nim.sendDmPayload(
            context->handle,
            context->peerId.c_str(),
            reinterpret_cast<const unsigned char *>(context->payload.data()),
            context->payload.size());
    };

    auto complete = [](napi_env envComp, napi_status status, void *data) {
        auto *context = static_cast<SendDirectAsyncContext *>(data);
        napi_value resolved;
        if (status == napi_ok) {
            napi_get_boolean(envComp, context->result, &resolved);
            napi_resolve_deferred(envComp, context->deferred, resolved);
        } else {
            napi_value failure;
            napi_get_boolean(envComp, false, &failure);
            napi_reject_deferred(envComp, context->deferred, failure);
        }
        napi_delete_async_work(envComp, context->work);
        delete context;
    };

    napi_status status = napi_create_async_work(env, nullptr, resourceName, execute, complete, ctx, &ctx->work);
    if (status != napi_ok) {
        napi_value failure;
        napi_get_boolean(env, false, &failure);
        napi_resolve_deferred(env, ctx->deferred, failure);
        delete ctx;
        return promise;
    }
    status = napi_queue_async_work(env, ctx->work);
    if (status != napi_ok) {
        napi_delete_async_work(env, ctx->work);
        napi_value failure;
        napi_get_boolean(env, false, &failure);
        napi_resolve_deferred(env, ctx->deferred, failure);
        delete ctx;
        return promise;
    }
    return promise;
}

napi_value napiSendChatControl(napi_env env, napi_callback_info info)
{
    size_t argc = 7;
    napi_value args[7];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 5) {
        napiThrowError(env, "sendChatControl(handle, peerId, op, messageId, target[, requestAck, timeoutMs]) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    std::string op = napiValueToString(env, args[2]);
    std::string messageId = napiValueToString(env, args[3]);
    std::string target = napiValueToString(env, args[4]);
    bool requestAck = false;
    uint32_t timeoutMs = 5000;
    if (argc >= 6) {
        napi_get_value_bool(env, args[5], &requestAck);
    }
    if (argc >= 7) {
        double timeoutDouble = 0;
        if (napi_get_value_double(env, args[6], &timeoutDouble) == napi_ok && timeoutDouble > 0) {
            timeoutMs = static_cast<uint32_t>(timeoutDouble);
        }
    }
    const char *targetPtr = target.empty() ? nullptr : target.c_str();
    bool ok = g_nim.sendChatControl && handle && !peerId.empty() && !op.empty() && !messageId.empty() &&
              g_nim.sendChatControl(handle, peerId.c_str(), op.c_str(), messageId.c_str(), targetPtr, requestAck, timeoutMs);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSendChatAck(napi_env env, napi_callback_info info)
{
    size_t argc = 5;
    napi_value args[5];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 4) {
        napiThrowError(env, "sendChatAck(handle, peerId, messageId, success[, errorMessage]) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    std::string messageId = napiValueToString(env, args[2]);
    bool success = false;
    napi_get_value_bool(env, args[3], &success);
    std::string error;
    if (argc >= 5) {
        napi_valuetype type;
        napi_typeof(env, args[4], &type);
        if (type == napi_string) {
            error = napiValueToString(env, args[4]);
        }
    }
    const char *errorPtr = error.empty() ? nullptr : error.c_str();
    bool ok = g_nim.sendChatAck && handle && !peerId.empty() && !messageId.empty() &&
              g_nim.sendChatAck(handle, peerId.c_str(), messageId.c_str(), success, errorPtr);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSendWithAck(napi_env env, napi_callback_info info)
{
    size_t argc = 4;
    napi_value args[4];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "sendWithAck(handle, peerId, payload[, timeoutMs]) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    std::string payload = napiValueToString(env, args[2]);
    uint32_t timeoutMs = 5000;
    if (argc >= 4) {
        double timeoutDouble = 0;
        if (napi_get_value_double(env, args[3], &timeoutDouble) == napi_ok && timeoutDouble > 0) {
            timeoutMs = static_cast<uint32_t>(timeoutDouble);
        }
    }
    bool ok = g_nim.sendWithAck && handle && !peerId.empty() &&
              g_nim.sendWithAck(handle, peerId.c_str(), payload.c_str(), timeoutMs);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiWaitSecureChannel(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "waitSecureChannel(handle, peerId, timeoutMs) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    double timeoutDouble = 0;
    napi_get_value_double(env, args[2], &timeoutDouble);
    uint32_t timeoutMs = timeoutDouble > 0 ? static_cast<uint32_t>(timeoutDouble) : 5000;
    bool ok = g_nim.waitSecureChannel && handle && !peerId.empty() &&
              g_nim.waitSecureChannel(handle, peerId.c_str(), timeoutMs);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiUpsertLivestreamConfig(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "upsertLivestreamConfig(handle, streamKey, configJson) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string streamKey = napiValueToString(env, args[1]);
    std::string config = napiValueToString(env, args[2]);
    bool ok = g_nim.upsertLivestreamCfg && handle && !streamKey.empty() &&
              g_nim.upsertLivestreamCfg(handle, streamKey.c_str(), config.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiPublishLivestreamFrame(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "publishLivestreamFrame(handle, streamKey, payload) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string streamKey = napiValueToString(env, args[1]);
    auto payload = napiValueToByteVector(env, args[2]);
    bool ok = false;
    if (g_nim.publishLivestreamFrame && handle && !streamKey.empty() && payload.has_value() &&
        !payload->empty()) {
        ok = g_nim.publishLivestreamFrame(
            handle,
            streamKey.c_str(),
            payload->data(),
            payload->size());
    }
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiPublishBroadcast(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "publish(handle, topic, payload) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string topic = napiValueToString(env, args[1]);
    auto payload = napiValueToByteVector(env, args[2]);
    bool ok = false;
    if (g_nim.publish && handle && !topic.empty() && payload.has_value() && !payload->empty()) {
        size_t delivered = 0;
        int rc = g_nim.publish(handle, topic.c_str(), payload->data(), payload->size(), &delivered);
        ok = (rc == 0);
    }
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSubscribeTopic(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "subscribeTopic(handle, topic, callback) required");
        return nullptr;
    }
    if (!ensureLibraryLoaded() || g_nim.pubsubSubscribe == nullptr || g_nim.pubsubUnsubscribe == nullptr) {
        napiThrowError(env, "libnimlibp2p pubsub functions unavailable");
        return nullptr;
    }

    void *handle = getNodeHandle(env, args[0]);
    std::string topic = napiValueToString(env, args[1]);
    bool isFunction = false;
    napi_valuetype callbackType;
    if (napi_typeof(env, args[2], &callbackType) == napi_ok) {
        isFunction = (callbackType == napi_function);
    }
    if (!handle) {
        napiThrowError(env, "Invalid libp2p node handle");
        return nullptr;
    }
    if (topic.empty()) {
        napiThrowError(env, "Topic must not be empty");
        return nullptr;
    }
    if (!isFunction) {
        napiThrowError(env, "Callback must be a function");
        return nullptr;
    }

    auto *subscription = new PubsubSubscription();
    subscription->nodeHandle = handle;
    subscription->topic = topic;

    retainSubscription(subscription); // reference for thread-safe function

    napi_value resourceName;
    napi_create_string_utf8(env, "nimLibp2pSubscribe", sizeof("nimLibp2pSubscribe") - 1, &resourceName);

    napi_threadsafe_function tsfn = nullptr;
    napi_status tsfnStatus = napi_create_threadsafe_function(
        env,
        args[2],
        nullptr,
        resourceName,
        0,
        1,
        subscription,
        pubsubTsfnFinalize,
        subscription,
        pubsubCallJs,
        &tsfn);
    if (tsfnStatus != napi_ok) {
        releaseSubscription(subscription); // thread-safe function ref
        releaseSubscription(subscription); // external ref
        napiThrowError(env, "Failed to create pubsub callback bridge");
        return nullptr;
    }
    subscription->tsfn = tsfn;

    ThreadScope scope;
    void *nativeSub = g_nim.pubsubSubscribe(handle, topic.c_str(), pubsubNativeCallback, subscription);
    if (!nativeSub) {
        performUnsubscribe(subscription, napi_tsfn_abort);
        subscription->externalReleased = true;
        releaseSubscription(subscription);
        napiThrowError(env, "Native subscribe returned null");
        return nullptr;
    }
    subscription->nativeSubscription = nativeSub;

    napi_value external;
    napi_status externalStatus = napi_create_external(env, subscription, subscriptionExternalFinalizer, nullptr, &external);
    if (externalStatus != napi_ok) {
        performUnsubscribe(subscription, napi_tsfn_abort);
        subscription->externalReleased = true;
        releaseSubscription(subscription);
        napiThrowError(env, "Failed to wrap subscription handle");
        return nullptr;
    }

    return external;
}

napi_value napiUnsubscribeTopic(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "unsubscribeTopic(handle, subscription) required");
        return nullptr;
    }
    if (!g_nim.pubsubUnsubscribe) {
        napiThrowError(env, "pubsub unsubscribe not available");
        return nullptr;
    }
    void *handle = getNodeHandle(env, args[0]);
    if (!handle) {
        napiThrowError(env, "Invalid libp2p node handle");
        return nullptr;
    }
    (void)handle;
    void *data = nullptr;
    if (napi_get_value_external(env, args[1], &data) != napi_ok) {
        napiThrowError(env, "Invalid subscription handle");
        return nullptr;
    }
    auto *subscription = static_cast<PubsubSubscription *>(data);
    bool success = false;
    if (subscription && !subscription->externalReleased) {
        subscription->externalReleased = true;
        performUnsubscribe(subscription, napi_tsfn_release);
        releaseSubscription(subscription);
        success = true;
    }
    napi_value result;
    napiSetBool(env, success, &result);
    return result;
}

napi_value napiBoostConnectivity(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "boostConnectivity requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    bool ok = g_nim.boostConnectivity && handle && g_nim.boostConnectivity(handle);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiReconnectBootstrap(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "reconnectBootstrap requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    bool ok = g_nim.reconnectBootstrap && handle && g_nim.reconnectBootstrap(handle);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSetMdnsInterval(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "setMdnsInterval(handle, seconds) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    int32_t interval = 0;
    napi_get_value_int32(env, args[1], &interval);
    bool ok = g_nim.mdnsSetInterval && handle && g_nim.mdnsSetInterval(handle, interval);
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiSetMdnsInterface(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "setMdnsInterface(handle, ipv4) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string ipv4 = napiValueToString(env, args[1]);
    bool ok = g_nim.mdnsSetInterface && handle && g_nim.mdnsSetInterface(handle, ipv4.c_str());
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiMdnsProbe(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "mdnsProbe requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    bool ok = false;
    if (g_nim.mdnsProbe && handle) {
        ok = g_nim.mdnsProbe(handle);
        OH_LOG_Print(LOG_APP, LOG_INFO, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "napiMdnsProbe handle=%{public}p result=%{public}d", handle, ok ? 1 : 0);
    }
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiMdnsDebug(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "mdnsDebug requires handle");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string payload = "{}";
    if (g_nim.mdnsDebug && handle) {
        if (char *raw = g_nim.mdnsDebug(handle); raw != nullptr) {
            payload.assign(raw);
            g_nim.stringFree(raw);
        }
    }
    napi_value result;
    napi_create_string_utf8(env, payload.c_str(), payload.size(), &result);
    return result;
}

napi_value napiSetMdnsEnabled(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "setMdnsEnabled(handle, enabled) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    bool enabled = false;
    napi_get_value_bool(env, args[1], &enabled);
    napi_value result;
    bool ok = false;
    if (g_nim.mdnsSetEnabled && handle) {
        ok = g_nim.mdnsSetEnabled(handle, enabled);
        OH_LOG_Print(LOG_APP, LOG_INFO, BRIDGE_LOG_DOMAIN, BRIDGE_LOG_TAG,
                     "napiSetMdnsEnabled handle=%{public}p enabled=%{public}d result=%{public}d",
                     handle, enabled ? 1 : 0, ok ? 1 : 0);
    }
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiRegisterPeerHints(napi_env env, napi_callback_info info)
{
    size_t argc = 4;
    napi_value args[4];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 3) {
        napiThrowError(env, "registerPeerHints(handle, peerId, addresses[, source]) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string peerId = napiValueToString(env, args[1]);
    std::string addressesJson;
    napi_valuetype argType = napi_undefined;
    napi_typeof(env, args[2], &argType);
    if (argType == napi_string) {
        addressesJson = napiValueToString(env, args[2]);
    } else if (argType == napi_object) {
        auto addressesOpt = napiArrayToStringVector(env, args[2]);
        if (addressesOpt.has_value() && !addressesOpt->empty()) {
            addressesJson = jsonEncodeStringArray(addressesOpt.value());
        }
    }
    std::string source;
    bool hasSource = false;
    if (argc >= 4) {
        source = napiValueToString(env, args[3]);
        hasSource = !source.empty();
    }
    bool ok = false;
    if (g_nim.registerPeerHints && handle != nullptr && !peerId.empty() && !addressesJson.empty()) {
        const char *sourcePtr = hasSource ? source.c_str() : nullptr;
        ok = g_nim.registerPeerHints(handle, peerId.c_str(), addressesJson.c_str(), sourcePtr);
    }
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiAddExternalAddress(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 2) {
        napiThrowError(env, "addExternalAddress(handle, multiaddr) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    std::string multiaddr = napiValueToString(env, args[1]);
    bool ok = false;
    if (g_nim.addExternalAddress && handle != nullptr && !multiaddr.empty()) {
        ok = g_nim.addExternalAddress(handle, multiaddr.c_str());
    }
    napi_value result;
    napiSetBool(env, ok, &result);
    return result;
}

napi_value napiGenerateIdentity(napi_env env, napi_callback_info info)
{
    (void)info;
    if (!ensureLibraryLoaded()) {
        napiThrowError(env, "Failed to load libnimlibp2p.so");
        return nullptr;
    }
    if (!g_nim.generateIdentity) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    ThreadScope scope;
    char *json = g_nim.generateIdentity();
    return wrapString(env, json);
}

napi_value napiIdentityFromSeed(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "identityFromSeed(seed) required");
        return nullptr;
    }
    if (!ensureLibraryLoaded() || !g_nim.identityFromSeed) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    ThreadScope scope;
    std::string seed = napiValueToString(env, args[0]);
    if (seed.empty()) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    const unsigned char *bytes = reinterpret_cast<const unsigned char *>(seed.data());
    char *json = g_nim.identityFromSeed(bytes, seed.size());
    return wrapString(env, json);
}

napi_value napiGetLastError(napi_env env, napi_callback_info info)
{
    if (!ensureLibraryLoaded() || !g_nim.getLastError) {
        napi_value nullValue;
        napi_get_null(env, &nullValue);
        return nullValue;
    }
    ThreadScope scope;
    char *msg = g_nim.getLastError();
    return wrapString(env, msg);
}

napi_value napiPollEvents(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
    if (argc < 1) {
        napiThrowError(env, "pollEvents(handle[, limit]) required");
        return nullptr;
    }
    ThreadScope scope;
    void *handle = getNodeHandle(env, args[0]);
    int32_t limit = 0;
    if (argc >= 2) {
        napi_get_value_int32(env, args[1], &limit);
    }
    std::string payload = "[]";
    if (ensureLibraryLoaded() && g_nim.pollEvents && handle != nullptr) {
        char *raw = g_nim.pollEvents(handle, static_cast<int>(limit));
        if (raw != nullptr) {
            payload.assign(raw);
            if (g_nim.stringFree) {
                g_nim.stringFree(raw);
            }
        }
    }
    napi_value result;
    napi_create_string_utf8(env, payload.c_str(), payload.size(), &result);
    return result;
}

napi_value initModule(napi_env env, napi_value exports)
{
    napi_property_descriptor descriptors[] = {
        {"initNode", nullptr, napiNodeInit, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"nodeInit", nullptr, napiNodeInit, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"startNode", nullptr, napiNodeStart, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"nodeStart", nullptr, napiNodeStart, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"stopNode", nullptr, napiNodeStop, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"nodeStop", nullptr, napiNodeStop, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"freeNode", nullptr, napiNodeFree, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"nodeFree", nullptr, napiNodeFree, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"nodeIsStarted", nullptr, napiNodeIsStarted, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"listenAddresses", nullptr, napiGetListenAddresses, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getListenAddresses", nullptr, napiGetListenAddresses, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getDialableAddresses", nullptr, napiGetDialableAddresses, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"localPeerId", nullptr, napiGetLocalPeerId, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"initializeConnEvents", nullptr, napiInitializeConnEvents, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getBootstrapStatus", nullptr, napiGetBootstrapStatus, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getDiagnostics", nullptr, napiGetDiagnostics, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getConnectedPeers", nullptr, napiGetConnectedPeers, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getConnectedPeersInfo", nullptr, napiGetConnectedPeersInfo, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getPeerMultiaddrs", nullptr, napiGetPeerMultiaddrs, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"lanEndpoints", nullptr, napiGetLanEndpoints, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"lanEndpointsAsync", nullptr, napiGetLanEndpointsAsync, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getLanEndpoints", nullptr, napiGetLanEndpoints, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"fetchFeedSnapshot", nullptr, napiFetchFeedSnapshot, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"publishFeed", nullptr, napiPublishFeedEntry, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"subscribeFeedPeer", nullptr, napiSubscribeFeedPeer, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"unsubscribeFeedPeer", nullptr, napiUnsubscribeFeedPeer, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"publishFeedEntry", nullptr, napiPublishFeedEntry, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"syncPeerstoreState", nullptr, napiSyncPeerstoreState, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"loadStoredPeers", nullptr, napiLoadStoredPeers, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getLastDirectError", nullptr, napiGetLastDirectError, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"lastDirectError", nullptr, napiGetLastDirectError, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"refreshPeerConnections", nullptr, napiRefreshPeerConnections, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"connectPeer", nullptr, napiConnectPeer, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"connectPeerAsync", nullptr, napiConnectPeerAsync, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"disconnectPeer", nullptr, napiDisconnectPeer, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"connectMultiaddr", nullptr, napiConnectMultiaddr, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"connectMultiaddrAsync", nullptr, napiConnectMultiaddrAsync, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"sendDirect", nullptr, napiSendDirect, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"sendDirectAsync", nullptr, napiSendDirectAsync, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"keepAlivePin", nullptr, napiKeepAlivePin, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"keepAliveUnpin", nullptr, napiKeepAliveUnpin, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"isPeerConnected", nullptr, napiIsPeerConnected, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"sendDirectText", nullptr, napiSendDirectText, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"sendChatControl", nullptr, napiSendChatControl, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"sendChatAck", nullptr, napiSendChatAck, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"sendWithAck", nullptr, napiSendWithAck, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"waitSecureChannel", nullptr, napiWaitSecureChannel, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"upsertLivestreamConfig", nullptr, napiUpsertLivestreamConfig, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"publishLivestreamFrame", nullptr, napiPublishLivestreamFrame, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"publishTopic", nullptr, napiPublishBroadcast, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"subscribeTopic", nullptr, napiSubscribeTopic, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"unsubscribeTopic", nullptr, napiUnsubscribeTopic, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"boostConnectivity", nullptr, napiBoostConnectivity, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"reconnectBootstrap", nullptr, napiReconnectBootstrap, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"setMdnsInterval", nullptr, napiSetMdnsInterval, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"setMdnsInterface", nullptr, napiSetMdnsInterface, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"mdnsDebug", nullptr, napiMdnsDebug, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"mdnsProbe", nullptr, napiMdnsProbe, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"setMdnsEnabled", nullptr, napiSetMdnsEnabled, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"generateIdentity", nullptr, napiGenerateIdentity, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"identityFromSeed", nullptr, napiIdentityFromSeed, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"registerPeerHints", nullptr, napiRegisterPeerHints, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"addExternalAddress", nullptr, napiAddExternalAddress, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"upsertLivestreamConfig", nullptr, napiUpsertLivestreamConfig, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"upsertLivestream", nullptr, napiUpsertLivestreamConfig, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"getLastError", nullptr, napiGetLastError, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"lastInitError", nullptr, napiGetLastError, nullptr, nullptr, nullptr, napi_default, nullptr},
        {"pollEvents", nullptr, napiPollEvents, nullptr, nullptr, nullptr, napi_default, nullptr}
    };
    napi_define_properties(env, exports, sizeof(descriptors) / sizeof(descriptors[0]), descriptors);
    return exports;
}

} // namespace

EXTERN_C_START
static napi_value RegisterModule(napi_env env, napi_value exports)
{
    return initModule(env, exports);
}

static napi_module g_module = {
    .nm_version = 1,
    .nm_flags = 0,
    .nm_filename = nullptr,
    .nm_register_func = RegisterModule,
    .nm_modname = "nimbridge",
    .nm_priv = nullptr,
    .reserved = {0}
};

__attribute__((constructor)) static void RegisterNimBridgeModule(void)
{
    napi_module_register(&g_module);
}
EXTERN_C_END

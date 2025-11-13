#include <jni.h>
#include <android/log.h>
#include <string>
#include <vector>
#include <cstdint>

extern "C" {

void nim_thread_attach();
void nim_thread_detach();

const char *libp2p_get_last_error();
void *libp2p_node_init(const char *configJson);
int libp2p_node_start(void *handle);
int libp2p_node_stop(void *handle);
void libp2p_node_free(void *handle);
bool libp2p_node_is_started(void *handle);

const char *libp2p_poll_events(void *handle, int maxEvents);
const char *libp2p_get_lan_endpoints_json(void *handle);
const char *libp2p_get_listen_addresses(void *handle);
const char *libp2p_get_dialable_addresses(void *handle);
const char *libp2p_get_connected_peers_json(void *handle);
const char *libp2p_connected_peers_info(void *handle);
const char *libp2p_get_local_peer_id(void *handle);
const char *libp2p_fetch_feed_snapshot(void *handle);
const char *libp2p_get_last_direct_error(void *handle);
const char *libp2p_identity_from_seed(const uint8_t *seed, size_t seedLen);
bool libp2p_register_peer_hints(void *handle, const char *peerId, const char *addressesJson, const char *source);
bool libp2p_add_external_address(void *handle, const char *multiaddr);
bool libp2p_is_peer_connected(void *handle, const char *peerId);

bool libp2p_mdns_set_enabled(void *handle, bool enabled);
bool libp2p_mdns_set_interface(void *handle, const char *ipv4);
bool libp2p_mdns_probe(void *handle);

int libp2p_connect_peer(void *handle, const char *peerId);
int libp2p_connect_multiaddr(void *handle, const char *multiaddr);
bool libp2p_send_dm_payload(void *handle, const char *peerId, const uint8_t *payload, size_t payloadLen);

bool libp2p_feed_publish_entry(void *handle, const char *jsonPayload);

bool libp2p_upsert_livestream_cfg(void *handle, const char *streamKey, const char *configJson);
bool libp2p_publish_livestream_frame_ffi(void *handle, const char *streamKey, const uint8_t *payload, size_t payloadLen);

void libp2p_string_free(const char *value);

void nim_bridge_emit_event(const char *topic, const char *payload);

bool libp2p_initialize_conn_events(void *handle);

} // extern "C"

namespace {

constexpr const char *kLogTag = "NimBridge";

void logInfo(const char *message) {
  __android_log_print(ANDROID_LOG_INFO, kLogTag, "%s", message);
}

void logError(const char *message) {
  __android_log_print(ANDROID_LOG_ERROR, kLogTag, "%s", message);
}

inline void ensureThreadAttached() {
  nim_thread_attach();
}

inline std::string jstringToUtf8(JNIEnv *env, jstring value) {
  if (value == nullptr) {
    return {};
  }
  const char *chars = env->GetStringUTFChars(value, nullptr);
  if (chars == nullptr) {
    return {};
  }
  std::string result(chars);
  env->ReleaseStringUTFChars(value, chars);
  return result;
}

inline jstring toJString(JNIEnv *env, const std::string &value) {
  return env->NewStringUTF(value.c_str());
}

inline jstring copyAndFree(JNIEnv *env, const char *raw) {
  if (raw == nullptr) {
    return nullptr;
  }
  std::string buffer(raw);
  libp2p_string_free(raw);
  return toJString(env, buffer);
}

inline std::vector<uint8_t> jbyteArrayToVector(JNIEnv *env, jbyteArray array) {
  std::vector<uint8_t> result;
  if (array == nullptr) {
    return result;
  }
  const jsize length = env->GetArrayLength(array);
  result.resize(static_cast<size_t>(length));
  env->GetByteArrayRegion(array, 0, length, reinterpret_cast<jbyte *>(result.data()));
  return result;
}

inline void *toHandle(jlong handle) {
  return reinterpret_cast<void *>(handle);
}

} // namespace

extern "C" void nim_bridge_emit_event(const char *topic, const char *payload) {
  if (topic == nullptr || payload == nullptr) {
    return;
  }
  __android_log_print(ANDROID_LOG_DEBUG, kLogTag, "[event] %s %s", topic, payload);
}

extern "C" JNIEXPORT jlong JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeInit(JNIEnv *env, jclass,
                                                         jstring configJson) {
  ensureThreadAttached();
  const std::string config = jstringToUtf8(env, configJson);
  void *handle = libp2p_node_init(config.empty() ? nullptr : config.c_str());
  return reinterpret_cast<jlong>(handle);
}

extern "C" JNIEXPORT jint JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeStart(JNIEnv *, jclass,
                                                          jlong handle) {
  ensureThreadAttached();
  return static_cast<jint>(libp2p_node_start(toHandle(handle)));
}

extern "C" JNIEXPORT jint JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeStop(JNIEnv *, jclass,
                                                         jlong handle) {
  ensureThreadAttached();
  return static_cast<jint>(libp2p_node_stop(toHandle(handle)));
}

extern "C" JNIEXPORT void JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeFree(JNIEnv *, jclass,
                                                         jlong handle) {
  ensureThreadAttached();
  libp2p_node_free(toHandle(handle));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeIsStarted(JNIEnv *, jclass,
                                                              jlong handle) {
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_node_is_started(toHandle(handle)));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativePollEvents(JNIEnv *env,
                                                               jclass,
                                                               jlong handle,
                                                               jint maxEvents) {
  ensureThreadAttached();
  const char *raw = libp2p_poll_events(toHandle(handle), static_cast<int>(maxEvents));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetLanEndpoints(JNIEnv *env,
                                                                    jclass,
                                                                    jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_get_lan_endpoints_json(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeIdentityFromSeed(JNIEnv *env,
                                                                     jclass,
                                                                     jbyteArray seedArray) {
  ensureThreadAttached();
  if (seedArray == nullptr) {
    return nullptr;
  }
  const jsize length = env->GetArrayLength(seedArray);
  if (length <= 0) {
    return nullptr;
  }
  std::vector<uint8_t> seed(static_cast<size_t>(length));
  env->GetByteArrayRegion(seedArray, 0, length,
                          reinterpret_cast<jbyte *>(seed.data()));
  const char *raw =
      libp2p_identity_from_seed(seed.data(), static_cast<size_t>(seed.size()));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetListenAddresses(JNIEnv *env,
                                                                       jclass,
                                                                       jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_get_listen_addresses(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetDialableAddresses(JNIEnv *env,
                                                                         jclass,
                                                                         jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_get_dialable_addresses(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetConnectedPeers(JNIEnv *env,
                                                                      jclass,
                                                                      jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_get_connected_peers_json(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetConnectedPeersInfo(JNIEnv *env,
                                                                          jclass,
                                                                          jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_connected_peers_info(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetLocalPeerId(JNIEnv *env,
                                                                   jclass,
                                                                   jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_get_local_peer_id(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeSetMdnsEnabled(JNIEnv *, jclass,
                                                                   jlong handle,
                                                                   jboolean enabled) {
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_mdns_set_enabled(toHandle(handle), enabled));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeSetMdnsInterface(JNIEnv *env,
                                                                     jclass,
                                                                     jlong handle,
                                                                     jstring ipv4) {
  ensureThreadAttached();
  const std::string ip = jstringToUtf8(env, ipv4);
  return static_cast<jboolean>(
      libp2p_mdns_set_interface(toHandle(handle), ip.empty() ? nullptr : ip.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeMdnsProbe(JNIEnv *, jclass,
                                                              jlong handle) {
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_mdns_probe(toHandle(handle)));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeInitializeConnEvents(JNIEnv *, jclass,
                                                                         jlong handle) {
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_initialize_conn_events(toHandle(handle)));
}

extern "C" JNIEXPORT jint JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeConnectPeer(JNIEnv *env,
                                                                jclass,
                                                                jlong handle,
                                                                jstring peerId) {
  ensureThreadAttached();
  const std::string peer = jstringToUtf8(env, peerId);
  return static_cast<jint>(
      libp2p_connect_peer(toHandle(handle), peer.empty() ? nullptr : peer.c_str()));
}

extern "C" JNIEXPORT jint JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeConnectMultiaddr(JNIEnv *env,
                                                                     jclass,
                                                                     jlong handle,
                                                                     jstring multiaddr) {
  ensureThreadAttached();
  const std::string addr = jstringToUtf8(env, multiaddr);
  return static_cast<jint>(
      libp2p_connect_multiaddr(toHandle(handle), addr.empty() ? nullptr : addr.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeSendDirect(JNIEnv *env,
                                                               jclass,
                                                               jlong handle,
                                                               jstring peerId,
                                                               jbyteArray payload) {
  ensureThreadAttached();
  const std::string peer = jstringToUtf8(env, peerId);
  auto data = jbyteArrayToVector(env, payload);
  if (peer.empty() || data.empty()) {
    return JNI_FALSE;
  }
  return static_cast<jboolean>(
      libp2p_send_dm_payload(
          toHandle(handle),
          peer.c_str(),
          reinterpret_cast<const uint8_t *>(data.data()),
          data.size()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeRegisterPeerHints(JNIEnv *env,
                                                                      jclass,
                                                                      jlong handle,
                                                                      jstring peerId,
                                                                      jstring addressesJson,
                                                                      jstring source) {
  ensureThreadAttached();
  const std::string peer = jstringToUtf8(env, peerId);
  const std::string addresses = jstringToUtf8(env, addressesJson);
  const std::string src = jstringToUtf8(env, source);
  if (peer.empty() || addresses.empty()) {
    return JNI_FALSE;
  }
  const char *sourcePtr = src.empty() ? nullptr : src.c_str();
  return static_cast<jboolean>(
      libp2p_register_peer_hints(
          toHandle(handle),
          peer.c_str(),
          addresses.c_str(),
          sourcePtr));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeAddExternalAddress(JNIEnv *env,
                                                                       jclass,
                                                                       jlong handle,
                                                                       jstring multiaddr) {
  ensureThreadAttached();
  const std::string addr = jstringToUtf8(env, multiaddr);
  if (addr.empty()) {
    return JNI_FALSE;
  }
  return static_cast<jboolean>(
      libp2p_add_external_address(toHandle(handle), addr.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeIsPeerConnected(JNIEnv *env,
                                                                    jclass,
                                                                    jlong handle,
                                                                    jstring peerId) {
  ensureThreadAttached();
  const std::string peer = jstringToUtf8(env, peerId);
  if (peer.empty()) {
    return JNI_FALSE;
  }
  return static_cast<jboolean>(
      libp2p_is_peer_connected(toHandle(handle), peer.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativePublishFeed(JNIEnv *env,
                                                                jclass,
                                                                jlong handle,
                                                                jstring jsonPayload) {
  ensureThreadAttached();
  const std::string payload = jstringToUtf8(env, jsonPayload);
  if (payload.empty()) {
    return JNI_FALSE;
  }
  return static_cast<jboolean>(
      libp2p_feed_publish_entry(toHandle(handle), payload.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeFetchFeedSnapshot(JNIEnv *env,
                                                                      jclass,
                                                                      jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_fetch_feed_snapshot(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeUpsertLivestream(JNIEnv *env,
                                                                     jclass,
                                                                     jlong handle,
                                                                     jstring streamKey,
                                                                     jstring configJson) {
  ensureThreadAttached();
  const std::string key = jstringToUtf8(env, streamKey);
  const std::string config = jstringToUtf8(env, configJson);
  if (key.empty()) {
    return JNI_FALSE;
  }
  return static_cast<jboolean>(
      libp2p_upsert_livestream_cfg(
          toHandle(handle),
          key.c_str(),
          config.empty() ? nullptr : config.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativePublishLivestreamFrame(JNIEnv *env,
                                                                           jclass,
                                                                           jlong handle,
                                                                           jstring streamKey,
                                                                           jbyteArray payload) {
  ensureThreadAttached();
  const std::string key = jstringToUtf8(env, streamKey);
  auto data = jbyteArrayToVector(env, payload);
  if (key.empty() || data.empty()) {
    return JNI_FALSE;
  }
  return static_cast<jboolean>(
      libp2p_publish_livestream_frame_ffi(
          toHandle(handle),
          key.c_str(),
          reinterpret_cast<const uint8_t *>(data.data()),
          data.size()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetLastDirectError(JNIEnv *env,
                                                                       jclass,
                                                                       jlong handle) {
  ensureThreadAttached();
  const char *raw = libp2p_get_last_direct_error(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetLastInitError(JNIEnv *env,
                                                                     jclass) {
  ensureThreadAttached();
  const char *raw = libp2p_get_last_error();
  return copyAndFree(env, raw);
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *) {
  JNIEnv *env = nullptr;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6) != JNI_OK) {
    logError("Failed to get JNI environment");
    return JNI_ERR;
  }
  logInfo("nimbridge JNI loaded");
  ensureThreadAttached();
  return JNI_VERSION_1_6;
}

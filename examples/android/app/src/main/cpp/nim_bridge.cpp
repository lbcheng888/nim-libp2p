#include <jni.h>
#include <android/log.h>
#include <string>
#include <vector>
#include <cstdint>
#include <mutex>

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

bool libp2p_dex_init(void *handle, const char *configJson);
bool libp2p_dex_submit_order(void *handle, const char *orderJson);
bool libp2p_dex_submit_mixer_intent(void *handle, const char *intentJson);
const char *libp2p_dex_get_market_data(void *handle, const char *asset);

// Adapter Signatures
const char *libp2p_adapter_generate_secret();
const char *libp2p_adapter_compute_payment_point(const char *secret);
const char *libp2p_adapter_sign(const char *msg, const char *privKey, const char *point);
    const char* libp2p_adapter_complete_sig(const char* adaptorSig, const char* secret);
    const char* libp2p_adapter_extract_secret(const char* adaptorSig, const char* validSig);
    
    const char* libp2p_mpc_keygen_init();
    const char* libp2p_mpc_keygen_finalize(const char* localPub, const char* remotePub);
    const char* libp2p_mpc_sign_init();
    const char* libp2p_mpc_sign_partial(const char* msg, const char* secret, const char* nonce, const char* jointPub, const char* jointNoncePub);
    const char* libp2p_mpc_sign_combine(const char* s1, const char* s2, const char* jointNoncePub);

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
  // Nim's GC foreign-thread setup is only safe to run once per thread.
  // JNI calls can happen on pooled threads (Dispatchers.IO / Default), so guard it.
  thread_local bool nimAttached = false;
  if (!nimAttached) {
    nim_thread_attach();
    nimAttached = true;
  }
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


static JavaVM *g_vm = nullptr;
static std::mutex g_bridge_mutex;
static std::mutex g_nim_mutex;
static jclass g_bridge_class = nullptr;      // Global ref
static jmethodID g_on_native_event = nullptr;

static void cacheBridgeClass(JNIEnv *env, jclass bridgeClass) {
  if (env == nullptr || bridgeClass == nullptr) {
    return;
  }

  std::lock_guard<std::mutex> guard(g_bridge_mutex);
  if (g_bridge_class != nullptr && g_on_native_event != nullptr) {
    return;
  }

  jclass globalClass = reinterpret_cast<jclass>(env->NewGlobalRef(bridgeClass));
  if (globalClass == nullptr) {
    if (env->ExceptionCheck()) {
      env->ExceptionClear();
    }
    __android_log_print(ANDROID_LOG_ERROR, kLogTag, "Failed to cache NimBridge global ref");
    return;
  }

  jmethodID methodId = env->GetStaticMethodID(
      globalClass,
      "onNativeEvent",
      "(Ljava/lang/String;Ljava/lang/String;)V");
  if (methodId == nullptr) {
    if (env->ExceptionCheck()) {
      env->ExceptionClear();
    }
    env->DeleteGlobalRef(globalClass);
    __android_log_print(ANDROID_LOG_ERROR, kLogTag, "Failed to cache NimBridge.onNativeEvent");
    return;
  }

  g_bridge_class = globalClass;
  g_on_native_event = methodId;
}

extern "C" void nim_bridge_emit_event(const char *topic, const char *payload) {
  if (topic == nullptr || payload == nullptr || g_vm == nullptr) {
    return;
  }

  JNIEnv *env = nullptr;
  bool attached = false;
  int status = g_vm->GetEnv((void **)&env, JNI_VERSION_1_6);
  if (status == JNI_EDETACHED) {
    if (g_vm->AttachCurrentThread(&env, nullptr) != 0) {
      __android_log_print(ANDROID_LOG_ERROR, kLogTag, "Failed to attach thread for event");
      return;
    }
    attached = true;
  } else if (status != JNI_OK) {
	  __android_log_print(ANDROID_LOG_ERROR, kLogTag, "Failed to get JNIEnv");
	  return;
	}

  jclass bridgeClass = nullptr;
  jmethodID methodId = nullptr;
  {
    std::lock_guard<std::mutex> guard(g_bridge_mutex);
    bridgeClass = g_bridge_class;
    methodId = g_on_native_event;
  }

  if (bridgeClass != nullptr && methodId != nullptr) {
    jstring jTopic = env->NewStringUTF(topic);
    jstring jPayload = env->NewStringUTF(payload);
    env->CallStaticVoidMethod(bridgeClass, methodId, jTopic, jPayload);
    env->DeleteLocalRef(jTopic);
    env->DeleteLocalRef(jPayload);
    if (env->ExceptionCheck()) {
      // Never let callback exceptions crash native-attached threads.
      env->ExceptionClear();
      __android_log_print(ANDROID_LOG_ERROR, kLogTag, "Exception while delivering onNativeEvent");
    }
  }

  if (attached) {
    g_vm->DetachCurrentThread();
  }
}

extern "C" JNIEXPORT jlong JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeInit(JNIEnv *env, jclass clazz,
                                                         jstring configJson) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  cacheBridgeClass(env, clazz);
  const std::string config = jstringToUtf8(env, configJson);
  void *handle = libp2p_node_init(config.empty() ? nullptr : config.c_str());
  return reinterpret_cast<jlong>(handle);
}

extern "C" JNIEXPORT jint JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeStart(JNIEnv *, jclass,
                                                          jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return static_cast<jint>(libp2p_node_start(toHandle(handle)));
}

extern "C" JNIEXPORT jint JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeStop(JNIEnv *, jclass,
                                                         jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return static_cast<jint>(libp2p_node_stop(toHandle(handle)));
}

extern "C" JNIEXPORT void JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeFree(JNIEnv *, jclass,
                                                         jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  libp2p_node_free(toHandle(handle));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeIsStarted(JNIEnv *, jclass,
                                                              jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_node_is_started(toHandle(handle)));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativePollEvents(JNIEnv *env,
                                                               jclass,
                                                               jlong handle,
                                                               jint maxEvents) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_poll_events(toHandle(handle), static_cast<int>(maxEvents));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetLanEndpoints(JNIEnv *env,
                                                                    jclass,
                                                                    jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_get_lan_endpoints_json(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeIdentityFromSeed(JNIEnv *env,
                                                                     jclass,
                                                                     jbyteArray seedArray) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_get_listen_addresses(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetDialableAddresses(JNIEnv *env,
                                                                         jclass,
                                                                         jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_get_dialable_addresses(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetConnectedPeers(JNIEnv *env,
                                                                      jclass,
                                                                      jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_get_connected_peers_json(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetConnectedPeersInfo(JNIEnv *env,
                                                                          jclass,
                                                                          jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_connected_peers_info(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetLocalPeerId(JNIEnv *env,
                                                                   jclass,
                                                                   jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_get_local_peer_id(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeSetMdnsEnabled(JNIEnv *, jclass,
                                                                   jlong handle,
                                                                   jboolean enabled) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_mdns_set_enabled(toHandle(handle), enabled));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeSetMdnsInterface(JNIEnv *env,
                                                                     jclass,
                                                                     jlong handle,
                                                                     jstring ipv4) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string ip = jstringToUtf8(env, ipv4);
  return static_cast<jboolean>(
      libp2p_mdns_set_interface(toHandle(handle), ip.empty() ? nullptr : ip.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeMdnsProbe(JNIEnv *, jclass,
                                                              jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_mdns_probe(toHandle(handle)));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeInitializeConnEvents(JNIEnv *, jclass,
                                                                         jlong handle) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return static_cast<jboolean>(libp2p_initialize_conn_events(toHandle(handle)));
}

extern "C" JNIEXPORT jint JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeConnectPeer(JNIEnv *env,
                                                                jclass,
                                                                jlong handle,
                                                                jstring peerId) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
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
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_get_last_direct_error(toHandle(handle));
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeGetLastInitError(JNIEnv *env,
                                                                     jclass) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const char *raw = libp2p_get_last_error();
  return copyAndFree(env, raw);
}

extern "C" void NimMain();

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *) {
  g_vm = vm;
  JNIEnv *env = nullptr;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6) != JNI_OK) {
    logError("Failed to get JNI environment");
    return JNI_ERR;
  }
  logInfo("nimbridge JNI loaded");

  // Cache NimBridge callback target while we're still on the Java loader thread.
  jclass bridgeClass = env->FindClass("com/example/libp2psmoke/native/NimBridge");
  if (bridgeClass != nullptr) {
    cacheBridgeClass(env, bridgeClass);
    env->DeleteLocalRef(bridgeClass);
  } else if (env->ExceptionCheck()) {
    env->ExceptionClear();
  }
  
  // Initialize Nim Main (Global Init)
  {
    std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
    NimMain();
    ensureThreadAttached();
  }
  return JNI_VERSION_1_6;
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeDexInit(JNIEnv *env,
                                                            jclass,
                                                            jlong handle,
                                                            jstring configJson) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string config = jstringToUtf8(env, configJson);
  return static_cast<jboolean>(
      libp2p_dex_init(toHandle(handle), config.empty() ? nullptr : config.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeDexSubmitOrder(JNIEnv *env,
                                                                   jclass,
                                                                   jlong handle,
                                                                   jstring orderJson) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string order = jstringToUtf8(env, orderJson);
  return static_cast<jboolean>(
      libp2p_dex_submit_order(toHandle(handle), order.empty() ? nullptr : order.c_str()));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeDexSubmitMixerIntent(JNIEnv *env,
                                                                         jclass,
                                                                         jlong handle,
                                                                         jstring intentJson) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string intent = jstringToUtf8(env, intentJson);
  return static_cast<jboolean>(
      libp2p_dex_submit_mixer_intent(toHandle(handle), intent.empty() ? nullptr : intent.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeDexGetMarketData(JNIEnv *env,
                                                                     jclass,
                                                                     jlong handle,
                                                                     jstring asset) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string assetStr = jstringToUtf8(env, asset);
  const char *raw = libp2p_dex_get_market_data(toHandle(handle), assetStr.empty() ? nullptr : assetStr.c_str());
  return copyAndFree(env, raw);
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeAdapterGenerateSecret(JNIEnv *env, jclass) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return copyAndFree(env, libp2p_adapter_generate_secret());
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeAdapterComputePaymentPoint(JNIEnv *env, jclass, jstring secret) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string s = jstringToUtf8(env, secret);
  return copyAndFree(env, libp2p_adapter_compute_payment_point(s.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeAdapterSign(JNIEnv *env, jclass, jstring msg, jstring privKey, jstring point) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string m = jstringToUtf8(env, msg);
  const std::string k = jstringToUtf8(env, privKey);
  const std::string p = jstringToUtf8(env, point);
  return copyAndFree(env, libp2p_adapter_sign(m.c_str(), k.c_str(), p.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeAdapterCompleteSig(JNIEnv *env, jclass, jstring adaptorSig, jstring secret) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string a = jstringToUtf8(env, adaptorSig);
  const std::string s = jstringToUtf8(env, secret);
  return copyAndFree(env, libp2p_adapter_complete_sig(a.c_str(), s.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeAdapterExtractSecret(JNIEnv *env, jclass, jstring adaptorSig, jstring validSig) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string a = jstringToUtf8(env, adaptorSig);
  const std::string v = jstringToUtf8(env, validSig);
  return copyAndFree(env, libp2p_adapter_extract_secret(a.c_str(), v.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeMpcKeygenInit(JNIEnv *env, jclass) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return copyAndFree(env, libp2p_mpc_keygen_init());
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeMpcKeygenFinalize(JNIEnv *env, jclass, jstring localPub, jstring remotePub) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string l = jstringToUtf8(env, localPub);
  const std::string r = jstringToUtf8(env, remotePub);
  return copyAndFree(env, libp2p_mpc_keygen_finalize(l.c_str(), r.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeMpcSignInit(JNIEnv *env, jclass) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  return copyAndFree(env, libp2p_mpc_sign_init());
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeMpcSignPartial(JNIEnv *env, jclass, jstring msg, jstring secret, jstring nonce, jstring jointPub, jstring jointNoncePub) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string m = jstringToUtf8(env, msg);
  const std::string s = jstringToUtf8(env, secret);
  const std::string n = jstringToUtf8(env, nonce);
  const std::string jp = jstringToUtf8(env, jointPub);
  const std::string jnp = jstringToUtf8(env, jointNoncePub);
  return copyAndFree(env, libp2p_mpc_sign_partial(m.c_str(), s.c_str(), n.c_str(), jp.c_str(), jnp.c_str()));
}

extern "C" JNIEXPORT jstring JNICALL
Java_com_example_libp2psmoke_native_NimBridge_nativeMpcSignCombine(JNIEnv *env, jclass, jstring s1, jstring s2, jstring jointNoncePub) {
  std::lock_guard<std::mutex> nimGuard(g_nim_mutex);
  ensureThreadAttached();
  const std::string sig1 = jstringToUtf8(env, s1);
  const std::string sig2 = jstringToUtf8(env, s2);
  const std::string jnp = jstringToUtf8(env, jointNoncePub);
  return copyAndFree(env, libp2p_mpc_sign_combine(sig1.c_str(), sig2.c_str(), jnp.c_str()));
}

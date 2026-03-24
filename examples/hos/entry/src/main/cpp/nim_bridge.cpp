#include <hilog/log.h>

extern "C" void nim_bridge_emit_event(const char *topic, const char *payload) {
  const char *safeTopic = topic != nullptr ? topic : "";
  const char *safePayload = payload != nullptr ? payload : "";
  OH_LOG_Print(
      LOG_APP,
      LOG_INFO,
      0x0000,
      "NIM-BRIDGE",
      "topic=%{public}s payload=%{public}s",
      safeTopic,
      safePayload);
}

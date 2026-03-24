package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"
)

var (
	handlesMu  sync.Mutex
	nextHandle int64 = 1
	handles          = map[int64]*bridge{}
)

func cString(value *C.char) string {
	if value == nil {
		return ""
	}
	return C.GoString(value)
}

func toCString(payload any) *C.char {
	encoded, err := json.Marshal(payload)
	if err != nil {
		encoded = []byte(fmt.Sprintf(`{"ok":false,"error":"json_marshal:%s"}`, err.Error()))
	}
	return C.CString(string(encoded))
}

func withBridge(handle C.longlong, fn func(*bridge) any) *C.char {
	handlesMu.Lock()
	instance := handles[int64(handle)]
	handlesMu.Unlock()
	if instance == nil {
		return toCString(responseEnvelope{OK: false, Error: "invalid_bridge_handle"})
	}
	return toCString(fn(instance))
}

//export TsnetBridgeCreate
func TsnetBridgeCreate(configJSON *C.char) C.longlong {
	cfg := bridgeConfig{}
	if raw := cString(configJSON); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return 0
		}
	}
	instance := newBridge(cfg)
	handlesMu.Lock()
	defer handlesMu.Unlock()
	handle := nextHandle
	nextHandle++
	handles[handle] = instance
	return C.longlong(handle)
}

//export TsnetBridgeRelease
func TsnetBridgeRelease(handle C.longlong) {
	handlesMu.Lock()
	instance := handles[int64(handle)]
	delete(handles, int64(handle))
	handlesMu.Unlock()
	if instance != nil {
		_ = instance.close()
	}
}

//export TsnetBridgeReset
func TsnetBridgeReset(handle C.longlong) C.int {
	handlesMu.Lock()
	instance := handles[int64(handle)]
	handlesMu.Unlock()
	if instance == nil {
		return 0
	}
	if err := instance.reset(); err != nil {
		return 0
	}
	return 1
}

//export TsnetBridgeStatusJSON
func TsnetBridgeStatusJSON(handle C.longlong) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return instance.statusPayload()
	})
}

//export TsnetBridgePingJSON
func TsnetBridgePingJSON(handle C.longlong, requestJSON *C.char) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return instance.pingPayload(cString(requestJSON))
	})
}

//export TsnetBridgeDERPMapJSON
func TsnetBridgeDERPMapJSON(handle C.longlong) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return tailnetDERPMapPayload(instance.queryDERPMap())
	})
}

//export TsnetBridgeListenTCPProxyJSON
func TsnetBridgeListenTCPProxyJSON(handle C.longlong, requestJSON *C.char) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return instance.listenTCPProxy(cString(requestJSON))
	})
}

//export TsnetBridgeListenUDPProxyJSON
func TsnetBridgeListenUDPProxyJSON(handle C.longlong, requestJSON *C.char) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return instance.listenUDPProxy(cString(requestJSON))
	})
}

//export TsnetBridgeDialTCPProxyJSON
func TsnetBridgeDialTCPProxyJSON(handle C.longlong, requestJSON *C.char) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return instance.dialTCPProxy(cString(requestJSON))
	})
}

//export TsnetBridgeDialUDPProxyJSON
func TsnetBridgeDialUDPProxyJSON(handle C.longlong, requestJSON *C.char) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return instance.dialUDPProxy(cString(requestJSON))
	})
}

//export TsnetBridgeResolveRemoteJSON
func TsnetBridgeResolveRemoteJSON(handle C.longlong, requestJSON *C.char) *C.char {
	return withBridge(handle, func(instance *bridge) any {
		return instance.resolveRemotePayload(cString(requestJSON))
	})
}

//export TsnetBridgeStringFree
func TsnetBridgeStringFree(value *C.char) {
	if value != nil {
		C.free(unsafe.Pointer(value))
	}
}

func main() {}

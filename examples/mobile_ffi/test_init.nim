{.emit: """
void nim_bridge_emit_event(const char *topic, const char *payload) {
  (void)topic;
  (void)payload;
}
""".}

import std/[base64, json, os, strformat]
import libnimlibp2p

let dataDir = "/tmp/unimaker-test"
createDir(dataDir)
var privSeed = newSeq[byte](32)
# fill with incremental values
for i in 0..<privSeed.len:
  privSeed[i] = byte(i)
var publicKey = newSeq[byte](32)
for i in 0..<publicKey.len:
  publicKey[i] = byte(255 - i)
var privateCombined = newSeq[byte](64)
for i in 0..<32:
  privateCombined[i] = privSeed[i]
for i in 0..<32:
  privateCombined[32 + i] = publicKey[i]
let privateBase64 = base64.encode(privateCombined)
let publicBase64 = base64.encode(publicKey)
let peerId = "12D3KooWTestPeerIdPlaceholder"
let cfg = %*{
  "identity": {
    "privateKey": privateBase64,
    "publicKey": publicBase64,
    "peerId": peerId,
    "source": "test"
  },
  "dataDir": dataDir
}
let cfgStr = $cfg
echo "config=", cfgStr
let handle = libp2p_node_init(cfgStr.cstring)
if handle.isNil:
  let err = $cast[cstring](libp2p_get_last_error())
  echo "libp2p_node_init returned nil, err=", err
  quit(1)
echo &"libp2p_node_init handle=0x{cast[int](handle):x}"
let rcStart = libp2p_node_start(handle)
echo "start rc=", rcStart
if rcStart == 0:
  echo "node started"
else:
  echo "node start failed"
let rcStop = libp2p_node_stop(handle)
echo "stop rc=", rcStop
libp2p_node_free(handle)

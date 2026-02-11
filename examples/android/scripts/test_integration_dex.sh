#!/bin/bash
set -e

# Function to clean up background processes on exit
cleanup() {
  if [ -n "$DEX_PID" ]; then
    echo "Stopping dex_node (PID $DEX_PID)..."
    kill $DEX_PID || true
  fi
  rm -f temp_mobile_client temp_mobile_client.nim dex_node.log
}
trap cleanup EXIT

echo "Checking dependencies..."
if ! command -v nim &> /dev/null; then
    echo "nim not found"
    exit 1
fi

# Install chronicles if missing (using nimble)
if ! nimble path chronicles > /dev/null 2>&1; then
    echo "Installing chronicles..."
    nimble install -y chronicles
fi

echo "Building dex_node..."
# Use --path to ensure chronicles is found if installed via nimble in standard location
# Or rely on nimble's environment
nim c -d:debug --threads:on --mm:orc -d:chronicles_log_level=TRACE examples/dex/dex_node.nim

echo "Starting dex_node (Coordinator)..."
./examples/dex/dex_node --listen:/ip4/127.0.0.1/tcp/0 --enableMixer=true > dex_node.log 2>&1 &
DEX_PID=$!
echo "dex_node started with PID $DEX_PID"

# Wait for node to start and print logs if it fails early
sleep 5
if ! ps -p $DEX_PID > /dev/null; then
    echo "dex_node failed to start:"
    cat dex_node.log
    exit 1
fi

# Extract peer ID and multiaddr from log
PEER_ID=$(grep "Local PeerID" dex_node.log | awk -F'=' '{print $2}' | tr -d ' ')
PORT=$(grep "Listening on" dex_node.log | grep "/ip4/127.0.0.1/tcp/" | awk -F'/' '{print $5}' | head -n 1)

if [ -z "$PEER_ID" ] || [ -z "$PORT" ]; then
  echo "Failed to get PeerID or Port from dex_node.log"
  cat dex_node.log
  exit 1
fi

MULTIADDR="/ip4/127.0.0.1/tcp/$PORT/p2p/$PEER_ID"
echo "DEX Node Multiaddr: $MULTIADDR"

echo "Creating simulated mobile client..."
cat > temp_mobile_client.nim <<EOF
import ../../libp2p
import ../../libp2p/protocols/pubsub/gossipsub
import json, asyncdispatch, os, strformat

const MixerTopic = "unimaker-mixer-v1"

proc runClient() {.async.} =
  let switch = newStandardSwitch(
    listenAddrs = @[MultiAddress.init("/ip4/127.0.0.1/tcp/0").get()]
  )
  await switch.start()
  
  echo "Client connecting to $MULTIADDR"
  let remote = MultiAddress.init("$MULTIADDR").get()
  await switch.connect(remote)
  echo "Connected to DEX node"

  let pubsub = GossipSub.init(switch)
  switch.mount(pubsub)
  
  var settled = false
  
  proc handler(topic: string, data: seq[byte]) {.async.} =
    let msg = cast[string](data)
    echo "Received mixer msg: ", msg
    try:
      let json = parseJson(msg)
      if json.hasKey("settlement"):
        echo "Settlement received!"
        settled = true
    except:
      discard

  await pubsub.subscribe(MixerTopic, handler)

  # Simulate Android's "Mixer Intent" submission
  # Sending a JSON with kind="onion_intent" and a dummy onionPacket
  let intentJson = %*{
    "kind": "onion_intent",
    "requestId": "android-test-auto",
    "initiator": \$switch.peerInfo.peerId,
    "onionPacket": [1, 2, 3, 4, 5],
    "asset": "BTC",
    "amount": 1.0,
    "hops": 3
  }
  
  echo "Sending intent: ", \$intentJson
  await pubsub.publish(MixerTopic, \$intentJson)
  
  # Wait to ensure message is processed
  await sleepAsync(5000)
  
  await switch.stop()
  echo "Client finished."
  quit(0)

waitFor(runClient())
EOF

echo "Running simulated mobile client..."
nim c -d:debug --threads:on --mm:orc temp_mobile_client.nim
./temp_mobile_client

if [ $? -eq 0 ]; then
  echo "Integration test passed: Client connected and sent intent."
else
  echo "Integration test failed."
  exit 1
fi


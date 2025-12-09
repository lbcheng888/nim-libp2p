#!/usr/bin/env bash
set -e

# DEX/CoinJoin Demo Script
# Launches 3 nodes:
# - Node 1: Coordinator/Matcher (Mode: Matcher)
# - Node 2: Trader A (Participant)
# - Node 3: Trader B (Participant)

# Compile
echo "[*] Compiling dex_node with -d:libp2p_coinjoin..."
nim c -d:libp2p_coinjoin -d:chronicles_log_level=INFO -o:./dex_node examples/dex/dex_node.nim

# Cleanup
rm -rf data_coord data_trader1 data_trader2
mkdir -p data_coord data_trader1 data_trader2

# Start Coordinator
# Listens on 10001
echo "[*] Starting Coordinator (Node 1)..."
./dex_node --mode=matcher \
  --listen="/ip4/127.0.0.1/tcp/10001" \
  --data-dir=data_coord \
  --enable-mixer=true \
  --mixer-interval=10000 \
  --mixer-slots=2 \
  --enable-trades=false \
  > dex_coord.log 2>&1 &
PID_COORD=$!

sleep 2

# Get Coordinator Address
# We assume peer ID is generated or we just use the listen addr and let Identify handle it?
# Multiaddress needs PeerID for `connectToPeers` usually, but let's try dialing addr directly if supported, 
# OR grep the peer ID.
COORD_PEER_ID=$(grep "id=" dex_coord.log | head -n 1 | cut -d'=' -f2)
COORD_ADDR="/ip4/127.0.0.1/tcp/10001/p2p/$COORD_PEER_ID"
echo "[*] Coordinator Peer: $COORD_ADDR"

# Start Trader 1
echo "[*] Starting Trader 1..."
./dex_node --mode=trader \
  --listen="/ip4/127.0.0.1/tcp/10002" \
  --peer="$COORD_ADDR" \
  --data-dir=data_trader1 \
  --enable-mixer=true \
  --identity-label="user=alice" \
  > dex_trader1.log 2>&1 &
PID_T1=$!

# Start Trader 2
echo "[*] Starting Trader 2..."
./dex_node --mode=trader \
  --listen="/ip4/127.0.0.1/tcp/10003" \
  --peer="$COORD_ADDR" \
  --data-dir=data_trader2 \
  --enable-mixer=true \
  --identity-label="user=bob" \
  > dex_trader2.log 2>&1 &
PID_T2=$!

echo "[*] Nodes running. Waiting for Mixer session (approx 30s)..."
sleep 35

echo "[*] Checking logs..."

echo "--- Coordinator Logs (Mixer) ---"
grep "mixer" dex_coord.log | grep -v "gossip" | tail -n 10

echo "--- Trader 1 Logs (Mixer) ---"
grep "mixer" dex_trader1.log | grep -v "gossip" | tail -n 5

echo "--- Trader 2 Logs (Mixer) ---"
grep "mixer" dex_trader2.log | grep -v "gossip" | tail -n 5

# Check for Success
if grep -q "Session .* completed" dex_trader1.log; then
  echo "[SUCCESS] Trader 1 completed session!"
else
  echo "[FAIL] Trader 1 did not complete session."
fi

# Cleanup
kill $PID_COORD $PID_T1 $PID_T2
wait $PID_COORD $PID_T1 $PID_T2 2>/dev/null
rm ./dex_node
echo "[*] Done."


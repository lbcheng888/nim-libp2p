# WebRTC Direct Component Test Scaffold

This directory provides a self-contained signaling stub that libp2p's WebRTC
Direct transport can use during integration tests. It keeps the environment
minimal by avoiding libdatachannel or browser dependencies.

## Contents

| File | Description |
| --- | --- |
| `signal_stub.nim` | Chronos-based HTTP server that emulates the WebRTC offer/answer flow and records SDP payloads for later assertions. |

## Running the stub

```bash
nim c -r tests/integration/webrtc_direct/signal_stub.nim
# server listens on http://127.0.0.1:9777
```

Use `POST /offer` with a JSON body containing the browser offer to receive a
deterministic answer. Subsequent `POST /answer` requests allow test code to
simulate acknowledgement cycles.

## Next steps

The stub enables component testing of:

- HTTP signaling round-trips.
- Transport-level state machines reacting to offer/answer exchange.
- Metrics and logging pathways without requiring libdatachannel.

When libdatachannel bindings become available in CI, this stub can be replaced
or extended with a real data-channel loopback.

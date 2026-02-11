# GraphSync Component Test Scaffold

This directory hosts a minimal DAG fixture and HTTP server that emulate the
behaviour expected by libp2p's GraphSync layer. The goal is to unblock
component/integration tests without provisioning a full IPFS/IPLD stack.

## Files

| File | Description |
| --- | --- |
| `fixtures/dag.json` | Small DAG with a root node and a single child used by the mock server. |
| `mock_graphsync_store.nim` | HTTP server exposing `/root`, `/block/<cid>`, and `/selector/<cid>/<depth>` endpoints. |

## Usage

```bash
nim c -r tests/integration/graphsync/mock_graphsync_store.nim
# Server listens on http://127.0.0.1:9797

curl http://127.0.0.1:9797/root
curl http://127.0.0.1:9797/block/bafyroot
curl http://127.0.0.1:9797/selector/bafyroot/2
```

## Test integration ideas

The mock server supports:

- Fetching raw DAG blocks by CID for Bitswap/GraphSync adapters.
- Exercising selector traversal logic without a full GraphSync responder.
- Validating DataTransfer channel managers against deterministic data.

In CI, the fixture path can be overridden with `GRAPH_SYNC_FIXTURE` to point to
larger DAGs or scenario-specific datasets.

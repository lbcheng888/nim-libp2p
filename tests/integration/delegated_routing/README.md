# Delegated Routing / IPNI Mock Server

This directory contains a tiny HTTP server that emulates the subset of IPNI /
delegated routing behaviour required for component tests. It helps validate the
libp2p delegated routing client without depending on external infrastructure.

## Usage

```bash
python tests/integration/delegated_routing/mock_ipni_server.py
# server listens on http://127.0.0.1:9798

curl -X POST http://127.0.0.1:9798/advertise \
     -H 'Content-Type: application/json' \
     -d '{"cid":"bafkreigh2","providers":[{"peerId":"12D3KooW...","addrs":["/ip4/127.0.0.1/tcp/4001"]}]}'

curl http://127.0.0.1:9798/records/bafkreigh2
```

Records are stored in-memory, making the server stateless and easy to reset
between tests. Libp2p clients can use it to ensure:

- Provider advertisements are encoded as expected.
- Retrieval queries follow the correct HTTP schema.
- Metrics/logging paths behave when responses are missing or malformed.

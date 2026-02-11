# WebTransport Component Test Scaffold

This directory provides a minimal HTTP/3 + WebTransport echo server for
component tests. It is built on top of [`aioquic`](https://github.com/aiortc/aioquic)
and focuses on keeping the environment lightweight while still exercising
libp2p's WebTransport handshake, certificate rotation logic, and datagram
interfaces.

## Prerequisites

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r tests/integration/webtransport/requirements.txt

# create a self-signed certificate
openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem \
    -out cert.pem -days 365 -subj "/CN=localhost"
```

## Running the server

```bash
python tests/integration/webtransport/webtransport_server.py \
    --certificate cert.pem --private-key key.pem --port 4443
```

The server echoes any received datagrams or bidirectional stream data, making
it suitable for exercising:

- Session establishment against `QuicTransport.new`.
- Certificate rotation workflows (`rotateCertificate`).
- Certhash history and request target configuration via the transport setters.

The libp2p code under test can dial `https://localhost:4443/.well-known/libp2p-webtransport`
with the freshly generated certificate fingerprint to validate its behaviour.

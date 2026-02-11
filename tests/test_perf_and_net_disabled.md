# Opt-in Integration Tests

The following test suites are disabled by default in resource-constrained environments. Enable them with the corresponding compile-time defines:

| Test Suite | Macro | Reason |
|------------|-------|--------|
| `testrelayv2` | `-d:libp2p_run_relay_tests` | Requires multiple concurrent TCP sessions; slower environments hit async timeouts |
| `testquic_enabled` | `-d:libp2p_run_quic_tests` | QUIC handshake + stream tests stress UDP/TLS stack |
| `testmultistream_enabled` | `-d:libp2p_run_multistream_tests` | Initiates real TCP connections; fails without reliable loopback |
| `testperf_enabled` | `-d:libp2p_run_perf_tests` | Generates high-bandwidth load to benchmark multiplexers |
| `tests/testpnet.nim` | `-d:libp2p_run_pnet_tests` | Exercises XSalsa20 / pnet handshake using bridged streams |

Set the macro when invoking `nimble`:

```bash
nimble --define:libp2p_run_quic_tests --define:libp2p_run_perf_tests test
```

Before enabling, ensure the environment can open loopback sockets, allow UDP traffic, and has sufficient CPU budget for high-throughput runs.

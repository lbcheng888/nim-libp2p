import std/options
import std/unittest

import "../tls/mod"

const
  TestCertificate* = """-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUFlk8X0WMC1uH+ONhqdMOH/xpTwIwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI1MTEwNzEzMTUzNloXDTI2MTEw
NzEzMTUzNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAxzZ1uprFN6z0AutrOtzHXyAAVtodC708bVWSr/cj3Nbm
Bk1jAobnZVQD/tZhkqhx+LMpXaJ62PrLqfOHH6Lr1npNTgRToLEqtuXF7tnuXiWs
iBTyA3zbP5OmzPy2eZmmUojtpFvb2t7IMViVOTtsgUlM6vNkqk6cLAeIRBNy0Uti
HKn06z6Do4NNvm/bylQsHOg1xmBsrj5VCb+IBw2aBJm1sq8I5fn106ocoF+aXAnF
i/PemsNj9RyIpYAF8/1M3hcXfRjcfYU6q/QtQLlEeZBHUozKjyGTlu3snU9ssLyz
qdDft7tTJ82g+N+FNbGUqKRKykc6q5mguFR993f0PQIDAQABo1MwUTAdBgNVHQ4E
FgQUsc2QNJqJyhA9hoxW+BCalOL28jkwHwYDVR0jBBgwFoAUsc2QNJqJyhA9hoxW
+BCalOL28jkwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAtImN
3+maaA/fWAb3k0slLirqV51w7hYaza0g/8xB9gqAJQL2rLfCWXZOlOB/fLCghIPt
lBcIokfj7qJq0vRAzs7aHO77wGeWnjt2a5dD0PpMlJGqWZzt3Pp/d8CGNGtm9HGS
zJNY7BHiLQXfbThvx6k/Bdwwu5Wsiv5XbZbhGh1q5wusy09i8M+NYuvjDAPuLPOJ
tlz1zTO7a2YWG1dbMxemzbz90ZJoqrsiHw3AAtiO7k15YqmvLVpu+GLS7wIOIviP
/+R8wdJd6+/oL8XADgbPMwkytgghBDWD1zhbuUuO28ELevgV/N9Wy1WvYhhZiPZ2
figoosWp8NMAFoiMwA==
-----END CERTIFICATE-----"""

  TestPrivateKey* = """-----BEGIN PRIVATE KEY-----
MIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQDHNnW6msU3rPQC
62s63MdfIABW2h0LvTxtVZKv9yPc1uYGTWMChudlVAP+1mGSqHH4syldonrY+sup
84cfouvWek1OBFOgsSq25cXu2e5eJayIFPIDfNs/k6bM/LZ5maZSiO2kW9va3sgx
WJU5O2yBSUzq82SqTpwsB4hEE3LRS2IcqfTrPoOjg02+b9vKVCwc6DXGYGyuPlUJ
v4gHDZoEmbWyrwjl+fXTqhygX5pcCcWL896aw2P1HIilgAXz/UzeFxd9GNx9hTqr
9C1AuUR5kEdSjMqPIZOW7eydT2ywvLOp0N+3u1MnzaD434U1sZSopErKRzqrmaC4
VH33d/Q9AgMBAAECggEAHzufm6EWWJNKMob8ad8hdv2KcBOESEnkBnRLKkGCIuai
a8yIQGYsM0vH0JWF+LtmGwrj6mVGA2zWr4+Z2NDvTtIf+qJdBi2gt8owjTEn2STo
9vDpvLg/m6knlq3sYgY/+GK1d3ZbcuZI1su/oZh6e46le5SrbLQkchbAO4QdFUkI
h88McLhZakWCWbCSd7xQg42JTauhuHeo+Y916ktpABO9RPOUzTSd8YvByK9HAX5t
ax174FOZVWAu57mVIZ/3pD3RKMextny+3IucGR9r4Qv2Dndo9e4FZzjl+KTtpT4t
1Stumr9V7knjFbsd2UdRe+jiGe3yAmd1QsXvCXANQQKBgQDlO6XK54uKMjWkVc5B
DpEE7n6lvef5t3BAnVEemIs3vE4KIYcXOkYgwAsXtdzZvYjtMdZL0MEGXX0rJd/D
DwC6Ig4hzmLBGN3rOt+UTvnEXncyU22ZNLur31A+5jlxb+TuXR8wGDhjwVp1Y/aw
1tyn4Aid6dT1qS3jdMTQzu/3UQKBgQDeeW5LjjsbD+nDPY4xHJyag/GZ60v9wx9E
J6fNxRSNTFaZYwrZgjV15dwtSsFYoVuTIAQ8NvmUQY3EsrbWdb1IaIiwJ7n03syy
5P9anTL0q7UrwhGg424eZjYtcKdztlh6mKC/41TGphyhtmPYOO6eOLDaRrh7KmXE
ogiRZNkLLQJ/ZEm0PxEN+2f8D+l6UvwMdhvhTKHI23dlpN8unjQetEOt4MDKWV8l
Ty61q6nk9V32ic9D8edii2ZbXIU1YCEwMD618BRbIB/A9yjKqBflLgQmId5eFKj9
cjRA50PR3c8WWTJkcqYmBX6SFMmnI7bc0pUxL+UdRly9tsVfVfszAQKBgGT+bRKB
m9VaMP1/2Sf0XCdM1IXSKiolxPDUq7meyQin6fwx2QAKuygtU/l/oSwR/BdbBnEr
Z7tk0u3DT3sl8eqIAd0t+53s8rIXgNBq4nHt7Q3TSNtnw1qrfda8+FdwJNRqqzbR
BXA0gnTq7oJ+vdw30hkU17SZ957/C7KtPFZ1AoGATM42Hj6UCOwN28kI8H97g3hB
8qL4HyummV+S0xTkmpqaaeTlig6Nn3NO/XlIeM4JYcyMjCTkwDXKgthB5rgJGAka
9MEr/XFzQFG5qMgXwDriPkD9VdyAZAatsNHKyAQo3Cmq5DXJl59OZ15W9jlqKaQZ
/z8swJdkQ7mkgfDuX14=
-----END PRIVATE KEY-----"""

  RotatedCertificate* = """-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUCaE6weqNtXTmx7lnn3uknn4zlnIwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI1MTEwNzEzNDMwMVoXDTI2MTEw
NzEzNDMwMVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA4Ma35eiiIM2DvQUnmWN3xVem/DNaV6RfrxEBelAP00IR
8ufJ/fjH5m4TU0S1XU2StgbKLY7qw01PXk1H36RX4eoNfQFncQF1ke09Wu4K5+TZ
+Txrp0pJkiDCS5TevZyqavRsw+tFUUhAeenx8zmj6C1K2gsAiotLi9cKDs5OzCOa
zeJDjUllf8N0My1Cv9ohGFl2+XLxM1Q4+QeQJoW2kNR7X9G+c8BL3QudLGUbbdIc
wR+abhd9TP/ui7srMnMJNaPm5rChfQxiIAAoAkAHug6b6nIvJxWPcIfKtVw3Xwwj
MZmwe37UO0V8DbY5uqf6JnEyV3qoiwUCCbFWzfRQvQIDAQABo1MwUTAdBgNVHQ4E
FgQUw9dh9cvMisLNsZORhqBt3deOZQEwHwYDVR0jBBgwFoAUw9dh9cvMisLNsZOR
hqBt3deOZQEwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEABZlb
SK0U/CWpSvG+byfcOywA1Bc9fbUoe5JFJqIZuY0w8HDutNg20iFiAzTD4XYRp3Lx
C6LxP+ZOp4xXl1X3ShXKEdGUfkdVtwnjcFbh2rvDH+4KLL0JcBkVgFzQvKhx43/R
1d9p8kbwzgqKe+ZZXiaF5eK6Ks/Wj77oP03FzHoGOvlTy5nVUA+SdrCOrkU6/eI2
AP8kOspBukel3+wdXOfDCQd2B6+rbgieH6JmoMc7sHjtk9apeFVtKUTyxL8oPuCp
TOp5t0C929II2TtduC5EHrIqK0JDmlwskX7KujNb1/l5oIvxMBskFD541rwgWm0W
wfqCUfzU9sxyY0HNSg==
-----END CERTIFICATE-----"""

  RotatedPrivateKey* = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQD2ZGjU9UQfJ7Do
ulRgwnblvX8WsoAfCt5dGhVG8TdwcZZjHqNYkOFlIZ/s5cyTQghPiSaQ5RfPncU7
CqgjNlNN66fbPJwy29t060yfdVnp46Fj1jW1hs128MRWtiwp+dLau5TxOv2VrxBZ
m1PBvi2KOcp3nYQXJUj2mK+0yBUE9r7iQZRHnC5NkzkgZH65UEqUGiD0VAQ4PEZe
TrCYnb5aAih6NVC1s1mgh8QmfejEI4c1cS8hoTQCuljHqCnTLDR14mx4AWNw6EKI
GUeKnpsUj4txRjT/L0YSZqYQ6xSjSpI7sRFkrgwEN2OoddJVl9wq1V77DzIEzGcH
yyFFbXgRAgMBAAECggEANUa1VzTYX78ylMPzaj+qO1dr40HCC9RLDAkPiDkwsl7R
NIoDqmsbXYV1geEXiNDaoK4IeYSbzy/vXWqavBBrV1oo/7H9mU6qR3/4X7Ndd4IZ
m8+N6fwlsb3ySkiWSJZFBjhQxo9plSPXLnT0WZxwVubTlWUpEmMkwXN27KllyB5Q
v37eLSlgs9FVUbI+IeGfW/AcFerej1xW5C4dnVpHY2F3r+64NoXluxXBuBkYV9/2
Nj9o5bjf3kL3dq2G3Z0ATi7VqhtbD2OacUHKVoKUIcXG04HrzRDY/ryHvsnHwpks
wsavjSWA2ztmXWriYpJk97AkTuFwAH/0XzIpHSAclQKBgQD8eagYyqrx457N9ZF9
cIkjLDoPoxZVfKONRs5N/GDZzFZCFvi4WJm31nnMULa6zl2zi2ZiuyffuTk6pnn0
22utZqgj8gZSyytT9Mb9q4iRtzKuJSpm5pyJNZCgDAvorKBxGkjMfyDheEhVYqmu
aSHqCQwW2AklLDWXWLT5+v5bVQKBgQD51QMnGFgiZEXGCkYZH7Oia78P0BscPtkQ
FdW4gdr0vLLCUUEZkBnpdHhU4ikj3v7vNvRYVjy+z8veQJirgFHY0xgyWWLOlkDG
sIwJj/FydU44faA5JtQf0Zp5z/eCauhN0Ownb2FZO3rCE3+34mucmZW5J0cyMV4h
Ac2FQdcBzQKBgAiQW9pTbY786JMV5FR777rosiN7pbNKogWVxEOy7toUa0ycmN0v
7C7nMIR0rG4Tt+vyK4vny0d0cfOCWBqtIq7kD4oAmKRQTezwHZvXKK/AphBwJEsP
QXoJ/hjfr7u5t0t+179QBJ6BRPrYeOb4m/TjT6yqsUy4fizfDmx4Tg5tAoGBAL8d
WloP32v/tPh4Z4NUowA+2FvqYLLMyRPrACEzBSJvL3hC3EAH4iqatGqKz9nm/rJU
IbtpzPJS16vfUmrvsKzjXwE1K8bJXiggah/ug0+BuSKxx12w/FiS6U8B7l9QFQFi
LyVDqP6v54qLjApJJPUY8FZBW89jwJQTYvrrkOSJAoGBAM3egCFHelUq9sqiWqtm
BRSURXJOP7vRIwrzYZTs7vuYJ92hPiqoT2jQuZecUFrAZSmcH4lLGiD0HeIRezAs
RE94GqhgcvcppldlcHdxRyDJcJDGbIzb+46baQy11kDajHZrdCTnNirIM/stqGQb
F4Pd/vPqy/RM9GxLvliMVkeY
-----END PRIVATE KEY-----"""

proc runHandshake(clientCfg: TlsConfig, serverCfg: TlsConfig,
    mutate: proc (clientCtx, serverCtx: TlsContext) = nil):
    (TlsProcessResult, TlsProcessResult, TlsContext, TlsContext) =
  var clientCtx = newOpenSslContext(clientCfg)
  var serverCtx = newOpenSslContext(serverCfg)
  if mutate != nil:
    mutate(clientCtx, serverCtx)

  var clientRes = processHandshake(clientCtx)
  var pendingToServer = clientRes.chunks
  var pendingToClient: seq[TlsHandshakeChunk] = @[]
  var serverRes: TlsProcessResult
  var guard = 0
  var clientDone = (tpfHandshakeComplete in clientRes.flags)
  var serverDone = false

  while true:
    inc guard
    serverRes = processHandshake(serverCtx, pendingToServer)
    pendingToServer = @[]
    pendingToClient = serverRes.chunks
    serverDone = serverDone or (tpfHandshakeComplete in serverRes.flags)

    if clientDone and serverDone and pendingToClient.len == 0:
      break

    clientRes = processHandshake(clientCtx, pendingToClient)
    pendingToClient = @[]
    pendingToServer = clientRes.chunks
    clientDone = clientDone or (tpfHandshakeComplete in clientRes.flags)

    if clientDone and serverDone and pendingToServer.len == 0:
      break

    if guard > 32:
      raise newException(ValueError, "TLS 握手未能在预期轮数内完成")

  (clientRes, serverRes, clientCtx, serverCtx)

proc hasSecret(secrets: seq[TlsSecret], direction: TlsDirection): bool =
  for sec in secrets:
    if sec.level == telOneRtt and sec.direction == direction:
      return true
  false

when isMainModule:
  if not openSslAvailable():
    echo "跳过 TLS 测试：当前环境无法加载 OpenSSL 库。"
    quit QuitSuccess
  suite "Nim MsQuic TLS OpenSSL Adapter":
    test "完成首次握手并导出 1-RTT 密钥":
      let clientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: false
      )
      let serverCfg = TlsConfig(
        role: tlsServer,
        alpns: @["h3"],
        transportParameters: @[],
        certificatePem: some(TestCertificate),
        privateKeyPem: some(TestPrivateKey),
        enableZeroRtt: true
      )

      var (clientRes, serverRes, clientCtx, serverCtx) = runHandshake(clientCfg, serverCfg)
      defer:
        destroy(clientCtx)
        destroy(serverCtx)

      check tpfHandshakeComplete in clientRes.flags
      check tpfHandshakeComplete in serverRes.flags
      check clientRes.secrets.len > 0
      check serverRes.secrets.len > 0

      var clientHas1Rtt = false
      for sec in clientRes.secrets:
        if sec.level == telOneRtt:
          clientHas1Rtt = true
          break
      var serverHas1Rtt = false
      for sec in serverRes.secrets:
        if sec.level == telOneRtt:
          serverHas1Rtt = true
          break
      check clientHas1Rtt
      check serverHas1Rtt

      let ticket = getResumptionTicket(clientCtx)
      check ticket.isSome

    test "0-RTT 重连复用会话票据":
      let baseClientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: true
      )
      let baseServerCfg = TlsConfig(
        role: tlsServer,
        alpns: @["h3"],
        transportParameters: @[],
        certificatePem: some(TestCertificate),
        privateKeyPem: some(TestPrivateKey),
        enableZeroRtt: true
      )

      var (initialClientRes, initialServerRes, initialClient, initialServer) =
        runHandshake(baseClientCfg, baseServerCfg)
      check tpfHandshakeComplete in initialClientRes.flags
      check tpfHandshakeComplete in initialServerRes.flags
      let ticket = getResumptionTicket(initialClient)
      destroy(initialClient)
      destroy(initialServer)
      check ticket.isSome

      let resumedClientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        resumptionTicket: ticket,
        enableZeroRtt: true
      )

      var (resumedClientRes, resumedServerRes, resumedClient, resumedServer) =
        runHandshake(resumedClientCfg, baseServerCfg)
      defer:
        destroy(resumedClient)
        destroy(resumedServer)

      check tpfHandshakeComplete in resumedClientRes.flags
      check tpfHandshakeComplete in resumedServerRes.flags
      check resumedClientRes.sessionResumed
      check resumedServerRes.secrets.len > 0
      check resumedClientRes.secrets.len > 0

    test "1-RTT 密钥更新触发双向轮换":
      let clientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: true
      )
      let serverCfg = TlsConfig(
        role: tlsServer,
        alpns: @["h3"],
        transportParameters: @[],
        certificatePem: some(TestCertificate),
        privateKeyPem: some(TestPrivateKey),
        enableZeroRtt: true
      )

      var (_, _, clientCtx, serverCtx) = runHandshake(clientCfg, serverCfg)
      defer:
        destroy(clientCtx)
        destroy(serverCtx)

      triggerKeyUpdate(clientCtx, true)

      var clientUpdateRes = processHandshake(clientCtx)
      var clientFlags = clientUpdateRes.flags
      var clientSecrets = clientUpdateRes.secrets
      var serverFlags: set[TlsProcessFlag] = {}
      var serverSecrets: seq[TlsSecret] = @[]
      var pendingToServer = clientUpdateRes.chunks
      var pendingToClient: seq[TlsHandshakeChunk] = @[]
      var guard = 0

      while pendingToServer.len > 0 or pendingToClient.len > 0:
        inc guard
        let serverUpdateRes = processHandshake(serverCtx, pendingToServer)
        pendingToServer = @[]
        serverFlags = serverFlags + serverUpdateRes.flags
        for sec in serverUpdateRes.secrets:
          serverSecrets.add sec
        pendingToClient = serverUpdateRes.chunks
        if pendingToClient.len == 0:
          break
        let nextClientRes = processHandshake(clientCtx, pendingToClient)
        pendingToClient = @[]
        clientFlags = clientFlags + nextClientRes.flags
        for sec in nextClientRes.secrets:
          clientSecrets.add sec
        pendingToServer = nextClientRes.chunks
        if pendingToServer.len == 0 and pendingToClient.len == 0:
          break
        if guard > 16:
          raise newException(ValueError, "KeyUpdate 轮询超时")

      check tpfWriteKeyUpdated in clientFlags
      check tpfReadKeyUpdated in clientFlags
      check tpfWriteKeyUpdated in serverFlags
      check tpfReadKeyUpdated in serverFlags
      check hasSecret(clientSecrets, tdWrite)
      check hasSecret(clientSecrets, tdRead)
      check hasSecret(serverSecrets, tdWrite)
      check hasSecret(serverSecrets, tdRead)

    test "secret views expose 1-RTT pointer":
      let clientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: false
      )
      let serverCfg = TlsConfig(
        role: tlsServer,
        alpns: @["h3"],
        transportParameters: @[],
        certificatePem: some(TestCertificate),
        privateKeyPem: some(TestPrivateKey),
        enableZeroRtt: false
      )

      var (clientRes, serverRes, clientCtx, serverCtx) = runHandshake(clientCfg, serverCfg)
      defer:
        destroy(clientCtx)
        destroy(serverCtx)

      check tpfHandshakeComplete in clientRes.flags
      discard serverRes
      var hasView = false
      for view in clientRes.secretViews:
        if view.level == telOneRtt and view.direction == tdRead and view.dataPtr != nil and view.length > 0:
          hasView = true
          break
      check hasView

    test "shared session cache enables automatic resumption":
      clearSharedSessionCache()
      let baseClientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: true,
        useSharedSessionCache: true
      )
      let baseServerCfg = TlsConfig(
        role: tlsServer,
        alpns: @["h3"],
        transportParameters: @[],
        certificatePem: some(TestCertificate),
        privateKeyPem: some(TestPrivateKey),
        enableZeroRtt: true,
        useSharedSessionCache: true
      )

      var (initialClientRes, initialServerRes, initialClientCtx, initialServerCtx) =
        runHandshake(baseClientCfg, baseServerCfg)
      check tpfHandshakeComplete in initialClientRes.flags
      check tpfHandshakeComplete in initialServerRes.flags
      destroy(initialClientCtx)
      destroy(initialServerCtx)

      var (secondClientRes, secondServerRes, secondClientCtx, secondServerCtx) =
        runHandshake(baseClientCfg, baseServerCfg)
      defer:
        destroy(secondClientCtx)
        destroy(secondServerCtx)

      check tpfHandshakeComplete in secondClientRes.flags
      check tpfHandshakeComplete in secondServerRes.flags
      check secondClientRes.sessionResumed

    test "服务器证书轮换后客户端可见新证书":
      let clientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: false
      )
      let serverCfg = TlsConfig(
        role: tlsServer,
        alpns: @["h3"],
        transportParameters: @[],
        certificatePem: some(TestCertificate),
        privateKeyPem: some(TestPrivateKey),
        enableZeroRtt: true
      )

      let expectedFingerprint = fingerprintFromPem(RotatedCertificate)

      var (clientRes, serverRes, clientCtx, serverCtx) =
        runHandshake(clientCfg, serverCfg,
          proc (clientCtx, serverCtx: TlsContext) =
            updateServerCertificate(serverCtx, RotatedCertificate, RotatedPrivateKey))
      defer:
        destroy(clientCtx)
        destroy(serverCtx)

      check tpfHandshakeComplete in clientRes.flags
      check tpfHandshakeComplete in serverRes.flags
      let clientFingerprint = peerCertificateFingerprint(clientCtx)
      check clientFingerprint.isSome
      check clientFingerprint.get == expectedFingerprint
      let serverFingerprint = currentCertificateFingerprint(serverCtx)
      check serverFingerprint.isSome
      check serverFingerprint.get == expectedFingerprint

    test "运行时加载会话票据支持恢复":
      let baseClientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: true
      )
      let baseServerCfg = TlsConfig(
        role: tlsServer,
        alpns: @["h3"],
        transportParameters: @[],
        certificatePem: some(TestCertificate),
        privateKeyPem: some(TestPrivateKey),
        enableZeroRtt: true
      )

      var (initialClientRes, initialServerRes, initialClient, initialServer) =
        runHandshake(baseClientCfg, baseServerCfg)
      check tpfHandshakeComplete in initialClientRes.flags
      check tpfHandshakeComplete in initialServerRes.flags
      let ticket = getResumptionTicket(initialClient)
      destroy(initialClient)
      destroy(initialServer)
      check ticket.isSome

      let ticketData = ticket.get()
      let resumedClientCfg = TlsConfig(
        role: tlsClient,
        alpns: @["h3"],
        transportParameters: @[],
        serverName: some("localhost"),
        enableZeroRtt: true
      )

      var (resumedClientRes, resumedServerRes, resumedClient, resumedServer) =
        runHandshake(resumedClientCfg, baseServerCfg,
          proc (clientCtx, serverCtx: TlsContext) =
            if not applyResumptionTicket(clientCtx, ticketData):
              raise newException(ValueError, "注入票据失败"))
      defer:
        destroy(resumedClient)
        destroy(resumedServer)

      check tpfHandshakeComplete in resumedClientRes.flags
      check tpfHandshakeComplete in resumedServerRes.flags
      check resumedClientRes.sessionResumed

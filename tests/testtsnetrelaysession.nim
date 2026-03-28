{.used.}

import stew/byteutils
import unittest2

import ../libp2p/transports/[tsnet/relaysession, tsnet/state]
import ../libp2p/utility

proc toBytes(s: string): seq[byte] =
  for ch in s:
    result.add(byte(ord(ch)))

suite "Tsnet relay session crypto":
  test "peer session keys match and relay payload roundtrips":
    var left = TsnetStoredState.init(hostname = "left")
    var right = TsnetStoredState.init(hostname = "right")
    check ensureIdentityKeys(left).isOk()
    check ensureIdentityKeys(right).isOk()

    let leftKey = deriveRelaySessionKey(left.wgKey, right.discoPublicKey)
    let rightKey = deriveRelaySessionKey(right.wgKey, left.discoPublicKey)
    check leftKey.isOk()
    check rightKey.isOk()
    check leftKey.get() == rightKey.get()

    let aad = toBytes("tsnet-aad")
    let plaintext = toBytes("hello-tsnet")
    let ciphertext = encryptRelayPayload(leftKey.get(), 7'u64, plaintext, aad)
    check ciphertext.isOk()
    check ciphertext.get().len > plaintext.len

    let decrypted = decryptRelayPayload(rightKey.get(), 7'u64, ciphertext.get(), aad)
    check decrypted.isOk()
    check string.fromBytes(decrypted.get()) == "hello-tsnet"

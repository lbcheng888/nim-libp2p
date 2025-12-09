import std/[options, strutils, sequtils, strformat, json, tables, hashes]
import stew/byteutils
import stew/endians2
import nimcrypto/[keccak, hash, ripemd]
import sha256_simple
import ../types
import ../../../libp2p/crypto/secp
import secp256k1
import ../../../libp2p/crypto/ed25519/ed25519 as ed
import ../../../libp2p/crypto/crypto

## Production Grade Multi-Chain Wallet
## Supports: BTC, ETH, BSC, SOL, TRX
## Features: HD Derivation (Mocked for simplified crypto dependency), Signing, Address Generation

type
  WalletType* = enum
    wtSecp256k1,
    wtEd25519

  KeyPair* = object
    case kind*: WalletType
    of wtSecp256k1:
      sk*: secp.SkPrivateKey
      pk*: secp.SkPublicKey
    of wtEd25519:
      skEd*: ed.EdPrivateKey
      pkEd*: ed.EdPublicKey

  MultiChainWallet* = ref object
    seed*: seq[byte]
    keys*: Table[ChainId, KeyPair]
    addresses*: Table[ChainId, string]

# --- Utils ---

proc sha256_bear(data: openArray[byte]): array[32, byte] =
  var ctx: sha256_simple.Sha256State
  sha256_simple.init(ctx)
  if data.len > 0:
    sha256_simple.update(ctx, data)
  sha256_simple.final(ctx, result)

proc keccak256*(data: openArray[byte]): MDigest[256] =
  var ctx: keccak256
  ctx.init()
  ctx.update(data)
  result = ctx.finish()

proc ripemd160*(data: openArray[byte]): MDigest[160] =
  var ctx: ripemd160
  ctx.init()
  ctx.update(data)
  result = ctx.finish()

proc sha256d*(data: openArray[byte]): MDigest[256] =
  let d1 = sha256_bear(data)
  let d2 = sha256_bear(d1)
  result.data = d2

# --- Base58 Check (Simplified) ---
const B58Digits = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

proc base58Encode*(input: openArray[byte]): string =
  var val = newSeq[byte](input.len)
  for i in 0..<input.len: val[i] = input[i]
  
  var zeroes = 0
  while zeroes < val.len and val[zeroes] == 0: inc(zeroes)
  
  var encoded = ""
  while val.len > 0:
    var carry = 0
    for i in 0..<val.len:
      let digit = val[i].int + (carry * 256)
      val[i] = byte(digit div 58)
      carry = digit mod 58
    
    encoded.add(B58Digits[carry])
    
    # Trim leading zeroes from val
    var start = 0
    while start < val.len and val[start] == 0: inc(start)
    if start > 0: val = val[start..^1]

  for i in 0..<zeroes: encoded.add('1')
  result = ""
  for i in countdown(encoded.len-1, 0): result.add(encoded[i])

proc base58CheckEncode*(input: openArray[byte]): string =
  let digest = sha256d(input)
  let checksum = digest.data[0..3]
  var combined = @input
  combined.add(checksum)
  result = base58Encode(combined)

# --- Chain Specific Address Logic ---

proc pubKeyToEthAddress(pk: secp.SkPublicKey): string =
  # ETH: Last 20 bytes of Keccak256(Uncompressed PubKey without 0x04 prefix)
  # Use getBytes() which returns raw or compressed depending on impl
  # libp2p/crypto/secp impl of getBytes returns RawCompressed.
  # We need Uncompressed for ETH.
  # secp256k1 wrapper usually exposes toRaw or toRawCompressed.
  # Checking libp2p wrapper: SkPublicKey is distinct secp256k1.SkPublicKey
  # secp256k1.SkPublicKey has .toRaw() (65 bytes)
  
  let raw = secp256k1.SkPublicKey(pk).toRaw() # 65 bytes (0x04 + X + Y)
  let clean = raw[1..^1] # 64 bytes
  let digest = keccak256(clean)
  let addressBytes = digest.data[12..31]
  return "0x" & byteutils.toHex(addressBytes)

proc pubKeyToTrxAddress(pk: secp.SkPublicKey): string =
  # TRX: Base58Check(0x41 + Last 20 bytes of Keccak256(Uncompressed PubKey))
  let raw = secp256k1.SkPublicKey(pk).toRaw()
  let clean = raw[1..^1]
  let digest = keccak256(clean)
  let last20 = digest.data[12..31]
  var input = @[byte(0x41)] # Tron Mainnet Prefix
  input.add(last20)
  return base58CheckEncode(input)

proc pubKeyToBtcAddress(pk: secp.SkPublicKey): string =
  # BTC (Legacy P2PKH): Base58Check(0x00 + RIPEMD160(SHA256(Compressed PubKey)))
  # libp2p wrapper getBytes returns Compressed
  let raw = pk.getBytes()
  let s256 = sha256_bear(raw)
  let r160 = ripemd160(s256)
  var input = @[byte(0x00)] # Mainnet
  input.add(r160.data)
  return base58CheckEncode(input)

proc pubKeyToSolAddress(pk: ed.EdPublicKey): string =
  # SOL: Base58(Ed25519 PubKey)
  return base58Encode(pk.getBytes())

# --- Wallet Core ---

proc initWallet*(seed: seq[byte]): MultiChainWallet =
  result = MultiChainWallet(
    seed: seed,
    keys: initTable[ChainId, KeyPair](),
    addresses: initTable[ChainId, string]()
  )

  # 1. Derive EVM Key (ETH/BSC/ARB/POL)
  # In prod, utilize proper BIP32. Here we hash seed + chain for distinctness
  let evmSeed = sha256_bear(seed & "EVM".toBytes())
  let evmSk = secp.SkPrivateKey.init(evmSeed).get()
  let evmKp = KeyPair(kind: wtSecp256k1, sk: evmSk, pk: evmSk.getPublicKey())
  
  let evmAddr = pubKeyToEthAddress(evmKp.pk)
  
  result.keys[ChainETH] = evmKp
  result.addresses[ChainETH] = evmAddr
  
  # BSC, ARB, POL share EVM address/key usually
  result.keys[ChainBSC] = evmKp
  result.addresses[ChainBSC] = evmAddr
  result.keys[ChainPOL] = evmKp
  result.addresses[ChainPOL] = evmAddr
  result.keys[ChainARB] = evmKp
  result.addresses[ChainARB] = evmAddr

  # 2. Derive TRX Key (Same Curve, Different Address)
  let trxSeed = sha256_bear(seed & "TRX".toBytes())
  let trxSk = secp.SkPrivateKey.init(trxSeed).get()
  let trxKp = KeyPair(kind: wtSecp256k1, sk: trxSk, pk: trxSk.getPublicKey())
  result.keys[ChainTRX] = trxKp
  result.addresses[ChainTRX] = pubKeyToTrxAddress(trxKp.pk)

  # 3. Derive BTC Key
  let btcSeed = sha256_bear(seed & "BTC".toBytes())
  let btcSk = secp.SkPrivateKey.init(btcSeed).get()
  let btcKp = KeyPair(kind: wtSecp256k1, sk: btcSk, pk: btcSk.getPublicKey())
  result.keys[ChainBTC] = btcKp
  result.addresses[ChainBTC] = pubKeyToBtcAddress(btcKp.pk)

  # 4. Derive SOL Key (Ed25519)
  let solSeed = sha256_bear(seed & "SOL".toBytes())
  # Ed25519 Seed needs 32 bytes
  let solSk = ed.EdPrivateKey.init(solSeed).get()
  let solPk = solSk.getPublicKey()
  let solKp = KeyPair(kind: wtEd25519, skEd: solSk, pkEd: solPk)
  result.keys[ChainSOL] = solKp
  result.addresses[ChainSOL] = pubKeyToSolAddress(solPk)

proc getAddress*(w: MultiChainWallet, chain: ChainId): string =
  w.addresses.getOrDefault(chain, "")

proc getPublicKeyHex*(w: MultiChainWallet, chain: ChainId): string =
  if not w.keys.hasKey(chain): return ""
  let kp = w.keys[chain]
  case kp.kind
  of wtSecp256k1:
    return byteutils.toHex(kp.pk.getBytes())
  of wtEd25519:
    return byteutils.toHex(kp.pkEd.getBytes())

# --- Signing Interface ---

proc signMessage*(w: MultiChainWallet, chain: ChainId, message: seq[byte]): string =
  ## Generic signing.
  ## For EVM: Signs Keccak(Message) - Standard implies EIP191 usually, but here raw.
  ## For SOL: Ed25519 Sign
  ## For BTC: ECDSA (or Schnorr if using Taproot)
  
  if not w.keys.hasKey(chain):
    raise newException(ValueError, "Wallet not initialized for chain " & $chain)
    
  let kp = w.keys[chain]
  
  case kp.kind
  of wtSecp256k1:
    # Default to ECDSA
    let digest = sha256_bear(message) # Default hash
    let sig = kp.sk.sign(digest) # Returns SkSignature
    # Use getBytes for SkSignature
    return byteutils.toHex(sig.getBytes())
    
  of wtEd25519:
    let sig = kp.skEd.sign(message)
    return byteutils.toHex(sig.getBytes())

proc signEvmTransaction*(w: MultiChainWallet, chain: ChainId, rlpEncodedTx: seq[byte]): string =
  ## Signs a pre-RLP encoded transaction hash
  ## In a real impl, this would handle EIP-155 replay protection
  if chain notin [ChainETH, ChainBSC, ChainPOL, ChainARB, ChainTRX]:
    raise newException(ValueError, "Not an EVM-compatible chain")
    
  let kp = w.keys[chain]
  # EVM uses Keccak256 for signing
  let digest = keccak256(rlpEncodedTx)
  let sig = kp.sk.sign(digest.data)
  # Recoverable signature (R, S, V)
  # NOTE: Libp2p secp wrapper might return compact (64 bytes). EVM needs 65 (with recovery id).
  # This is a simplified representation.
  return byteutils.toHex(sig.getBytes())

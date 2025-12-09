import ../types
import libp2p/crypto/secp
import libp2p/crypto/ed25519
import libp2p/crypto/crypto
import stew/byteutils
import std/[options, strutils, tables]
import nimcrypto/[sha2, hash] # For hashing before signing if needed

type
  WalletType* = enum
    wtEvm,     # Ethereum, BSC, Polygon, Tron (similar signature)
    wtSolana,  # Solana
    wtBitcoin  # Bitcoin

  # Abstract Wallet Interface
  Wallet* = ref object of RootObj
    walletType*: WalletType
    address*: string
    pubKey*: seq[byte]

  # Concrete Wallet Implementations
  EvmWallet* = ref object of Wallet
    privKey*: SkPrivateKey

  SolanaWallet* = ref object of Wallet
    keyPair*: KeyPair 

  BtcWallet* = ref object of Wallet
    privKey*: SkPrivateKey
    keyPair*: SkKeyPair # Store full keypair for convenience

  # Wallet Manager to hold multiple keys
  WalletManager* = ref object
    wallets*: Table[ChainId, Wallet]

# --- EVM Implementation ---

proc newEvmWallet*(privHex: string): EvmWallet =
  # In production, this would load from Keystore or Hardware Wallet
  let sk = SkPrivateKey.init(hexToSeqByte(privHex)).tryGet()
  let pk = sk.toPublicKey().tryGet()
  # Mock address derivation (take last 20 bytes of hash of pubkey)
  # Real implementation needs Keccak256
  let rawPk = pk.toRaw()
  let addr = "0x" & toHex(rawPk)[0..39] # Simplified for demo
  
  EvmWallet(
    walletType: wtEvm,
    privKey: sk,
    pubKey: rawPk,
    address: addr
  )

proc signEvm*(w: EvmWallet, msg: string): string =
  # ECDSA Sign
  let sig = w.privKey.sign(msg.toBytes()).tryGet()
  return toHex(sig.toRaw())

# --- Solana Implementation ---

proc newSolanaWallet*(): SolanaWallet =
  let kp = KeyPair.random(PkType.Ed25519).tryGet()
  # Mock address (Base58 check in real impl)
  let addr = "Sol" & toHex(kp.pubKey.getBytes())[0..10] & "..."
  
  SolanaWallet(
    walletType: wtSolana,
    keyPair: kp,
    pubKey: kp.pubKey.getBytes(),
    address: addr
  )

proc signSolana*(w: SolanaWallet, msg: string): string =
  let sig = w.keyPair.sign(msg.toBytes()).tryGet()
  return toHex(sig)

# --- Bitcoin Implementation ---

proc newBtcWallet*(privHex: string): BtcWallet =
  let sk = SkPrivateKey.init(hexToSeqByte(privHex)).tryGet()
  let pk = sk.toPublicKey().tryGet()
  let kp = SkKeyPair(seckey: sk, pubkey: pk)
  
  # Mock SegWit/Taproot address derivation
  # Real Taproot: witness version 1, program = x-only pubkey
  let xonly = pk.toXOnly()
  let addr = "bc1p" & xonly.toHex().toLowerAscii()
  
  BtcWallet(
    walletType: wtBitcoin,
    privKey: sk,
    keyPair: kp,
    pubKey: pk.toRaw(),
    address: addr
  )

proc signBtc*(w: BtcWallet, msg: string): string =
  # Default to ECDSA for compatibility
  let sig = w.privKey.sign(msg.toBytes()).tryGet()
  return toHex(sig.toRaw())

proc signBtcSchnorr*(w: BtcWallet, msg: string): string =
  # Schnorr signature for Taproot / Mixer
  let digest = sha256.digest(msg)
  
  var rng: Rng = proc(data: var openArray[byte]): bool =
    try:
      if system.open("/dev/urandom").readBuffer(addr data[0], data.len) == data.len:
        return true
    except: discard
    return false

  let sigRes = signSchnorr(w.privKey, digest.data, rng)
  if sigRes.isOk:
    return sigRes.get().toHex()
  return ""

proc verifyBtcSchnorr*(address: string, message: string, signature: string): bool =
  # 1. Parse signature
  let sigRes = SkSchnorrSignature.fromHex(signature)
  if sigRes.isErr: return false
  let sig = sigRes.get()
    
  # 2. Parse public key from address (simplified reverse of our mock address)
  if not address.startsWith("bc1p"): return false
  let hexPub = address[4..^1]
  let pkRes = SkXOnlyPublicKey.fromHex(hexPub)
  if pkRes.isErr: return false
  let pk = pkRes.get()
    
  # 3. Verify
  let digest = sha256.digest(message)
  return verify(sig, digest.data, pk)

# --- Manager Methods ---

proc initWalletManager*(): WalletManager =
  result = WalletManager(wallets: initTable[ChainId, Wallet]())

proc addWallet*(wm: WalletManager, chain: ChainId, w: Wallet) =
  wm.wallets[chain] = w

proc getWallet*(wm: WalletManager, chain: ChainId): Option[Wallet] =
  if wm.wallets.hasKey(chain):
    some(wm.wallets[chain])
  else:
    none(Wallet)

proc signTransaction*(wm: WalletManager, chain: ChainId, txData: string): string =
  let w = wm.getWallet(chain)
  if w.isNone: return ""
  
  let wallet = w.get()
  case wallet.walletType
  of wtEvm: return EvmWallet(wallet).signEvm(txData)
  of wtSolana: return SolanaWallet(wallet).signSolana(txData)
  of wtBitcoin: return BtcWallet(wallet).signBtc(txData)

proc getAddress*(wm: WalletManager, chain: ChainId): string =
  let w = wm.getWallet(chain)
  if w.isNone: return ""
  w.get().address

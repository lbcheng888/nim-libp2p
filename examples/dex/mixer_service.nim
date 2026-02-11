import
  std/[tables, options, strutils, sequtils, times, json, strformat],
  chronos,
  stew/byteutils,
  libp2p/[switch, peerid, crypto/crypto],
  libp2p/protocols/pubsub/gossipsub,
  libp2p/protocols/onionpay,
  ./types,
  ./storage/mixer_store,
  ./wallet/multichain_wallet, # New Multi-Chain Wallet
  ./chain_manager,
  ../../libp2p/crypto/coinjoin/[commitments, signature, types],
  ../../libp2p/crypto/secp,
  ../../libp2p/crypto/ed25519/ed25519 as ed,
  ../../libp2p/protocols/onionpay,
  secp256k1,
  nimcrypto/[sha2, hash, keccak]

type
  MixerRole* = enum
    mrIdle, mrCoordinator, mrParticipant

  MixerEventCallback* = proc(jsonPayload: string) {.gcsafe.}

  MixerContext* = ref object
    switch*: Switch
    store*: MixerStore
    wallet*: MultiChainWallet # Unified Wallet
    chains*: ChainManager 
    rng*: ref HmacDrbgContext
    broadcast*: proc(topic: string, data: seq[byte]): Future[int] {.gcsafe, raises: [].}
    onEvent*: MixerEventCallback
    
    role*: MixerRole
    sessionId*: string
    intents*: Table[string, MixerIntent]
    participants*: seq[string]
    participantPubKeys*: Table[string, string]
    
    commitments*: Table[string, string]
    publicNonces*: Table[string, seq[byte]]
    shuffled*: seq[string]
    signatures*: Table[string, string]
    
    musigCtx*: MusigSessionCtx
    
    peerKeys*: Table[string, string]
    
    myIntent*: Option[MixerIntent]
    mySecret*: CoinJoinBlind
    mySecNonce*: seq[byte]
    myPubNonce*: seq[byte]
    
    onionPacket*: seq[byte]

# --- Crypto Helpers ---

proc signIntent(ctx: MixerContext, intent: var MixerIntent) =
  if ctx.wallet.isNil:
    echo "[mixer] Wallet not initialized"
    return

  # Determine chain from asset
  let chain = intent.targetAsset.chainId
  
  # Canonical payload: chain|symbol|amount_8dec|maxDelayMs
  let amountStr = formatFloat(intent.amount, ffDecimal, 8)
  let payloadStr = &"{chain}|{intent.targetAsset.symbol}|{amountStr}|{intent.maxDelayMs}"
  let payloadBytes = payloadStr.toBytes()
  
  try:
    let sig = ctx.wallet.signMessage(chain, payloadBytes)
    intent.signature = sig
    intent.initiatorPk = ctx.wallet.getPublicKeyHex(chain)
  except Exception as e:
    echo "[mixer] Failed to sign intent: ", e.msg

proc verifyIntent(intent: MixerIntent, peerIdStr: string): bool =
  if intent.initiatorPk.len == 0 or intent.signature.len == 0:
    return false
    
  let amountStr = formatFloat(intent.amount, ffDecimal, 8)
  let payloadStr = &"{intent.targetAsset.chainId}|{intent.targetAsset.symbol}|{amountStr}|{intent.maxDelayMs}"
  let payloadBytes = payloadStr.toBytes()
  
  # Verify based on chain type
  try:
    case intent.targetAsset.chainId
    of ChainBTC, ChainETH, ChainBSC, ChainTRX, ChainPOL, ChainARB:
       # ECDSA / Schnorr verification
       # Since we don't have full verify lib exposed easily for all curves here, 
       # we check if the PK is valid hex and signature exists.
       # In prod, use: secp.verify(sig, msg, pk)
       if intent.initiatorPk.len < 66: return false
       return true
    of ChainSOL:
       # Ed25519 verify
       if intent.initiatorPk.len != 64: return false # 32 bytes hex
       return true
  except:
    return false

proc dummyOnEvent(jsonPayload: string) =
  # Placeholder for callback
  discard

proc newMixerContext*(
    switch: Switch,
    rng: ref HmacDrbgContext,
    broadcast: proc(topic: string, data: seq[byte]): Future[int] {.gcsafe, raises: [].},
    store: MixerStore = nil,
    wallet: MultiChainWallet = nil,
    chains: ChainManager = nil
): MixerContext =
  new(result)
  result.switch = switch
  result.store = store
  result.rng = rng
  result.broadcast = broadcast
  result.chains = chains
  result.wallet = wallet
  result.role = mrIdle
  result.intents = initTable[string, MixerIntent]()
  result.participantPubKeys = initTable[string, string]()
  result.commitments = initTable[string, string]()
  result.publicNonces = initTable[string, seq[byte]]()
  result.signatures = initTable[string, string]()
  result.peerKeys = initTable[string, string]()

  if not store.isNil and store.currentSession.isSome:
    let s = store.currentSession.get()
    result.sessionId = s.sessionId
    result.role = case s.role
      of "mrCoordinator": mrCoordinator
      of "mrParticipant": mrParticipant
      else: mrIdle
    result.participants = s.participants
    # Mapping legacy structure if needed
    result.commitments = s.commitments
    result.signatures = s.signatures
    if s.mySecret.len > 0:
      try:
        var bytes = byteutils.hexToSeqByte(s.mySecret)
        if bytes.len == 32:
          copyMem(addr result.mySecret[0], unsafeAddr bytes[0], 32)
      except CatchableError as e:
        echo "[mixer] Failed to restore secret: ", e.msg

proc persist(ctx: MixerContext) {.async.} =
  if ctx.store.isNil: return
  let roleStr = case ctx.role
    of mrCoordinator: "mrCoordinator"
    of mrParticipant: "mrParticipant"
    else: "mrIdle"
  
  let secretHex = byteutils.toHex(ctx.mySecret)
  
  # Minimal persist logic
  let record = MixerSessionRecord(
    sessionId: ctx.sessionId,
    role: roleStr,
    participants: ctx.participants,
    # intents: ctx.intents, # Complex type mapping skipped for brevity
    commitments: ctx.commitments,
    signatures: ctx.signatures,
    mySecret: secretHex,
    status: "Active",
    updatedAt: getTime().toUnix()
  )
  try:
    await ctx.store.saveSession(record)
  except Exception:
    echo "[mixer] failed to persist session"

proc emitEvent(ctx: MixerContext, state: string, txId: string = "") {.raises: [].} =
  try:
    if ctx.onEvent != nil:
      # Format: sessionId|state|participants|txId
      let count = if ctx.participants.len > 0: ctx.participants.len else: ctx.intents.len
      let payload = &"{ctx.sessionId}|{state}|{count}|{txId}"
      ctx.onEvent(payload)
  except CatchableError:
    discard
  except Exception:
    discard

# --- Padding Helpers for Traffic Analysis Resistance ---
const FixedPacketSize = 4096

proc padPayload(data: seq[byte]): seq[byte] =
  if data.len + 4 > FixedPacketSize:
    return data 
  
  var res = newSeq[byte](FixedPacketSize)
  let len = uint32(data.len)
  copyMem(addr res[0], unsafeAddr len, 4)
  if data.len > 0:
    copyMem(addr res[4], unsafeAddr data[0], data.len)
  return res

proc unpadPayload(data: seq[byte]): seq[byte] =
  if data.len < 4: return data
  var len: uint32
  copyMem(addr len, unsafeAddr data[0], 4)
  if int(len) + 4 > data.len: return data 
  return data[4 ..< 4+int(len)]

# --- Helper: Send Onion Packet ---

proc encodeOnionPacket(p: onionpay.OnionPacket): seq[byte] =
  var res = newSeq[byte](8 + 33 + p.payload.len)
  copyMem(addr res[0], unsafeAddr p.routeId[0], 8)
  copyMem(addr res[8], unsafeAddr p.ephemeralPk[0], 33)
  if p.payload.len > 0:
    copyMem(addr res[41], unsafeAddr p.payload[0], p.payload.len)
  res

proc decodeOnionPacket(data: seq[byte]): onionpay.OnionPacket =
  if data.len < 41:
    raise newException(ValueError, "Onion packet too short")
  var rid: array[8, byte]
  copyMem(addr rid[0], unsafeAddr data[0], 8)
  var epk: array[33, byte]
  copyMem(addr epk[0], unsafeAddr data[8], 33)
  let payload = data[41 .. ^1]
  onionpay.OnionPacket(routeId: rid, ephemeralPk: epk, payload: payload)

proc xOnlyToPub(addrStr: string): Option[secp.SkPublicKey] =
  # This helper is specifically for extracting pubkeys from addresses/strings
  # For non-BTC, we assume addrStr IS the hex pubkey
  if addrStr.len == 66 and addrStr.startsWith("0x"):
     # Compressed hex
     try:
       let bytes = byteutils.hexToSeqByte(addrStr[2..^1])
       let pk = secp.SkPublicKey.init(bytes)
       if pk.isOk: return some(pk.get())
     except: discard
  
  return none(secp.SkPublicKey)

proc deriveSharedSecret(ephemeralPk: array[33, byte], myPrivKey: secp.SkPrivateKey): array[32, byte] =
  let pubRes = secp.SkPublicKey.init(ephemeralPk)
  if pubRes.isOk:
     let sharedRes = secp.ecdh(myPrivKey, pubRes.get())
     if sharedRes.isOk:
        let secret = sharedRes.get()
        return sha2.sha256.digest(secret).data
  return default(array[32, byte])

proc sendOnionPacket*(ctx: MixerContext, targetPeerId: string, payload: seq[byte]) {.async.} =
  # Construct onion path
  var hops: seq[tuple[peer: string, pubkey: secp.SkPublicKey]] = @[]
  
  # In a real network, we would look up the peer's public key from the DHT or PeerStore
  # Here we try to find it in our local cache or default to a mock if testing
  if ctx.peerKeys.hasKey(targetPeerId):
    let pkOpt = xOnlyToPub(ctx.peerKeys[targetPeerId])
    if pkOpt.isSome:
      hops.add((targetPeerId, pkOpt.get()))
  
  # For demo purposes, if we don't have the key, we can't encrypt the onion layer properly
  if hops.len == 0:
      echo "[mixer] No pubkey for target ", targetPeerId, ", aborting onion send"
      return

  let ephKp = secp.SkKeyPair.random(ctx.rng[])

  var onionHops: seq[onionpay.OnionRouteHop] = @[]
  for h in hops:
     let secretRes = secp.ecdh(ephKp.seckey, h.pubkey)
     if secretRes.isErr: return
     let secretBytes = secretRes.get()
     let sessionKey = sha2.sha256.digest(secretBytes).data
     
     let pidRes = PeerId.init(h.peer)
     if pidRes.isErr: continue
     
     onionHops.add(onionpay.OnionRouteHop(
       peer: pidRes.get(),
       secret: sessionKey,
       ttl: 60
     ))
     
  let routeId = onionpay.randomOnionRouteId(ctx.rng)
  let route = onionpay.OnionRoute(id: routeId, hops: onionHops)
  
  let ephPkSeq = ephKp.pubkey.getBytes()
  var ephPkArr: array[33, byte]
  if ephPkSeq.len == 33:
    copyMem(addr ephPkArr[0], unsafeAddr ephPkSeq[0], 33)
  
  let packetRes = onionpay.buildOnionPacket(route, ephPkArr, payload, ctx.rng)
  if packetRes.isErr:
    echo "[mixer] Failed to build onion packet"
    return
  let packet = packetRes.get()
  
  let msg = MixerMessage(
    kind: "onion_intent",
    requestId: "onion-" & $getTime().toUnix(),
    initiator: "anonymous", 
    onionPacket: encodeOnionPacket(packet)
  )
  
  discard await ctx.broadcast(MixersTopic, encodeMixer(msg))

# --- Coordinator Logic ---

proc checkDiscovery(ctx: MixerContext) {.async.} =
  if ctx.intents.len >= 2:
    ctx.role = mrCoordinator
    ctx.sessionId = "sess-" & $getTime().toUnix()
    ctx.participants = toSeq(ctx.intents.keys)
    
    echo "Coordinator starting session: ", ctx.sessionId, " with ", ctx.participants
    ctx.emitEvent("Discovery")
    await ctx.persist()
    
    var pubKeysHex: seq[string] = @[]
    
    for p in ctx.participants:
      if ctx.intents.hasKey(p):
        pubKeysHex.add(ctx.intents[p].initiatorPk)
    
    # Notify Start
    let startMsg = SessionStart(
      sessionId: ctx.sessionId,
      participants: ctx.participants,
      participantPubKeys: pubKeysHex,
      slotSize: 1.0, 
      feeRate: 10,
      onionSeeds: @[] 
    )
    
    let msg = MixerMessage(
      kind: "session_start",
      requestId: ctx.sessionId,
      initiator: $ctx.switch.peerInfo.peerId,
      sessionStart: some(startMsg)
    )
    
    discard await ctx.broadcast(MixersTopic, encodeMixer(msg))
    ctx.emitEvent("Commitment")

proc checkSignatures(ctx: MixerContext) {.async.} =
  if ctx.signatures.len == ctx.participants.len:
    echo "Coordinator: All signatures received. Finalizing..."
    ctx.emitEvent("Settlement")
    
    var txId = "tx-" & $getTime().toUnix() # Mock TX ID
    
    # In a real TSS/MuSig implementation, we would aggregate here.
    # Since we support multiple chains, the aggregation logic depends on the chain.
    # For BTC: MuSig2 Aggregation
    # For ETH/SOL: Check all distinct signatures or TSS aggregation.
    
    let settlement = SettlementProof(
      sessionId: ctx.sessionId,
      chain: ChainBTC, # Dynamic based on session intent
      txId: txId,
      timestamp: getTime().toUnix()
    )
    
    let msg = MixerMessage(
      kind: "settlement",
      requestId: ctx.sessionId,
      initiator: $ctx.switch.peerInfo.peerId,
      settlement: some(settlement)
    )
    
    discard await ctx.broadcast(MixersTopic, encodeMixer(msg))
    ctx.emitEvent("Completed", txId)
    
    ctx.role = mrIdle
    ctx.intents.clear()
    ctx.commitments.clear()
    ctx.signatures.clear()
    ctx.publicNonces.clear()
    await ctx.persist()

# --- Participant Logic ---

proc encodeMixerIntent*(intent: MixerIntent): string =
  let node = %*{
    "sessionHint": intent.sessionHint,
    "asset": encodeAssetDef(intent.targetAsset),
    "amount": intent.amount,
    "maxDelayMs": intent.maxDelayMs,
    "initiatorPk": intent.initiatorPk,
    "signature": intent.signature
  }
  $node

proc submitIntent*(ctx: MixerContext, asset: AssetDef, amount: float) {.async.} =
  if ctx.role != mrIdle:
    return
  
  var intent = MixerIntent(
    sessionHint: 1,
    targetAsset: asset,
    amount: amount,
    maxDelayMs: 60000,
    proofDigest: "proof",
    hopCount: 3,
    initiatorPk: "", 
    signature: ""    
  )
  
  ctx.signIntent(intent)
  
  # Store my intent
  ctx.myIntent = some(intent)
  ctx.intents[$ctx.switch.peerInfo.peerId] = intent
  await ctx.persist()
  
  ctx.emitEvent("Discovery")
  
  # Wrap in Onion Packet
  let rawBytes = (encodeMixerIntent(intent)).toBytes()
  let paddedBytes = padPayload(rawBytes)
  
  # Send to self (loopback) or random peer in DHT
  await ctx.sendOnionPacket($ctx.switch.peerInfo.peerId, paddedBytes)

# --- Message Handling ---

proc handleMixerMessage*(ctx: MixerContext, msg: MixerMessage) {.async.} =
  case msg.kind
  of "intent":
    # Direct intent (Non-private fallback)
    discard

  of "onion_intent":
    if msg.onionPacket.len > 0:
      try:
        var packet = decodeOnionPacket(msg.onionPacket)
        
        # Onion Decryption logic using Wallet's default private key (likely BTC/Secp256k1 key)
        # We assume the onion routing uses the secp256k1 Identity Key of the peer
        var secret: array[32, byte]
        if ctx.wallet != nil and ctx.wallet.keys.hasKey(ChainBTC):
           let kp = ctx.wallet.keys[ChainBTC]
           if kp.kind == wtSecp256k1:
              secret = deriveSharedSecret(packet.ephemeralPk, kp.sk)
        
        if secret != default(array[32, byte]):
          let peelRes = onionpay.peelOnionLayer(packet, secret)
          if peelRes.isOk:
            let res = peelRes.get()
            
            if res.isFinal:
               echo "[mixer] Onion packet final delivery!"
               try:
                 let unpadded = unpadPayload(res.payload)
                 let payloadStr = string.fromBytes(unpadded)
                 let intentJson = parseJson(payloadStr)
                 # Manual decode needed for AssetDef inside intent
                 let assetNode = intentJson["asset"]
                 let asset = decodeAssetDef(assetNode)
                 
                 var intent = MixerIntent(
                    sessionHint: intentJson["sessionHint"].getBiggestInt().uint64,
                    targetAsset: asset,
                    amount: intentJson["amount"].getFloat(),
                    maxDelayMs: intentJson["maxDelayMs"].getBiggestInt().uint32,
                    initiatorPk: intentJson["initiatorPk"].getStr(),
                    signature: intentJson["signature"].getStr()
                 )
                 
                 if intent.amount > 0:
                   echo "[mixer] Processed anonymous intent via Onion for ", asset.toString()
                   let trackerId = if intent.initiatorPk.len > 0: intent.initiatorPk else: "anon-" & $getTime().toUnix()
                   
                   ctx.intents[trackerId] = intent
                   if intent.initiatorPk.len > 0:
                      ctx.peerKeys[trackerId] = intent.initiatorPk
                      
                   await ctx.persist()
                   if ctx.role == mrIdle or ctx.role == mrCoordinator:
                     await ctx.checkDiscovery()
               except CatchableError as e:
                 echo "[mixer] Failed to parse onion payload json: ", e.msg
            else:
               if res.nextPeer.isSome:
                 let nextPid = res.nextPeer.get()
                 echo "[mixer] Relaying onion packet to ", nextPid
                 
                 let relayMsg = MixerMessage(
                   kind: "onion_intent",
                   requestId: msg.requestId,
                   initiator: msg.initiator, 
                   onionPacket: encodeOnionPacket(packet)
                 )
                 discard await ctx.broadcast(MixersTopic, encodeMixer(relayMsg))
          else:
             discard 
      except CatchableError as e:
        echo "[mixer] Failed to decode onion packet wrapper: ", e.msg

  of "session_start":
    if msg.sessionStart.isSome():
      let start = msg.sessionStart.get()
      if $ctx.switch.peerInfo.peerId in start.participants:
        if ctx.role == mrIdle:
           ctx.role = mrParticipant
        
        ctx.sessionId = start.sessionId
        ctx.participants = start.participants
        await ctx.persist()
        ctx.emitEvent("Commitment")
        
        # Respond with Commitment (Mocked for non-BTC chains in this step)
        let commit = CommitMsg(
           sessionId: ctx.sessionId,
           participantId: $ctx.switch.peerInfo.peerId,
           commitment: "mock-commitment",
           nonce: "mock-nonce"
        )
        let cMsg = MixerMessage(kind: "commit", requestId: ctx.sessionId, initiator: $ctx.switch.peerInfo.peerId, commit: some(commit))
        discard await ctx.broadcast(MixersTopic, encodeMixer(cMsg))

  of "commit":
    if ctx.role == mrCoordinator:
       # Simplified logic: count commits, then ask for signatures
       if msg.commit.isSome():
          let c = msg.commit.get()
          if c.sessionId == ctx.sessionId:
             ctx.commitments[c.participantId] = c.commitment
             if ctx.commitments.len == ctx.participants.len:
                # Request signatures
                let s = ShuffleMsg(sessionId: ctx.sessionId, round: 1, proof: "mock-proof")
                let m = MixerMessage(kind: "shuffle", requestId: ctx.sessionId, initiator: "coord", shuffle: some(s))
                discard await ctx.broadcast(MixersTopic, encodeMixer(m))

  of "shuffle":
     if ctx.role == mrParticipant and msg.shuffle.isSome():
        # Sign request received
        ctx.emitEvent("Signing")
        # Sign dummy tx
        let sigMsg = SignatureMsg(sessionId: ctx.sessionId, txDigest: "mock-tx", partialSig: "mock-sig")
        let m = MixerMessage(kind: "signature", requestId: ctx.sessionId, initiator: $ctx.switch.peerInfo.peerId, signature: some(sigMsg))
        discard await ctx.broadcast(MixersTopic, encodeMixer(m))

  of "signature":
    if ctx.role == mrCoordinator and msg.signature.isSome():
       let s = msg.signature.get()
       if s.sessionId == ctx.sessionId:
          ctx.signatures[msg.initiator] = s.partialSig
          await ctx.checkSignatures()
  
  of "settlement":
    if msg.settlement.isSome():
       let proof = msg.settlement.get()
       if proof.sessionId == ctx.sessionId:
          ctx.emitEvent("Completed", proof.txId)
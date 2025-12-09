# Nim-Libp2p
# Copyright (c) 2025 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import std/[tables, sequtils, strutils]
import nimcrypto/sha2
import stew/byteutils
import results

import ./types
import ../secp # Import Secp Wrapper for SkKeyPair

when defined(libp2p_coinjoin):
  import secp256k1/musig
  import secp256k1/abi

proc aggregatePubKeys*(
    pubKeys: seq[seq[byte]],
    outKeyAggCache: var seq[byte],
    outAggPk: var seq[byte]
): CoinJoinResult[void] =
  when defined(libp2p_coinjoin):
    if not coinJoinSupportEnabled():
      return errDisabled[void]("aggregatePubKeys")
      
    if pubKeys.len == 0:
      return err(newCoinJoinError(cjInvalidInput, "no public keys"))
      
    var parsedPks: seq[secp256k1_pubkey] = newSeq[secp256k1_pubkey](pubKeys.len)
    var parsedPtrs: seq[ptr secp256k1_pubkey] = newSeq[ptr secp256k1_pubkey](pubKeys.len)
    
    for i, raw in pubKeys:
      if secp256k1_ec_pubkey_parse(secp256k1_context_no_precomp, addr parsedPks[i], unsafeAddr raw[0], csize_t(raw.len)) != 1:
        return err(newCoinJoinError(cjInvalidInput, "invalid pubkey"))
      parsedPtrs[i] = addr parsedPks[i]
      
    var cache: MusigKeyAggCache
    var aggPk: secp256k1_xonly_pubkey
    
    if secp256k1_musig_pubkey_agg(secp256k1_context_no_precomp, addr aggPk, addr cache, addr parsedPtrs[0], csize_t(pubKeys.len)) != 1:
      return err(newCoinJoinError(cjInternal, "key aggregation failed"))
      
    outKeyAggCache = @(cache.data)
    
    var serializedAggPk: array[32, byte]
    if secp256k1_xonly_pubkey_serialize(secp256k1_context_no_precomp, addr serializedAggPk[0], addr aggPk) != 1:
      return err(newCoinJoinError(cjInternal, "agg pubkey serialization failed"))
      
    outAggPk = @serializedAggPk
    ok()
  else:
    return errDisabled[void]("aggregatePubKeys")

proc initSession*(
    aggNonce: seq[byte],
    msg: seq[byte],
    keyAggCache: seq[byte],
    outSession: var seq[byte]
): CoinJoinResult[void] =
  when defined(libp2p_coinjoin):
    if not coinJoinSupportEnabled():
      return errDisabled[void]("initSession")
      
    if aggNonce.len != 66: return err(newCoinJoinError(cjInvalidInput, "invalid agg nonce len"))
    if keyAggCache.len != 197: return err(newCoinJoinError(cjInvalidInput, "invalid key agg cache len"))
    if msg.len != 32: return err(newCoinJoinError(cjInvalidInput, "msg must be 32 bytes"))
    
    var nonce: MusigAggNonce
    if secp256k1_musig_aggnonce_parse(secp256k1_context_no_precomp, addr nonce, unsafeAddr aggNonce[0]) != 1:
      return err(newCoinJoinError(cjInvalidInput, "failed to parse agg nonce"))
      
    var cache: MusigKeyAggCache
    copyMem(addr cache.data[0], unsafeAddr keyAggCache[0], 197)
    
    var session: MusigSession
    if secp256k1_musig_nonce_process(secp256k1_context_no_precomp, addr session, addr nonce, unsafeAddr msg[0], addr cache) != 1:
      return err(newCoinJoinError(cjInternal, "session init failed"))
      
    outSession = @(session.data)
    ok()
  else:
    return errDisabled[void]("initSession")

proc signPartial*(
    secNonce: seq[byte],
    keyPair: SkKeyPair,
    keyAggCache: seq[byte],
    session: seq[byte],
    outPartialSig: var seq[byte]
): CoinJoinResult[void] =
  when defined(libp2p_coinjoin):
    if not coinJoinSupportEnabled():
      return errDisabled[void]("signPartial")
      
    if secNonce.len != 132: return err(newCoinJoinError(cjInvalidInput, "invalid sec nonce len"))
    if session.len != 133: return err(newCoinJoinError(cjInvalidInput, "invalid session len"))
    if keyAggCache.len != 197: return err(newCoinJoinError(cjInvalidInput, "invalid key agg cache len"))
    
    var sn: MusigSecNonce
    copyMem(addr sn.data[0], unsafeAddr secNonce[0], 132)
    
    var sess: MusigSession
    copyMem(addr sess.data[0], unsafeAddr session[0], 133)
    
    var cache: MusigKeyAggCache
    copyMem(addr cache.data[0], unsafeAddr keyAggCache[0], 197)
    
    var kp: secp256k1_keypair
    let skBytes = keyPair.seckey.getBytes()
    if secp256k1_keypair_create(secp256k1_context_no_precomp, addr kp, unsafeAddr skBytes[0]) != 1:
      return err(newCoinJoinError(cjInternal, "failed to create keypair from secret"))
      
    var ps: MusigPartialSig
    if secp256k1_musig_partial_sign(secp256k1_context_no_precomp, addr ps, addr sn, addr kp, addr cache, addr sess) != 1:
      return err(newCoinJoinError(cjInternal, "partial sign failed"))
      
    var sigBytes: array[32, byte]
    if secp256k1_musig_partial_sig_serialize(secp256k1_context_no_precomp, addr sigBytes[0], addr ps) != 1:
      return err(newCoinJoinError(cjInternal, "partial sig serialize failed"))
      
    outPartialSig = @sigBytes
    ok()
  else:
    return errDisabled[void]("signPartial")

proc startAggregation*(
    ctx: var MusigSessionCtx, participantIds: seq[string]
): CoinJoinResult[void] =
  when defined(libp2p_coinjoin):
    if not coinJoinSupportEnabled():
      return errDisabled[void]("startAggregation")
    ctx.participantIds = participantIds
    ctx.sessionId.inc
    ctx.usedNonces = initTable[string, NonceState]()
    ctx.partialSignatures = initTable[string, seq[byte]]()
    ok()
  else:
    return errDisabled[void]("startAggregation")

proc generateNoncePair*(
    outSecNonce: var seq[byte],
    outPubNonce: var seq[byte]
): CoinJoinResult[void] =
  when defined(libp2p_coinjoin):
    if not coinJoinSupportEnabled():
      return errDisabled[void]("generateNoncePair")
      
    var sec: MusigSecNonce
    var pub: MusigPubNonce
    
    var session_secrand32: array[32, byte]
    try:
      let f = open("/dev/urandom")
      if f.readBuffer(addr session_secrand32[0], 32) != 32:
        return err(newCoinJoinError(cjInternal, "RNG failed"))
      f.close()
    except:
      return err(newCoinJoinError(cjInternal, "RNG exception"))
  
    if secp256k1_musig_nonce_gen(secp256k1_context_no_precomp, addr sec, addr pub, addr session_secrand32[0], nil, nil, nil, nil, nil) != 1:
      return err(newCoinJoinError(cjInternal, "nonce generation failed"))
      
    var pubBytes: array[66, byte] 
    if secp256k1_musig_pubnonce_serialize(secp256k1_context_no_precomp, addr pubBytes[0], addr pub) != 1:
      return err(newCoinJoinError(cjInternal, "nonce serialization failed"))
      
    outSecNonce = @(sec.data)
    outPubNonce = @(pubBytes)
    ok()
  else:
    return errDisabled[void]("generateNoncePair")

proc aggregateNonces*(
    pubNonces: seq[seq[byte]],
    outAggNonce: var seq[byte]
): CoinJoinResult[void] =
  when defined(libp2p_coinjoin):
     if not coinJoinSupportEnabled():
       return errDisabled[void]("aggregateNonces")
     if pubNonces.len == 0:
       return err(newCoinJoinError(cjInvalidInput, "no nonces to aggregate"))
       
     var parsedNonces: seq[MusigPubNonce] = newSeq[MusigPubNonce](pubNonces.len)
     var parsedPtrs: seq[ptr MusigPubNonce] = newSeq[ptr MusigPubNonce](pubNonces.len)
     
     for i, raw in pubNonces:
       if raw.len != 66:
         return err(newCoinJoinError(cjInvalidInput, "invalid nonce length"))
       if secp256k1_musig_pubnonce_parse(secp256k1_context_no_precomp, addr parsedNonces[i], unsafeAddr raw[0]) != 1:
         return err(newCoinJoinError(cjInvalidInput, "invalid nonce format"))
       parsedPtrs[i] = addr parsedNonces[i]
       
     var aggNonce: MusigAggNonce
     if secp256k1_musig_nonce_agg(secp256k1_context_no_precomp, addr aggNonce, addr parsedPtrs[0], csize_t(pubNonces.len)) != 1:
       return err(newCoinJoinError(cjInternal, "nonce aggregation failed"))
       
     var aggBytes: array[66, byte]
     if secp256k1_musig_aggnonce_serialize(secp256k1_context_no_precomp, addr aggBytes[0], addr aggNonce) != 1:
       return err(newCoinJoinError(cjInternal, "agg nonce serialization failed"))
       
     outAggNonce = @(aggBytes)
     ok()
  else:
     return errDisabled[void]("aggregateNonces")

proc processPartialSignature*(
    ctx: var MusigSessionCtx, participantId: string, payload: seq[byte]
): CoinJoinResult[void] =
  when defined(libp2p_coinjoin):
    if not coinJoinSupportEnabled():
      return errDisabled[void]("processPartialSignature")
    if participantId.len == 0 or payload.len == 0:
      return err(newCoinJoinError(cjInvalidInput, "participantId/payload missing"))
    
    if participantId notin ctx.participantIds:
      return err(newCoinJoinError(cjInvalidInput, "unknown participant"))
  
    if payload.len != 32:
       return err(newCoinJoinError(cjInvalidInput, "partial signature must be 32 bytes"))
       
    var sig: MusigPartialSig
    if secp256k1_musig_partial_sig_parse(secp256k1_context_no_precomp, addr sig, unsafeAddr payload[0]) != 1:
      return err(newCoinJoinError(cjInvalidInput, "invalid partial signature format"))
  
    ctx.usedNonces[participantId] = nsReceived
    ctx.partialSignatures[participantId] = payload
    ok()
  else:
    return errDisabled[void]("processPartialSignature")

proc finalizeAggregation*(
    ctx: var MusigSessionCtx
): CoinJoinResult[seq[byte]] =
  when defined(libp2p_coinjoin):
    if not coinJoinSupportEnabled():
      return errDisabled[seq[byte]]("finalizeAggregation")
    
    if ctx.partialSignatures.len == 0:
      return err(newCoinJoinError(cjInvalidInput, "no signatures to aggregate"))

    if ctx.sessionCache.len != 133:
       return err(newCoinJoinError(cjInvalidInput, "invalid session cache"))

    var sess: MusigSession
    copyMem(addr sess.data[0], unsafeAddr ctx.sessionCache[0], 133)

    var sigs: seq[MusigPartialSig] = newSeq[MusigPartialSig](ctx.partialSignatures.len)
    var sigPtrs: seq[ptr MusigPartialSig] = newSeq[ptr MusigPartialSig](ctx.partialSignatures.len)
    
    var idx = 0
    for _, rawSig in ctx.partialSignatures:
      if rawSig.len != 32:
        return err(newCoinJoinError(cjInternal, "invalid stored signature length"))
      
      if secp256k1_musig_partial_sig_parse(secp256k1_context_no_precomp, addr sigs[idx], unsafeAddr rawSig[0]) != 1:
        return err(newCoinJoinError(cjInternal, "failed to parse stored signature"))
        
      sigPtrs[idx] = addr sigs[idx]
      idx.inc

    var finalSig: array[64, byte]
    if secp256k1_musig_partial_sig_agg(secp256k1_context_no_precomp, addr finalSig[0], addr sess, addr sigPtrs[0], csize_t(ctx.partialSignatures.len)) != 1:
      return err(newCoinJoinError(cjInternal, "signature aggregation failed"))
    
    ok(@finalSig)
  else:
    return errDisabled[seq[byte]]("finalizeAggregation")

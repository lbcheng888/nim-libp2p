import
  ./abi,
  stew/[byteutils, ctops],
  results

export results

type
  # Opaque structs - fixed size based on header
  MusigKeyAggCache* = object
    data*: array[197, byte]

  MusigSecNonce* = object
    data*: array[132, byte]

  MusigPubNonce* = object
    data*: array[132, byte]

  MusigAggNonce* = object
    data*: array[132, byte]

  MusigSession* = object
    data*: array[133, byte]

  MusigPartialSig* = object
    data*: array[36, byte]

  # Imports
  
{.push noconv, importc, cdecl.}

# Key Aggregation
proc secp256k1_musig_pubkey_agg*(
  ctx: ptr secp256k1_context,
  agg_pk: ptr secp256k1_xonly_pubkey,
  keyagg_cache: ptr MusigKeyAggCache,
  pubkeys: ptr ptr secp256k1_pubkey,
  n_pubkeys: csize_t
): cint

# Nonce Generation
proc secp256k1_musig_nonce_gen*(
  ctx: ptr secp256k1_context,
  secnonce: ptr MusigSecNonce,
  pubnonce: ptr MusigPubNonce,
  session_secrand32: ptr byte, # 32 bytes
  seckey: ptr byte, # 32 bytes optional
  pubkey: ptr secp256k1_pubkey, # optional
  msg32: ptr byte, # optional
  keyagg_cache: ptr MusigKeyAggCache, # optional
  extra_input32: ptr byte # optional
): cint

# Nonce Aggregation
proc secp256k1_musig_nonce_agg*(
  ctx: ptr secp256k1_context,
  aggnonce: ptr MusigAggNonce,
  pubnonces: ptr ptr MusigPubNonce, # array of pointers
  n_pubnonces: csize_t
): cint

# Nonce Process (Session Setup)
proc secp256k1_musig_nonce_process*(
  ctx: ptr secp256k1_context,
  session: ptr MusigSession,
  aggnonce: ptr MusigAggNonce,
  msg32: ptr byte,
  keyagg_cache: ptr MusigKeyAggCache
): cint

# Partial Sign
proc secp256k1_musig_partial_sign*(
  ctx: ptr secp256k1_context,
  partial_sig: ptr MusigPartialSig,
  secnonce: ptr MusigSecNonce,
  keypair: ptr secp256k1_keypair,
  keyagg_cache: ptr MusigKeyAggCache,
  session: ptr MusigSession
): cint

# Verify Partial Sig
proc secp256k1_musig_partial_sig_verify*(
  ctx: ptr secp256k1_context,
  partial_sig: ptr MusigPartialSig,
  pubnonce: ptr MusigPubNonce,
  pubkey: ptr secp256k1_pubkey,
  keyagg_cache: ptr MusigKeyAggCache,
  session: ptr MusigSession
): cint

# Aggregate Partial Sigs
proc secp256k1_musig_partial_sig_agg*(
  ctx: ptr secp256k1_context,
  sig64: ptr byte, # 64 bytes Schnorr sig
  session: ptr MusigSession,
  partial_sigs: ptr ptr MusigPartialSig, # array of pointers
  n_sigs: csize_t
): cint

# Serialization
proc secp256k1_musig_pubnonce_serialize*(
  ctx: ptr secp256k1_context,
  out66: ptr byte,
  nonce: ptr MusigPubNonce
): cint

proc secp256k1_musig_pubnonce_parse*(
  ctx: ptr secp256k1_context,
  nonce: ptr MusigPubNonce,
  in66: ptr byte
): cint

proc secp256k1_musig_aggnonce_serialize*(
  ctx: ptr secp256k1_context,
  out66: ptr byte,
  nonce: ptr MusigAggNonce
): cint

proc secp256k1_musig_aggnonce_parse*(
  ctx: ptr secp256k1_context,
  nonce: ptr MusigAggNonce,
  in66: ptr byte
): cint

proc secp256k1_musig_partial_sig_serialize*(
  ctx: ptr secp256k1_context,
  out32: ptr byte,
  sig: ptr MusigPartialSig
): cint

proc secp256k1_musig_partial_sig_parse*(
  ctx: ptr secp256k1_context,
  sig: ptr MusigPartialSig,
  in32: ptr byte
): cint

{.pop.}

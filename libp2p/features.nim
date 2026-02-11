# Nim-LibP2P feature flags
# Copyright

{.push raises: [].}

const
  ## Compile-time feature toggles. Pass ``-d:libp2p_disable_<feature>``
  ## to Nim to turn off the corresponding capability.
  libp2pFetchEnabled* = not defined(libp2p_disable_fetch)
  libp2pHttpEnabled* = not defined(libp2p_disable_http)
  libp2pEpisubEnabled* = not defined(libp2p_disable_episub)
  libp2pDataTransferEnabled* = not defined(libp2p_disable_datatransfer)

template disabledFeatureMessage*(featureName, flag: string): string =
  featureName & " feature disabled at compile time (use -d:" & flag & " to toggle)"

# Nim-LibP2P example: delegated routing find providers

import chronos
import libp2p/[delegatedrouting, cid]

proc main() {.async.} =
  # Replace the base URL with the delegated routing endpoint you want to target.
  let client = DelegatedRoutingClient.new(
    "https://delegated-ipfs.dev",
    protocolFilter = @["transport-bitswap"],
  )

  let cidResult = Cid.init("bafybeigdyrzt6ptr6rr7dnnvlrwzdnjq5u527csmdv4uadlfnb4k4cg37m")
  if cidResult.isErr:
    echo "failed to parse CID: ", cidResult.error()
    return

  let contentId = cidResult.get()
  try:
    let providers = await client.findProviders(contentId)
    if providers.len == 0:
      echo "no providers advertised for ", $contentId
    else:
      echo "providers for ", $contentId
      for entry in providers:
        var addrStrings: seq[string] = @[]
        for addr in entry.addresses:
          addrStrings.add($addr)
        echo "- ", $entry.peerId, " (", entry.schema, ")\n  addrs: ", addrStrings.join(", ")
  except DelegatedRoutingError as exc:
    echo "delegated routing error: ", exc.msg

when isMainModule:
  waitFor main()

import std/[base64, options, os, unittest]

import ../libp2p/lsmr
import ../libp2p/rwad/[codec, identity, node, types]
import ../libp2p/rwad/storage/store

proc makeNodeConfig(root: string, identity: NodeIdentity): RwadNodeConfig =
  let identityPath = root / "identity.json"
  let genesisPath = root / "genesis.json"
  saveIdentity(identityPath, identity)
  saveGenesis(genesisPath, createDefaultGenesis(identity, "rwad-blob-test"))
  RwadNodeConfig(
    dataDir: root,
    listenAddrs: @[],
    bootstrapAddrs: @[],
    legacyBootstrapAddrs: @[],
    lsmrBootstrapAddrs: @[],
    identityPath: identityPath,
    genesisPath: genesisPath,
    consensusTickMs: 50,
    routingMode: legacyOnly,
    primaryPlane: legacy,
    lsmrConfig: none(LsmrConfig),
  )

proc chunk(index: int, cid: string, bytes: string, mime = "application/octet-stream"): ManifestChunk =
  ManifestChunk(
    index: index,
    cid: cid,
    chunkHash: hashHex(bytes),
    size: bytes.len,
    mime: mime,
  )

suite "RWAD blob access":
  test "public, ticketed, preview-only, and range reads obey manifest policy":
    let identity = newNodeIdentity()
    let root = getTempDir() / ("rwad-blob-" & identity.account[0 .. 7])
    createDir(root)
    let node = newRwadNode(makeNodeConfig(root, identity))
    defer:
      node.store.close()
      removeDir(root)

    let publicBytes = "hello-public-blob"
    let publicManifest = ContentManifest(
      version: 1,
      manifestCid: "manifest-public",
      contentHash: hashHex(publicBytes),
      totalSize: publicBytes.len,
      chunkSize: publicBytes.len,
      mime: "text/plain",
      createdAt: 1,
      snapshotAt: 1,
      previewCid: "preview-public",
      thumbnailCid: "",
      accessPolicy: "public",
      originHolder: identity.account,
      readOnly: true,
      authorSignature: "sig-public",
      chunks: @[chunk(0, "cid-public-0", publicBytes, "text/plain")],
      tickets: @[],
    )
    node.storeBlob(publicManifest, @[encode(publicBytes)])
    check node.blobHead("manifest-public").isSome()
    check node.blobChunkBase64("manifest-public", 0).get() == encode(publicBytes)
    check node.blobRangeBase64("manifest-public", 0, 5).get() == encode("hello")

    let ticketedBytes = "ticketed-blob"
    let ticketedManifest = ContentManifest(
      version: 1,
      manifestCid: "manifest-ticketed",
      contentHash: hashHex(ticketedBytes),
      totalSize: ticketedBytes.len,
      chunkSize: ticketedBytes.len,
      mime: "text/plain",
      createdAt: 2,
      snapshotAt: 2,
      previewCid: "preview-ticketed",
      thumbnailCid: "",
      accessPolicy: "followers",
      originHolder: identity.account,
      readOnly: true,
      authorSignature: "sig-ticketed",
      chunks: @[chunk(0, "cid-ticketed-0", ticketedBytes, "text/plain")],
      tickets: @[
        CapabilityTicket(
          ticketId: "ticket-1",
          reader: "reader-1",
          expiresAt: 100,
          readCountLimit: 2,
          allowForward: false,
          allowPreviewOnly: false,
        )
      ],
    )
    node.storeBlob(ticketedManifest, @[encode(ticketedBytes)])
    check node.blobHead("manifest-ticketed").isNone()
    check node.blobHead("manifest-ticketed", "reader-1", "ticket-1", 10).isSome()
    check node.blobChunkBase64("manifest-ticketed", 0, "reader-1", "ticket-1", 11).isSome()
    check node.blobRangeBase64("manifest-ticketed", 0, 4, "reader-1", "ticket-1", 12).isNone()

    let previewBytes = "preview-only-blob"
    let previewManifest = ContentManifest(
      version: 1,
      manifestCid: "manifest-preview",
      contentHash: hashHex(previewBytes),
      totalSize: previewBytes.len,
      chunkSize: previewBytes.len,
      mime: "text/plain",
      createdAt: 3,
      snapshotAt: 3,
      previewCid: "preview-preview",
      thumbnailCid: "thumb-preview",
      accessPolicy: "ticket",
      originHolder: identity.account,
      readOnly: true,
      authorSignature: "sig-preview",
      chunks: @[chunk(0, "cid-preview-0", previewBytes, "text/plain")],
      tickets: @[
        CapabilityTicket(
          ticketId: "preview-ticket",
          reader: "reader-2",
          expiresAt: 100,
          readCountLimit: 0,
          allowForward: false,
          allowPreviewOnly: true,
        )
      ],
    )
    node.storeBlob(previewManifest, @[encode(previewBytes)])
    check node.blobHead("manifest-preview", "reader-2", "preview-ticket", 20).isSome()
    check node.blobChunkBase64("manifest-preview", 0, "reader-2", "preview-ticket", 21).isNone()
    check node.blobRangeBase64("manifest-preview", 0, 7, "reader-2", "preview-ticket", 22).isNone()

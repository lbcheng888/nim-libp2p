## nim-libp2p mobile updater manifest definitions

import std/[json, options, tables]
import pkg/results
import stew/byteutils
import ../libp2p/signed_envelope

type
  ManifestArtifact* = object
    platform*: string
    kind*: string
    cid*: string
    sizeBytes*: uint64
    hash*: string
    url*: string
    metadata*: Table[string, string]

  Manifest* = object
    channel*: string
    version*: string
    sequence*: uint64
    publishedAt*: string
    manifestCid*: string
    previousCid*: string
    artifacts*: seq[ManifestArtifact]
    metadata*: Table[string, string]

  SignedManifest* = SignedPayload[Manifest]

const
  ManifestPayloadDomain* = "io.unimaker.mobile.updater"
  ManifestPayloadType* = "/unimaker/mobile/manifest/1.0.0"

proc payloadDomain*(T: typedesc[Manifest]): string = ManifestPayloadDomain
proc payloadType*(T: typedesc[Manifest]): seq[byte] = ManifestPayloadType.toBytes()

proc artifactToJson(artifact: ManifestArtifact): JsonNode =
  result = newJObject()
  result["platform"] = newJString(artifact.platform)
  result["kind"] = newJString(artifact.kind)
  result["cid"] = newJString(artifact.cid)
  result["sizeBytes"] = newJInt(artifact.sizeBytes.int64)
  if artifact.hash.len > 0:
    result["hash"] = newJString(artifact.hash)
  if artifact.url.len > 0:
    result["url"] = newJString(artifact.url)
  if artifact.metadata.len > 0:
    var meta = newJObject()
    for k, v in artifact.metadata.pairs():
      meta[k] = newJString(v)
    result["metadata"] = meta

proc manifestToJson(manifest: Manifest): JsonNode =
  result = newJObject()
  result["channel"] = newJString(manifest.channel)
  result["version"] = newJString(manifest.version)
  result["sequence"] = newJInt(manifest.sequence.int64)
  result["publishedAt"] = newJString(manifest.publishedAt)
  result["manifestCid"] = newJString(manifest.manifestCid)
  result["previousCid"] = newJString(manifest.previousCid)

  var arr = newJArray()
  for artifact in manifest.artifacts:
    arr.add(artifactToJson(artifact))
  result["artifacts"] = arr

  if manifest.metadata.len > 0:
    var meta = newJObject()
    for k, v in manifest.metadata.pairs():
      meta[k] = newJString(v)
    result["metadata"] = meta

proc encode*(manifest: Manifest): seq[byte] =
  ($manifestToJson(manifest)).toBytes()

proc expectString(
    node: JsonNode, key: string, mandatory: bool
): Result[string, cstring] {.raises: [].} =
  if node.kind != JObject:
    return err(cstring("manifest field `" & key & "` missing"))
  for k, v in node.pairs():
    if k == key:
      if v.kind != JString:
        return err(cstring("manifest field `" & key & "` has invalid type"))
      return ok(v.getStr())
  if mandatory:
    return err(cstring("manifest field `" & key & "` missing"))
  ok("")

proc expectUint64(
    node: JsonNode, key: string, mandatory: bool, defaultValue: uint64 = 0
): Result[uint64, cstring] {.raises: [].} =
  if node.kind != JObject:
    return err(cstring("manifest field `" & key & "` missing"))
  for k, v in node.pairs():
    if k == key:
      case v.kind
      of JInt:
        return ok(uint64(v.getInt()))
      of JFloat:
        return ok(uint64(v.getFloat()))
      else:
        return err(cstring("manifest field `" & key & "` has invalid type"))
  if mandatory:
    return err(cstring("manifest field `" & key & "` missing"))
  ok(defaultValue)

proc findChild(node: JsonNode, key: string): Option[JsonNode] {.raises: [].} =
  if node.kind != JObject:
    return none(JsonNode)
  for k, v in node.pairs():
    if k == key:
      return some(v)
  none(JsonNode)

proc decodeArtifact(node: JsonNode): Result[ManifestArtifact, cstring] {.raises: [].} =
  if node.kind != JObject:
    return err(cstring("manifest.artifacts elements must be objects"))

  var art = ManifestArtifact(
    platform: ?node.expectString("platform", true),
    kind: ?node.expectString("kind", true),
    cid: ?node.expectString("cid", true),
    sizeBytes: ?node.expectUint64("sizeBytes", false, 0),
    hash: ?node.expectString("hash", false),
    url: ?node.expectString("url", false),
    metadata: initTable[string, string](),
  )

  let metaOpt = node.findChild("metadata")
  if metaOpt.isSome:
    let metaNode = metaOpt.get()
    if metaNode.kind != JObject:
      return err(cstring("artifact.metadata must be an object"))
    for k, v in metaNode.pairs():
      if v.kind == JString:
        art.metadata[k] = v.getStr()
      else:
        art.metadata[k] = $v

  ok(art)

proc decode*(
    T: typedesc[Manifest], buffer: seq[byte]
): Result[Manifest, cstring] {.raises: [].} =
  let rawStr =
    try:
      string.fromBytes(buffer)
    except CatchableError as exc:
      return err(cstring("failed to convert manifest bytes to string: " & exc.msg))

  let root =
    try:
      parseJson(rawStr)
    except CatchableError as exc:
      return err(cstring("failed to parse manifest JSON: " & exc.msg))

  if root.kind != JObject:
    return err(cstring("manifest root must be a JSON object"))

  var manifest = Manifest(
    channel: ?root.expectString("channel", true),
    version: ?root.expectString("version", true),
    sequence: ?root.expectUint64("sequence", true, 0),
    publishedAt: ?root.expectString("publishedAt", true),
    manifestCid: ?root.expectString("manifestCid", true),
    previousCid: ?root.expectString("previousCid", false),
    artifacts: @[],
    metadata: initTable[string, string](),
  )

  if manifest.sequence == 0:
    return err(cstring("manifest.sequence must be a positive integer"))

  let artifactsOpt = root.findChild("artifacts")
  if artifactsOpt.isSome:
    let arr = artifactsOpt.get()
    if arr.kind != JArray:
      return err(cstring("manifest.artifacts must be an array"))
    for child in arr.items:
      manifest.artifacts.add(?decodeArtifact(child))

  let metaOpt = root.findChild("metadata")
  if metaOpt.isSome:
    let metaNode = metaOpt.get()
    if metaNode.kind != JObject:
      return err(cstring("manifest.metadata must be an object"))
    for k, v in metaNode.pairs():
      if v.kind == JString:
        manifest.metadata[k] = v.getStr()
      else:
        manifest.metadata[k] = $v

  ok(manifest)

proc checkValid*(payload: SignedManifest): Result[void, EnvelopeError] =
  if payload.data.channel.len == 0 or payload.data.version.len == 0:
    return err(EnvelopeInvalidProtobuf)
  if payload.data.sequence == 0:
    return err(EnvelopeInvalidSignature)
  ok()

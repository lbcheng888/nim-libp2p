## Mock GraphSync data store for component testing.
##
## This server exposes a minimal HTTP API that mimics retrieving DAG blocks by
## CID and traversing them with a depth-first selector. It allows libp2p's
## GraphSync adapter and DataTransfer integration to run against deterministic
## responses without standing up a full IPLD/IPFS stack.

import std/[json, os, strformat]
import std/asynchttpserver
import std/asyncdispatch

type
  DagLink = object
    name: string
    cid: string

  DagNode = object
    data: string
    links: seq[DagLink]

  Dag = object
    root: string
    nodes: Table[string, DagNode]

var dag: Dag

proc loadDag(path: string): Dag =
  let parsed = parseFile(path)
  result.root = parsed["root"].getStr()
  for (cid, node) in parsed["nodes"].pairs:
    var links: seq[DagLink] = @[]
    for link in node["Links"].items:
      links.add(DagLink(name: link["Name"].getStr(), cid: link["Cid"].getStr()))
    result.nodes[cid] = DagNode(data: node["Data"].getStr(), links: links)

proc respond(req: Request, code: HttpCode, body: string) {.async.} =
  await req.respond(code, body, newHttpHeaders({"Content-Type": "application/json"}))

proc serveBlock(req: Request, cid: string) {.async.} =
  if cid notin dag.nodes:
    await respond(req, Http404, fmt"""{{"error":"unknown cid","cid":"{cid}"}}""")
    return
  let node = dag.nodes[cid]
  let linksJson = node.links.mapIt(%*{"name": it.name, "cid": it.cid})
  let payload = %*{"cid": cid, "data": node.data, "links": linksJson}
  await respond(req, Http200, $payload)

proc walkSelector(cid: string, depth: int): JsonNode =
  if cid notin dag.nodes or depth <= 0:
    return newJArray()
  let node = dag.nodes[cid]
  var children = newJArray()
  for link in node.links:
    let sub = %*{
      "edge": link.name,
      "cid": link.cid,
      "children": walkSelector(link.cid, depth - 1)
    }
    children.add(sub)
  children

proc serveSelector(req: Request, cid: string, depth: int) {.async.} =
  if cid notin dag.nodes:
    await respond(req, Http404, fmt"""{{"error":"unknown cid","cid":"{cid}"}}""")
    return
  let response = %*{
    "root": cid,
    "depth": depth,
    "nodes": walkSelector(cid, depth)
  }
  await respond(req, Http200, $response)

proc handler(req: Request) {.async.} =
  let path = req.url.path
  if path == "/root":
    await respond(req, Http200, %*{"root": dag.root}.toJson())
    return
  if path.startsWith("/block/"):
    let cid = path["/block/".len .. ^1]
    await serveBlock(req, cid)
    return
  if path.startsWith("/selector/"):
    let remainder = path["/selector/".len .. ^1]
    let parts = remainder.split('/')
    let depth = if parts.len > 1: parseInt(parts[1]) else: 2
    await serveSelector(req, parts[0], depth)
    return
  await respond(req, Http404, """{"error":"not found"}""")

when isMainModule:
  let fixturePath = getEnv("GRAPH_SYNC_FIXTURE", "tests/integration/graphsync/fixtures/dag.json")
  dag = loadDag(fixturePath)
  let server = newAsyncHttpServer()
  echo "GraphSync mock store listening on http://127.0.0.1:9797"
  waitFor server.serve(Port(9797), handler)

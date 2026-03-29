import std/[os, options]

import ../libp2p/fabric/storage/store

let base = paramStr(1)
for idx in 0 .. 3:
  let root = base / ("node-" & $idx) / "fabric"
  let s = newFabricStore(root)
  let latest = s.loadLatestCheckpoint()
  echo idx, " events=", s.loadAllEvents().len,
    " certs=", s.loadAllEventCertificates().len,
    " candidates=", s.loadAllCheckpointCandidates().len,
    " votes=", s.loadAllCheckpointVotes().len,
    " checkpoints=", s.loadAllCheckpoints().len,
    " latest=", (if latest.isSome(): latest.get().checkpointId else: "none")
  s.close()

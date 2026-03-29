{.used.}

import std/[json, strutils, tables]

import unittest2

import ../examples/mobile_ffi/game_mesh_core

proc relayGameMessages(senderRt, receiverRt: GameMeshRuntime, senderPeerId, receiverPeerId,
                       conversationId, label: string, outbound: seq[GameOutboundMessage]) =
  var pendingForward = outbound
  var pendingBackward: seq[GameOutboundMessage] = @[]
  var iteration = 0
  while pendingForward.len > 0 or pendingBackward.len > 0:
    inc(iteration)
    doAssert iteration <= 32, label & ": relay loop exceeded"
    if pendingForward.len > 0:
      let current = pendingForward
      pendingForward = @[]
      for index, msg in current:
        let applied = handleIncomingGameMessage(
          receiverRt,
          senderPeerId,
          conversationId,
          label & "-forward-" & $iteration & "-" & $index,
          msg.payload,
          receiverPeerId
        )
        doAssert applied.ok, label & ": forward relay failed: " & applied.error
        pendingBackward.add(applied.outbound)
    if pendingBackward.len > 0:
      let current = pendingBackward
      pendingBackward = @[]
      for index, msg in current:
        let applied = handleIncomingGameMessage(
          senderRt,
          receiverPeerId,
          conversationId,
          label & "-backward-" & $iteration & "-" & $index,
          msg.payload,
          senderPeerId
        )
        doAssert applied.ok, label & ": backward relay failed: " & applied.error
        pendingForward.add(applied.outbound)

suite "Game mesh core deterministic room init":
  test "chess invite room creation and replay verification do not overflow":
    let rt = newGameMeshRuntime()
    let result = createInviteRoom(
      rt,
      "chess",
      "12D3KooWLocalPeerExample1111111111111111111111111111",
      "12D3KooWRemotePeerExample2222222222222222222222222222",
      "dm:12D3KooWRemotePeerExample2222222222222222222222222222",
      @["/ip4/192.168.3.4/tcp/4001/p2p/12D3KooWRemotePeerExample2222222222222222222222222222"],
      "",
      sendInvite = false
    )

    check result.ok
    check result.error.len == 0
    check result.roomId.startsWith("chess_")
    check result.statePayload.kind == JObject
    check result.statePayload["stateHash"].getStr().len > 0
    check result.statePayload["state"]["board"].kind == JArray

    let replay = replayVerify(rt, "chess", result.roomId)
    check replay["ok"].getBool()
    check replay["stateHash"].getStr() == result.statePayload["stateHash"].getStr()

  test "doudizhu deterministic setup and replay verification do not overflow":
    let deck = deterministicDdzDeck("codex-ddz-seed")
    check deck.len == 54
    check deterministicTurn("codex-ddz-seed") in 0..2

    let rt = newGameMeshRuntime()
    let result = createInviteRoom(
      rt,
      "doudizhu",
      "12D3KooWLocalPeerExample1111111111111111111111111111",
      "12D3KooWRemotePeerExample2222222222222222222222222222",
      "dm:12D3KooWRemotePeerExample2222222222222222222222222222",
      @["/ip4/192.168.3.4/udp/4001/quic-v1/p2p/12D3KooWRemotePeerExample2222222222222222222222222222"],
      "codex-ddz-seed",
      sendInvite = false
    )

    check result.ok
    check result.error.len == 0
    check result.roomId.startsWith("doudizhu_")
    check result.statePayload.kind == JObject
    check result.statePayload["stateHash"].getStr().len > 0
    check result.statePayload["fullState"]["players"].kind == JArray

    let replay = replayVerify(rt, "doudizhu", result.roomId)
    check replay["ok"].getBool()
    check replay["stateHash"].getStr() == result.statePayload["stateHash"].getStr()

  test "doudizhu invite room defers bot opening turn until peer joins":
    var botFirstSeed = ""
    for idx in 0 .. 128:
      let candidate = "codex-ddz-bot-first-" & $idx
      if deterministicTurn(candidate) == 2:
        botFirstSeed = candidate
        break
    check botFirstSeed.len > 0

    let rt = newGameMeshRuntime()
    let result = createInviteRoom(
      rt,
      "doudizhu",
      "12D3KooWLocalPeerExample1111111111111111111111111111",
      "12D3KooWRemotePeerExample2222222222222222222222222222",
      "dm:12D3KooWRemotePeerExample2222222222222222222222222222",
      @["/ip4/192.168.3.4/udp/4001/quic-v1/p2p/12D3KooWRemotePeerExample2222222222222222222222222222"],
      botFirstSeed,
      sendInvite = false
    )

    check result.ok
    check result.statePayload["state"]["currentTurn"].getInt() == 2
    check result.statePayload["fullState"]["currentTurn"].getInt() == 2
    check result.statePayload["events"].len == 1
    check result.statePayload["events"][0]["event"].getStr() == "start"

    let replay = replayVerify(rt, "doudizhu", result.roomId)
    check replay["ok"].getBool()
    check replay["stateHash"].getStr() == result.statePayload["stateHash"].getStr()

  test "chess guest applies host game_event and advances seq":
    let hostPeer = "12D3KooWHostPeerExample111111111111111111111111111111"
    let guestPeer = "12D3KooWGuestPeerExample222222222222222222222222222"
    let conversationId = "dm:" & guestPeer
    let hostRt = newGameMeshRuntime()
    let guestRt = newGameMeshRuntime()

    let created = createInviteRoom(
      hostRt,
      "chess",
      hostPeer,
      guestPeer,
      conversationId,
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & guestPeer],
      "",
      sendInvite = false
    )
    check created.ok

    let joined = joinRoom(
      guestRt,
      "chess",
      created.roomId,
      guestPeer,
      hostPeer,
      conversationId,
      created.matchId,
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      "p2p_room"
    )
    check joined.ok

    var hostJoinResult = GameOperationResult()
    for index, outbound in joined.outbound:
      hostJoinResult = handleIncomingGameMessage(
        hostRt,
        guestPeer,
        conversationId,
        "guest-join-" & $index,
        outbound.payload,
        hostPeer
      )
    check hostJoinResult.ok

    for index, outbound in hostJoinResult.outbound:
      discard handleIncomingGameMessage(
        guestRt,
        hostPeer,
        conversationId,
        "host-sync-" & $index,
        outbound.payload,
        guestPeer
      )

    let stepped = applyAutomationStep(hostRt, "chess", created.roomId)
    check stepped.ok

    var delivered = false
    for index, outbound in stepped.outbound:
      if outbound.payload.kind == JObject and outbound.payload{"game"}.kind == JObject and
          outbound.payload["game"]["type"].getStr() == "game_event":
        let applied = handleIncomingGameMessage(
          guestRt,
          hostPeer,
          conversationId,
          "host-move-" & $index,
          outbound.payload,
          guestPeer
        )
        check applied.ok
        delivered = true
    check delivered

    let guestState = currentStatePayload(guestRt, "chess", created.roomId)
    check guestState["seq"].getInt() == 1
    check guestState["moveCount"].getInt() == 1
    check guestState["state"]["currentSide"].getStr() == "black"

  test "chess draw and rematch consensus are runtime-owned":
    let hostPeer = "12D3KooWHostPeerConsensus11111111111111111111111111"
    let guestPeer = "12D3KooWGuestPeerConsensus222222222222222222222222"
    let conversationId = "dm:" & guestPeer
    let hostRt = newGameMeshRuntime()
    let guestRt = newGameMeshRuntime()

    let created = createInviteRoom(
      hostRt,
      "chess",
      hostPeer,
      guestPeer,
      conversationId,
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & guestPeer],
      "",
      sendInvite = false
    )
    check created.ok

    let joined = joinRoom(
      guestRt,
      "chess",
      created.roomId,
      guestPeer,
      hostPeer,
      conversationId,
      created.matchId,
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      "p2p_room"
    )
    check joined.ok
    relayGameMessages(hostRt, guestRt, hostPeer, guestPeer, conversationId, "chess-consensus-join", joined.outbound)

    let drawOffered = offerXiangqiDraw(guestRt, created.roomId)
    check drawOffered.ok
    relayGameMessages(guestRt, hostRt, guestPeer, hostPeer, conversationId, "chess-consensus-draw-offer", drawOffered.outbound)

    let hostDrawState = currentStatePayload(hostRt, "chess", created.roomId)
    let guestDrawState = currentStatePayload(guestRt, "chess", created.roomId)
    check hostDrawState["pendingDrawOfferSide"].getStr() == "black"
    check guestDrawState["pendingDrawOfferSide"].getStr() == "black"
    check hostDrawState["phase"].getStr() == "PLAYING"

    let drawAccepted = respondXiangqiDraw(hostRt, created.roomId, true)
    check drawAccepted.ok
    relayGameMessages(hostRt, guestRt, hostPeer, guestPeer, conversationId, "chess-consensus-draw-accept", drawAccepted.outbound)

    let hostFinished = currentStatePayload(hostRt, "chess", created.roomId)
    let guestFinished = currentStatePayload(guestRt, "chess", created.roomId)
    check hostFinished["phase"].getStr() == "FINISHED"
    check guestFinished["phase"].getStr() == "FINISHED"
    check hostFinished["pendingDrawOfferSide"].getStr().len == 0
    check guestFinished["pendingDrawOfferSide"].getStr().len == 0
    check guestFinished["state"]["winner"].getStr().len == 0

    let rematchOffered = offerXiangqiRematch(hostRt, created.roomId)
    check rematchOffered.ok
    relayGameMessages(hostRt, guestRt, hostPeer, guestPeer, conversationId, "chess-consensus-rematch-offer", rematchOffered.outbound)

    let rematchAccepted = respondXiangqiRematch(guestRt, created.roomId, true)
    check rematchAccepted.ok
    relayGameMessages(guestRt, hostRt, guestPeer, hostPeer, conversationId, "chess-consensus-rematch-accept", rematchAccepted.outbound)

    let hostRematched = currentStatePayload(hostRt, "chess", created.roomId)
    let guestRematched = currentStatePayload(guestRt, "chess", created.roomId)
    check hostRematched["phase"].getStr() == "PLAYING"
    check guestRematched["phase"].getStr() == "PLAYING"
    check hostRematched["moveCount"].getInt() == 0
    check guestRematched["moveCount"].getInt() == 0
    check hostRematched["pendingRematchOfferSide"].getStr().len == 0
    check guestRematched["pendingRematchOfferSide"].getStr().len == 0

  test "doudizhu guest automation step acts on guest-visible seat":
    var guestFirstSeed = ""
    for idx in 0 .. 128:
      let candidate = "codex-ddz-guest-first-" & $idx
      let rt = newGameMeshRuntime()
      let created = createInviteRoom(
        rt,
        "doudizhu",
        "12D3KooWHostPeerExample111111111111111111111111111111",
        "12D3KooWGuestPeerExample222222222222222222222222222",
        "dm:12D3KooWGuestPeerExample222222222222222222222222222",
        @["/ip4/192.168.3.4/tcp/4001/p2p/12D3KooWGuestPeerExample222222222222222222222222222"],
        candidate,
        sendInvite = false
      )
      let state = created.statePayload["state"]
      if state.kind == JObject and state{"currentTurn"}.kind == JInt and state["currentTurn"].getInt() == 1:
        guestFirstSeed = candidate
        break
    check guestFirstSeed.len > 0

    let hostPeer = "12D3KooWHostPeerExample111111111111111111111111111111"
    let guestPeer = "12D3KooWGuestPeerExample222222222222222222222222222"
    let conversationId = "dm:" & guestPeer
    let hostRt = newGameMeshRuntime()
    let guestRt = newGameMeshRuntime()

    let created = createInviteRoom(
      hostRt,
      "doudizhu",
      hostPeer,
      guestPeer,
      conversationId,
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & guestPeer],
      guestFirstSeed,
      sendInvite = false
    )
    check created.ok
    check created.statePayload["state"]["currentTurn"].getInt() == 1

    let joined = joinRoom(
      guestRt,
      "doudizhu",
      created.roomId,
      guestPeer,
      hostPeer,
      conversationId,
      created.matchId,
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      "p2p_bot_room"
    )
    check joined.ok

    var hostJoinResult = GameOperationResult()
    for index, outbound in joined.outbound:
      hostJoinResult = handleIncomingGameMessage(
        hostRt,
        guestPeer,
        conversationId,
        "ddz-guest-join-" & $index,
        outbound.payload,
        hostPeer
      )
    check hostJoinResult.ok

    for index, outbound in hostJoinResult.outbound:
      discard handleIncomingGameMessage(
        guestRt,
        hostPeer,
        conversationId,
        "ddz-host-sync-" & $index,
        outbound.payload,
        guestPeer
      )

    let guestState = currentStatePayload(guestRt, "doudizhu", created.roomId)
    check guestState["state"]["currentTurn"].getInt() == 0

    let stepped = applyAutomationStep(guestRt, "doudizhu", created.roomId)
    check stepped.ok
    check stepped.outbound.len > 0
    check stepped.outbound[0].payload["game"]["type"].getStr() == "game_event"

  test "doudizhu host join advances deferred bot opening turn":
    var botFirstSeed = ""
    for idx in 0 .. 128:
      let candidate = "codex-ddz-host-bot-after-join-" & $idx
      if deterministicTurn(candidate) == 2:
        botFirstSeed = candidate
        break
    check botFirstSeed.len > 0

    let hostPeer = "12D3KooWHostPeerExample111111111111111111111111111111"
    let guestPeer = "12D3KooWGuestPeerExample222222222222222222222222222"
    let conversationId = "dm:" & guestPeer
    let hostRt = newGameMeshRuntime()
    let guestRt = newGameMeshRuntime()

    let created = createInviteRoom(
      hostRt,
      "doudizhu",
      hostPeer,
      guestPeer,
      conversationId,
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & guestPeer],
      botFirstSeed,
      sendInvite = false
    )
    check created.ok
    check created.statePayload["state"]["currentTurn"].getInt() == 2

    let joined = joinRoom(
      guestRt,
      "doudizhu",
      created.roomId,
      guestPeer,
      hostPeer,
      conversationId,
      created.matchId,
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      "p2p_bot_room"
    )
    check joined.ok

    var hostJoinResult = GameOperationResult()
    for index, outbound in joined.outbound:
      hostJoinResult = handleIncomingGameMessage(
        hostRt,
        guestPeer,
        conversationId,
        "ddz-host-bot-join-" & $index,
        outbound.payload,
        hostPeer
      )
    check hostJoinResult.ok

    let hostState = currentStatePayload(hostRt, "doudizhu", created.roomId)
    check hostState["connected"].getBool()
    check hostState["state"]["currentTurn"].getInt() != 2
    check hostState["seq"].getInt() >= 0

  test "chess automation completes full match with consistent replay state":
    let hostPeer = "12D3KooWHostPeerFullMatch1111111111111111111111111111"
    let guestPeer = "12D3KooWGuestPeerFullMatch222222222222222222222222222"
    let conversationId = "dm:" & guestPeer
    let hostRt = newGameMeshRuntime()
    let guestRt = newGameMeshRuntime()

    let created = createInviteRoom(
      hostRt,
      "chess",
      hostPeer,
      guestPeer,
      conversationId,
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & guestPeer],
      "",
      sendInvite = false
    )
    check created.ok

    let joined = joinRoom(
      guestRt,
      "chess",
      created.roomId,
      guestPeer,
      hostPeer,
      conversationId,
      created.matchId,
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & hostPeer],
      "p2p_room"
    )
    check joined.ok
    relayGameMessages(guestRt, hostRt, guestPeer, hostPeer, conversationId, "chess-join", joined.outbound)

    var finished = false
    for step in 0 ..< 8:
      let hostStep = applyAutomationStep(hostRt, "chess", created.roomId)
      check hostStep.ok
      relayGameMessages(hostRt, guestRt, hostPeer, guestPeer, conversationId, "chess-host-step-" & $step, hostStep.outbound)

      let guestStep = applyAutomationStep(guestRt, "chess", created.roomId)
      check guestStep.ok
      relayGameMessages(guestRt, hostRt, guestPeer, hostPeer, conversationId, "chess-guest-step-" & $step, guestStep.outbound)

      let hostState = currentStatePayload(hostRt, "chess", created.roomId)
      let guestState = currentStatePayload(guestRt, "chess", created.roomId)
      check hostRt.xiangqiSessions[created.roomId].isHost
      check not guestRt.xiangqiSessions[created.roomId].isHost
      if hostState["phase"].getStr() == "FINISHED" and guestState["phase"].getStr() == "FINISHED":
        finished = true
        check hostState["state"]["winner"].getStr() == "red"
        check guestState["state"]["winner"].getStr() == "red"
        check hostState["moveCount"].getInt() == 3
        check guestState["moveCount"].getInt() == 3
        check hostState["stateHash"].getStr() == guestState["stateHash"].getStr()
        check hostState["replayOk"].getBool()
        check guestState["replayOk"].getBool()
        let hostReplay = replayVerify(hostRt, "chess", created.roomId)
        let guestReplay = replayVerify(guestRt, "chess", created.roomId)
        check hostReplay["ok"].getBool()
        check guestReplay["ok"].getBool()
        check hostReplay["stateHash"].getStr() == hostState["stateHash"].getStr()
        check guestReplay["stateHash"].getStr() == guestState["stateHash"].getStr()
        break
    check finished

  test "doudizhu automation completes full match with consistent replay state":
    let hostPeer = "12D3KooWHostPeerFullMatch3333333333333333333333333333"
    let guestPeer = "12D3KooWGuestPeerFullMatch444444444444444444444444444"
    let conversationId = "dm:" & guestPeer
    let hostRt = newGameMeshRuntime()
    let guestRt = newGameMeshRuntime()

    let created = createInviteRoom(
      hostRt,
      "doudizhu",
      hostPeer,
      guestPeer,
      conversationId,
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & guestPeer],
      "host-runner-ddz-seed",
      sendInvite = false
    )
    check created.ok

    let joined = joinRoom(
      guestRt,
      "doudizhu",
      created.roomId,
      guestPeer,
      hostPeer,
      conversationId,
      created.matchId,
      @["/ip4/192.168.3.5/tcp/4001/p2p/" & guestPeer],
      @["/ip4/192.168.3.4/tcp/4001/p2p/" & hostPeer],
      "p2p_bot_room"
    )
    check joined.ok
    relayGameMessages(guestRt, hostRt, guestPeer, hostPeer, conversationId, "ddz-join", joined.outbound)

    var finished = false
    for step in 0 ..< 96:
      let hostStep = applyAutomationStep(hostRt, "doudizhu", created.roomId)
      check hostStep.ok
      relayGameMessages(hostRt, guestRt, hostPeer, guestPeer, conversationId, "ddz-host-step-" & $step, hostStep.outbound)

      let guestStep = applyAutomationStep(guestRt, "doudizhu", created.roomId)
      check guestStep.ok
      relayGameMessages(guestRt, hostRt, guestPeer, hostPeer, conversationId, "ddz-guest-step-" & $step, guestStep.outbound)

      let hostState = currentStatePayload(hostRt, "doudizhu", created.roomId)
      let guestState = currentStatePayload(guestRt, "doudizhu", created.roomId)
      check hostRt.doudizhuSessions[created.roomId].isHost
      check not guestRt.doudizhuSessions[created.roomId].isHost
      if hostState["phase"].getStr() == "FINISHED" and guestState["phase"].getStr() == "FINISHED":
        finished = true
        check hostState["stateHash"].getStr() == guestState["stateHash"].getStr()
        check hostState["replayOk"].getBool()
        check guestState["replayOk"].getBool()
        check hostState["auditValid"].getBool()
        check guestState["auditValid"].getBool()
        let hostReplay = replayVerify(hostRt, "doudizhu", created.roomId)
        let guestReplay = replayVerify(guestRt, "doudizhu", created.roomId)
        check hostReplay["ok"].getBool()
        check guestReplay["ok"].getBool()
        check hostReplay["stateHash"].getStr() == hostState["stateHash"].getStr()
        check guestReplay["stateHash"].getStr() == guestState["stateHash"].getStr()
        break
    check finished

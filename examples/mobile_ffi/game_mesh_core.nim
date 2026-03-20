import std/[algorithm, json, locks, math, options, sequtils, sets, strutils, tables, times]

const
  GameProtocolVersion* = 1
  DdzSmallJoker = 16
  DdzBigJoker = 17
  DdzTwo = 15
  DdzAce = 14
  GameSyncSampleWindow = 12
  GameSyncSampleMaxMs = 15_000'i64

type
  GameInviteDescriptor* = object
    appId*: string
    roomId*: string
    matchId*: string
    conversationId*: string
    hostPeerId*: string
    guestPeerId*: string
    seedAddrs*: seq[string]
    mode*: string

  GameReplaySummary* = object
    ok*: bool
    stateHash*: string
    detail*: string

  GameInviteRoute* = object
    appId*: string
    roomId*: string

  GameOutboundMessage* = object
    peerId*: string
    conversationId*: string
    payload*: JsonNode
    seedAddrs*: seq[string]
    source*: string

  GameOperationResult* = object
    ok*: bool
    handled*: bool
    error*: string
    appId*: string
    roomId*: string
    matchId*: string
    statePayload*: JsonNode
    uiMessage*: JsonNode
    outbound*: seq[GameOutboundMessage]
    outboundQueued*: bool
    outboundFailedSources*: seq[string]

  XiangqiPieceType* = enum
    xptKing, xptAdvisor, xptElephant, xptHorse, xptRook, xptCannon, xptPawn

  XiangqiSide* = enum
    xsRed, xsBlack

  XiangqiPhase* = enum
    xpPlaying, xpFinished

  XiangqiPiece* = object
    pieceType*: XiangqiPieceType
    side*: XiangqiSide

  XiangqiPosition* = object
    row*: int
    col*: int

  XiangqiMove* = object
    fromPos*: XiangqiPosition
    toPos*: XiangqiPosition
    captured*: Option[XiangqiPiece]

  XiangqiState* = object
    board*: seq[seq[Option[XiangqiPiece]]]
    currentSide*: XiangqiSide
    phase*: XiangqiPhase
    winner*: Option[XiangqiSide]
    moveHistory*: seq[XiangqiMove]
    check*: bool

  DdzSuit* = enum
    dsSpade, dsHeart, dsDiamond, dsClub, dsJoker

  DdzHandType* = enum
    dhtPass, dhtSingle, dhtPair, dhtTriple, dhtTriplePlusOne, dhtTriplePlusTwo,
    dhtStraight, dhtDoubleStraight, dhtAirplane, dhtAirplanePlusWings,
    dhtFourPlusTwo, dhtBomb, dhtRocket, dhtInvalid

  DdzGamePhase* = enum
    dgpWaiting, dgpBidding, dgpPlaying, dgpFinished

  DdzPlayerRole* = enum
    dprLandlord, dprFarmer

  DdzCard* = object
    suit*: DdzSuit
    rank*: int
    id*: int

  DdzHandResult* = object
    handType*: DdzHandType
    rank*: int
    length*: int
    hasLength*: bool

  DdzPlayer* = object
    id*: string
    name*: string
    hand*: seq[DdzCard]
    role*: DdzPlayerRole
    bid*: int

  DdzLastPlay* = object
    cards*: seq[DdzCard]
    playerIndex*: int
    result*: DdzHandResult

  DouDiZhuState* = object
    phase*: DdzGamePhase
    players*: seq[DdzPlayer]
    bonus*: seq[DdzCard]
    currentTurn*: int
    landlordIndex*: int
    lastPlay*: Option[DdzLastPlay]
    passCount*: int
    highestBid*: int
    highestBidder*: int
    bidRound*: int
    winner*: int

  XiangqiRoomSession* = object
    descriptor*: GameInviteDescriptor
    localPeerId*: string
    remotePeerId*: string
    isHost*: bool
    state*: XiangqiState
    seq*: int
    stateHash*: string
    connected*: bool
    replay*: GameReplaySummary
    notice*: string
    events*: seq[JsonNode]
    pendingDrawOfferSide*: Option[XiangqiSide]
    pendingRematchOfferSide*: Option[XiangqiSide]
    voiceRoomId*: string
    voiceStreamKey*: string
    voiceTitle*: string
    voiceAudioOnly*: bool
    syncSamplesMs*: seq[int64]

  DoudizhuRoomSession* = object
    descriptor*: GameInviteDescriptor
    localPeerId*: string
    remotePeerId*: string
    isHost*: bool
    localSeatAuth*: int
    state*: DouDiZhuState
    initialDeckIds*: seq[int]
    initialTurn*: int
    seq*: int
    stateHash*: string
    connected*: bool
    replay*: GameReplaySummary
    notice*: string
    events*: seq[JsonNode]
    syncSamplesMs*: seq[int64]
    auditReport*: JsonNode

  GameEnvelope* = object
    envelopeType*: string
    appId*: string
    roomId*: string
    matchId*: string
    protocolVersion*: int
    senderPeerId*: string
    seatId*: string
    seq*: int
    stateHash*: string
    sentAtMs*: int64
    payload*: JsonNode

  GameMeshRuntime* = ref object
    invites*: Table[string, GameInviteDescriptor]
    xiangqiSessions*: Table[string, XiangqiRoomSession]
    doudizhuSessions*: Table[string, DoudizhuRoomSession]
    processedInboundMessageIds*: HashSet[string]
    pendingSyncObservedAt*: Table[string, int64]

  DdzHandComposition = object
    rocket: seq[DdzCard]
    bombs: seq[seq[DdzCard]]
    airplanes: seq[seq[DdzCard]]
    triples: seq[seq[DdzCard]]
    pairs: seq[seq[DdzCard]]
    singles: seq[DdzCard]

var gameMessageIdCounter: int64 = 0
var gameMessageIdLock: Lock
initLock(gameMessageIdLock)

proc nowMillis*(): int64 =
  int64(epochTime() * 1000)

proc cloneJson*(node: JsonNode): JsonNode =
  if node.isNil:
    return newJNull()
  try:
    result = parseJson($node)
  except CatchableError:
    case node.kind
    of JObject:
      result = newJObject()
      for key, value in node:
        result[key] = cloneJson(value)
    of JArray:
      result = newJArray()
      for value in node:
        result.add(cloneJson(value))
    of JString:
      result = %node.getStr()
    of JInt:
      result = %node.getInt()
    of JFloat:
      result = %node.getFloat()
    of JBool:
      result = %node.getBool()
    of JNull:
      result = newJNull()

proc roomKey(appId, roomId: string): string =
  appId.strip() & "::" & roomId.strip()

proc stableHashHex*(raw: string): string =
  var hash = 1469598103934665603'u64
  for ch in raw:
    hash = hash xor uint64(ch.ord)
    hash = hash * 1099511628211'u64
  hash.toHex(16).toLowerAscii()

proc nextGameMessageId(descriptor: GameInviteDescriptor, envelopeType: string, seq: int,
                       stateHash: string, payload: JsonNode, seatId: string,
                       senderPeerId: string, sentAt: int64): string =
  var nonce: int64
  gameMessageIdLock.acquire()
  try:
    inc gameMessageIdCounter
    nonce = gameMessageIdCounter
  finally:
    gameMessageIdLock.release()
  let entropy = descriptor.appId & "|" &
    descriptor.roomId & "|" &
    envelopeType & "|" &
    $seq & "|" &
    stateHash & "|" &
    seatId & "|" &
    senderPeerId & "|" &
    $sentAt & "|" &
    $payload & "|" &
    nonce.toHex(16)
  "gm-" & descriptor.appId & "-" & descriptor.roomId & "-" &
    $sentAt & "-" & stableHashHex(entropy)

proc stableSeed(raw: string): uint32 =
  var hash = 2166136261'u32
  for ch in raw:
    hash = hash xor uint32(ch.ord)
    hash = hash * 16777619'u32
  if hash == 0'u32: 0x13572468'u32 else: hash

proc nextSeed(seed: uint32): uint32 =
  (seed * 1103515245'u32) + 12345'u32

proc stringList*(node: JsonNode): seq[string] =
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node:
    if item.kind == JString:
      let value = item.getStr().strip()
      if value.len > 0:
        result.add(value)

proc intList*(node: JsonNode): seq[int] =
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node:
    case item.kind
    of JInt:
      result.add(item.getInt())
    of JFloat:
      result.add(item.getFloat().int)
    else:
      discard

proc jsonGetStr(node: JsonNode, key: string, defaultValue = ""): string =
  if not node.isNil and node.kind == JObject and node.hasKey(key) and node[key].kind == JString:
    node[key].getStr()
  else:
    defaultValue

proc jsonGetBool(node: JsonNode, key: string, defaultValue = false): bool =
  if not node.isNil and node.kind == JObject and node.hasKey(key):
    case node[key].kind
    of JBool:
      node[key].getBool()
    of JString:
      let value = node[key].getStr().toLowerAscii()
      value in ["1", "true", "yes", "y"]
    else:
      defaultValue
  else:
    defaultValue

proc jsonGetInt(node: JsonNode, key: string, defaultValue = 0): int =
  if not node.isNil and node.kind == JObject and node.hasKey(key):
    case node[key].kind
    of JInt:
      node[key].getInt()
    of JFloat:
      node[key].getFloat().int
    of JString:
      try:
        parseInt(node[key].getStr())
      except ValueError:
        defaultValue
    else:
      defaultValue
  else:
    defaultValue

proc jsonGetInt64(node: JsonNode, key: string, defaultValue = 0'i64): int64 =
  if not node.isNil and node.kind == JObject and node.hasKey(key):
    case node[key].kind
    of JInt:
      node[key].getBiggestInt().int64
    of JFloat:
      node[key].getFloat().int64
    of JString:
      try:
        parseBiggestInt(node[key].getStr()).int64
      except ValueError:
        defaultValue
    else:
      defaultValue
  else:
    defaultValue

proc phaseName(phase: XiangqiPhase): string =
  case phase
  of xpPlaying: "PLAYING"
  of xpFinished: "FINISHED"

proc phaseName(phase: DdzGamePhase): string =
  case phase
  of dgpWaiting: "WAITING"
  of dgpBidding: "BIDDING"
  of dgpPlaying: "PLAYING"
  of dgpFinished: "FINISHED"

proc sideName(side: XiangqiSide): string =
  case side
  of xsRed: "red"
  of xsBlack: "black"

proc parseXiangqiSide(text: string): Option[XiangqiSide] =
  case text.strip().toLowerAscii()
  of "red": some(xsRed)
  of "black": some(xsBlack)
  else: none(XiangqiSide)

proc oppositeXiangqiSide(side: XiangqiSide): XiangqiSide =
  if side == xsRed: xsBlack else: xsRed

proc localXiangqiSide(session: XiangqiRoomSession): XiangqiSide =
  if session.isHost: xsRed else: xsBlack

proc pieceTypeName(pieceType: XiangqiPieceType): string =
  case pieceType
  of xptKing: "king"
  of xptAdvisor: "advisor"
  of xptElephant: "elephant"
  of xptHorse: "horse"
  of xptRook: "rook"
  of xptCannon: "cannon"
  of xptPawn: "pawn"

proc parsePieceType(text: string): Option[XiangqiPieceType] =
  case text.strip().toLowerAscii()
  of "king": some(xptKing)
  of "advisor": some(xptAdvisor)
  of "elephant": some(xptElephant)
  of "horse": some(xptHorse)
  of "rook": some(xptRook)
  of "cannon": some(xptCannon)
  of "pawn": some(xptPawn)
  else: none(XiangqiPieceType)

proc encodeDescriptor*(descriptor: GameInviteDescriptor): JsonNode =
  %*{
    "appId": descriptor.appId,
    "roomId": descriptor.roomId,
    "matchId": descriptor.matchId,
    "conversationId": descriptor.conversationId,
    "hostPeerId": descriptor.hostPeerId,
    "guestPeerId": descriptor.guestPeerId,
    "seedAddrs": descriptor.seedAddrs,
    "mode": descriptor.mode
  }

proc decodeDescriptor*(payload: JsonNode): Option[GameInviteDescriptor] =
  if payload.isNil or payload.kind != JObject:
    return none(GameInviteDescriptor)
  let appId = jsonGetStr(payload, "appId")
  let roomId = jsonGetStr(payload, "roomId")
  if appId.len == 0 or roomId.len == 0:
    return none(GameInviteDescriptor)
  some(GameInviteDescriptor(
    appId: appId,
    roomId: roomId,
    matchId: jsonGetStr(payload, "matchId"),
    conversationId: jsonGetStr(payload, "conversationId"),
    hostPeerId: jsonGetStr(payload, "hostPeerId"),
    guestPeerId: jsonGetStr(payload, "guestPeerId"),
    seedAddrs: stringList(payload{"seedAddrs"}),
    mode: jsonGetStr(payload, "mode", "room")
  ))

proc encodeReplay*(replay: GameReplaySummary): JsonNode =
  %*{
    "ok": replay.ok,
    "stateHash": replay.stateHash,
    "detail": replay.detail
  }

proc decodeReplay(payload: JsonNode): GameReplaySummary =
  if payload.kind != JObject:
    return GameReplaySummary()
  GameReplaySummary(
    ok: jsonGetBool(payload, "ok"),
    stateHash: jsonGetStr(payload, "stateHash"),
    detail: jsonGetStr(payload, "detail")
  )

proc buildGameInviteBody*(appId: string, roomId: string, title: string, summary: string): string =
  "[应用邀请][app:" & appId.strip() & "][room:" & roomId.strip() & "]\n" & title & " · " & summary

proc extractGameInviteRoute*(text: string): Option[GameInviteRoute] =
  let trimmed = text.strip()
  let appMarker = "[app:"
  let roomMarker = "[room:"
  let appIdx = trimmed.find(appMarker)
  let roomIdx = trimmed.find(roomMarker)
  if appIdx < 0 or roomIdx < 0:
    return none(GameInviteRoute)
  let appEnd = trimmed.find("]", appIdx + appMarker.len)
  let roomEnd = trimmed.find("]", roomIdx + roomMarker.len)
  if appEnd < 0 or roomEnd < 0:
    return none(GameInviteRoute)
  let appId = trimmed[appIdx + appMarker.len ..< appEnd].strip()
  let roomId = trimmed[roomIdx + roomMarker.len ..< roomEnd].strip()
  if appId.len == 0 or roomId.len == 0:
    return none(GameInviteRoute)
  some(GameInviteRoute(appId: appId, roomId: roomId))

proc newGameMeshRuntime*(): GameMeshRuntime =
  GameMeshRuntime(
    invites: initTable[string, GameInviteDescriptor](),
    xiangqiSessions: initTable[string, XiangqiRoomSession](),
    doudizhuSessions: initTable[string, DoudizhuRoomSession](),
    processedInboundMessageIds: initHashSet[string](),
    pendingSyncObservedAt: initTable[string, int64]()
  )

proc makePosition(row, col: int): XiangqiPosition =
  XiangqiPosition(row: row, col: col)

proc encodePosition*(pos: XiangqiPosition): JsonNode =
  %*{"row": pos.row, "col": pos.col}

proc decodePosition*(payload: JsonNode): Option[XiangqiPosition] =
  if payload.kind != JObject:
    return none(XiangqiPosition)
  let row = jsonGetInt(payload, "row", -1)
  let col = jsonGetInt(payload, "col", -1)
  if row notin 0..9 or col notin 0..8:
    return none(XiangqiPosition)
  some(XiangqiPosition(row: row, col: col))

proc xiangqiPiece(pieceType: XiangqiPieceType, side: XiangqiSide): Option[XiangqiPiece] =
  some(XiangqiPiece(pieceType: pieceType, side: side))

proc startXiangqiGame*(): XiangqiState =
  var board = newSeq[seq[Option[XiangqiPiece]]](10)
  for row in 0 ..< 10:
    board[row] = newSeq[Option[XiangqiPiece]](9)
    for col in 0 ..< 9:
      board[row][col] = none(XiangqiPiece)
  proc black(pieceType: XiangqiPieceType): Option[XiangqiPiece] = xiangqiPiece(pieceType, xsBlack)
  proc red(pieceType: XiangqiPieceType): Option[XiangqiPiece] = xiangqiPiece(pieceType, xsRed)
  board[0][0] = black(xptRook); board[0][1] = black(xptHorse); board[0][2] = black(xptElephant)
  board[0][3] = black(xptAdvisor); board[0][4] = black(xptKing); board[0][5] = black(xptAdvisor)
  board[0][6] = black(xptElephant); board[0][7] = black(xptHorse); board[0][8] = black(xptRook)
  board[2][1] = black(xptCannon); board[2][7] = black(xptCannon)
  for col in [0, 2, 4, 6, 8]:
    board[3][col] = black(xptPawn)
  board[9][0] = red(xptRook); board[9][1] = red(xptHorse); board[9][2] = red(xptElephant)
  board[9][3] = red(xptAdvisor); board[9][4] = red(xptKing); board[9][5] = red(xptAdvisor)
  board[9][6] = red(xptElephant); board[9][7] = red(xptHorse); board[9][8] = red(xptRook)
  board[7][1] = red(xptCannon); board[7][7] = red(xptCannon)
  for col in [0, 2, 4, 6, 8]:
    board[6][col] = red(xptPawn)
  XiangqiState(
    board: board,
    currentSide: xsRed,
    phase: xpPlaying,
    winner: none(XiangqiSide),
    moveHistory: @[],
    check: false
  )

proc inBounds(row, col: int): bool =
  row in 0..9 and col in 0..8

proc getPiece(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition): Option[XiangqiPiece] =
  if pos.row notin 0 ..< board.len or pos.col notin 0 ..< board[pos.row].len:
    return none(XiangqiPiece)
  board[pos.row][pos.col]

proc isOwnSide(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, side: XiangqiSide): bool =
  let piece = getPiece(board, pos)
  piece.isSome() and piece.get().side == side

proc cloneBoard(board: seq[seq[Option[XiangqiPiece]]]): seq[seq[Option[XiangqiPiece]]] =
  result = newSeq[seq[Option[XiangqiPiece]]](board.len)
  for row in 0 ..< board.len:
    result[row] = board[row]

proc cloneXiangqiState(state: XiangqiState): XiangqiState =
  result = XiangqiState(
    board: cloneBoard(state.board),
    currentSide: state.currentSide,
    phase: state.phase,
    winner: state.winner,
    moveHistory: @[],
    check: state.check
  )
  result.moveHistory = newSeqOfCap[XiangqiMove](state.moveHistory.len)
  for move in state.moveHistory:
    result.moveHistory.add(move)

proc movePiece(board: seq[seq[Option[XiangqiPiece]]], fromPos, toPos: XiangqiPosition): seq[seq[Option[XiangqiPiece]]] =
  result = cloneBoard(board)
  result[toPos.row][toPos.col] = result[fromPos.row][fromPos.col]
  result[fromPos.row][fromPos.col] = none(XiangqiPiece)

proc countBetween(board: seq[seq[Option[XiangqiPiece]]], fromPos, toPos: XiangqiPosition): int =
  if fromPos.row == toPos.row:
    for col in min(fromPos.col, toPos.col) + 1 ..< max(fromPos.col, toPos.col):
      if board[fromPos.row][col].isSome():
        inc result
  elif fromPos.col == toPos.col:
    for row in min(fromPos.row, toPos.row) + 1 ..< max(fromPos.row, toPos.row):
      if board[row][fromPos.col].isSome():
        inc result

proc hasPawnCrossedRiver(side: XiangqiSide, row: int): bool =
  if side == xsRed: row <= 4 else: row >= 5

proc getKingMoves(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, side: XiangqiSide): seq[XiangqiPosition] =
  let rowRange = if side == xsRed: 7..9 else: 0..2
  for (dr, dc) in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
    let row = pos.row + dr
    let col = pos.col + dc
    let target = XiangqiPosition(row: row, col: col)
    if row in rowRange and col in 3..5 and not isOwnSide(board, target, side):
      result.add(target)
  let opponentRows = if side == xsRed: 0..2 else: 7..9
  for row in opponentRows:
    let piece = board[row][pos.col]
    if piece.isSome() and piece.get().pieceType == xptKing and piece.get().side != side and
        countBetween(board, pos, XiangqiPosition(row: row, col: pos.col)) == 0:
      result.add(XiangqiPosition(row: row, col: pos.col))

proc getAdvisorMoves(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, side: XiangqiSide): seq[XiangqiPosition] =
  let rowRange = if side == xsRed: 7..9 else: 0..2
  for (dr, dc) in [(1, 1), (1, -1), (-1, 1), (-1, -1)]:
    let row = pos.row + dr
    let col = pos.col + dc
    let target = XiangqiPosition(row: row, col: col)
    if row in rowRange and col in 3..5 and not isOwnSide(board, target, side):
      result.add(target)

proc getElephantMoves(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, side: XiangqiSide): seq[XiangqiPosition] =
  let rowRange = if side == xsRed: 5..9 else: 0..4
  for candidate in @[(2, 2, 1, 1), (2, -2, 1, -1), (-2, 2, -1, 1), (-2, -2, -1, -1)]:
    let (dr, dc, br, bc) = candidate
    let row = pos.row + dr
    let col = pos.col + dc
    let blockRow = pos.row + br
    let blockCol = pos.col + bc
    let target = XiangqiPosition(row: row, col: col)
    if inBounds(row, col) and row in rowRange and board[blockRow][blockCol].isNone() and
        not isOwnSide(board, target, side):
      result.add(target)

proc getHorseMoves(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, side: XiangqiSide): seq[XiangqiPosition] =
  for (lr, lc, dr, dc) in @[
    (-1, 0, -2, -1), (-1, 0, -2, 1),
    (1, 0, 2, -1), (1, 0, 2, 1),
    (0, -1, -1, -2), (0, -1, 1, -2),
    (0, 1, -1, 2), (0, 1, 1, 2)
  ]:
    let legRow = pos.row + lr
    let legCol = pos.col + lc
    let row = pos.row + dr
    let col = pos.col + dc
    let target = XiangqiPosition(row: row, col: col)
    if inBounds(row, col) and board[legRow][legCol].isNone() and not isOwnSide(board, target, side):
      result.add(target)

proc getSlidingMoves(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, side: XiangqiSide, canJump: bool): seq[XiangqiPosition] =
  for (dr, dc) in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
    var row = pos.row + dr
    var col = pos.col + dc
    var jumped = false
    while inBounds(row, col):
      let target = XiangqiPosition(row: row, col: col)
      let piece = getPiece(board, target)
      if not canJump:
        if isOwnSide(board, target, side):
          break
        result.add(target)
        if piece.isSome():
          break
      else:
        if not jumped:
          if piece.isNone():
            result.add(target)
          else:
            jumped = true
        elif piece.isSome():
          if piece.get().side != side:
            result.add(target)
          break
      row += dr
      col += dc

proc getPawnMoves(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, side: XiangqiSide): seq[XiangqiPosition] =
  let forward = if side == xsRed: -1 else: 1
  let forwardPos = XiangqiPosition(row: pos.row + forward, col: pos.col)
  if inBounds(forwardPos.row, forwardPos.col) and not isOwnSide(board, forwardPos, side):
    result.add(forwardPos)
  if hasPawnCrossedRiver(side, pos.row):
    for delta in [-1, 1]:
      let target = XiangqiPosition(row: pos.row, col: pos.col + delta)
      if inBounds(target.row, target.col) and not isOwnSide(board, target, side):
        result.add(target)

proc findKing(board: seq[seq[Option[XiangqiPiece]]], side: XiangqiSide): Option[XiangqiPosition] =
  for row in 0 ..< board.len:
    for col in 0 ..< board[row].len:
      let piece = board[row][col]
      if piece.isSome() and piece.get().pieceType == xptKing and piece.get().side == side:
        return some(XiangqiPosition(row: row, col: col))
  none(XiangqiPosition)

proc attacksFor(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition, piece: XiangqiPiece): seq[XiangqiPosition] =
  case piece.pieceType
  of xptKing:
    getKingMoves(board, pos, piece.side)
  of xptAdvisor:
    getAdvisorMoves(board, pos, piece.side)
  of xptElephant:
    getElephantMoves(board, pos, piece.side)
  of xptHorse:
    getHorseMoves(board, pos, piece.side)
  of xptRook:
    getSlidingMoves(board, pos, piece.side, false)
  of xptCannon:
    getSlidingMoves(board, pos, piece.side, true)
  of xptPawn:
    getPawnMoves(board, pos, piece.side)

proc isInCheck(board: seq[seq[Option[XiangqiPiece]]], side: XiangqiSide): bool =
  let kingOpt = findKing(board, side)
  if kingOpt.isNone():
    return true
  let king = kingOpt.get()
  let opponent = if side == xsRed: xsBlack else: xsRed
  for row in 0 ..< board.len:
    for col in 0 ..< board[row].len:
      let pieceOpt = board[row][col]
      if pieceOpt.isSome() and pieceOpt.get().side == opponent:
        let attacks = attacksFor(board, XiangqiPosition(row: row, col: col), pieceOpt.get())
        if attacks.anyIt(it == king):
          return true
  false

proc getXiangqiLegalMoves*(board: seq[seq[Option[XiangqiPiece]]], pos: XiangqiPosition): seq[XiangqiPosition] =
  let pieceOpt = getPiece(board, pos)
  if pieceOpt.isNone():
    return @[]
  let piece = pieceOpt.get()
  let rawMoves = attacksFor(board, pos, piece)
  for move in rawMoves:
    if not isInCheck(movePiece(board, pos, move), piece.side):
      result.add(move)

proc isCheckmate(board: seq[seq[Option[XiangqiPiece]]], side: XiangqiSide): bool =
  for row in 0 ..< board.len:
    for col in 0 ..< board[row].len:
      let pieceOpt = board[row][col]
      if pieceOpt.isSome() and pieceOpt.get().side == side and
          getXiangqiLegalMoves(board, XiangqiPosition(row: row, col: col)).len > 0:
        return false
  true

proc applyXiangqiMove*(state: XiangqiState, fromPos, toPos: XiangqiPosition): XiangqiState =
  if state.phase == xpFinished:
    return state
  let pieceOpt = getPiece(state.board, fromPos)
  if pieceOpt.isNone():
    return state
  let piece = pieceOpt.get()
  if piece.side != state.currentSide:
    return state
  if toPos notin getXiangqiLegalMoves(state.board, fromPos):
    return state
  let captured = state.board[toPos.row][toPos.col]
  let board = movePiece(state.board, fromPos, toPos)
  let nextSide = if state.currentSide == xsRed: xsBlack else: xsRed
  let check = isInCheck(board, nextSide)
  let winner =
    if captured.isSome() and captured.get().pieceType == xptKing:
      some(state.currentSide)
    elif check and isCheckmate(board, nextSide):
      some(state.currentSide)
    else:
      none(XiangqiSide)
  XiangqiState(
    board: board,
    currentSide: nextSide,
    phase: if winner.isSome(): xpFinished else: xpPlaying,
    winner: winner,
    moveHistory: state.moveHistory & @[XiangqiMove(fromPos: fromPos, toPos: toPos, captured: captured)],
    check: check
  )

proc encodeXiangqiState*(state: XiangqiState): JsonNode =
  var boardArr = newJArray()
  for row in state.board:
    var rowArr = newJArray()
    for pieceOpt in row:
      if pieceOpt.isSome():
        let piece = pieceOpt.get()
        rowArr.add(%*{
          "type": pieceTypeName(piece.pieceType),
          "side": sideName(piece.side)
        })
      else:
        rowArr.add(newJNull())
    boardArr.add(rowArr)
  var moveArr = newJArray()
  for move in state.moveHistory:
    var row = %*{
      "from": encodePosition(move.fromPos),
      "to": encodePosition(move.toPos)
    }
    if move.captured.isSome():
      let piece = move.captured.get()
      row["captured"] = %*{
        "type": pieceTypeName(piece.pieceType),
        "side": sideName(piece.side)
      }
    else:
      row["captured"] = newJNull()
    moveArr.add(row)
  result = %*{
    "currentSide": sideName(state.currentSide),
    "phase": phaseName(state.phase),
    "winner": (if state.winner.isSome(): sideName(state.winner.get()) else: ""),
    "check": state.check,
    "board": boardArr,
    "moveHistory": moveArr
  }

proc decodeXiangqiState*(payload: JsonNode): Option[XiangqiState] =
  if payload.kind != JObject or payload{"board"}.kind != JArray:
    return none(XiangqiState)
  var board = newSeq[seq[Option[XiangqiPiece]]](10)
  for row in 0 ..< 10:
    board[row] = newSeq[Option[XiangqiPiece]](9)
    for col in 0 ..< 9:
      board[row][col] = none(XiangqiPiece)
  let boardArray = payload["board"]
  for row in 0 ..< min(10, boardArray.len):
    let rowArray = boardArray[row]
    if rowArray.kind != JArray:
      continue
    for col in 0 ..< min(9, rowArray.len):
      let pieceNode = rowArray[col]
      if pieceNode.kind != JObject:
        continue
      let pieceType = parsePieceType(jsonGetStr(pieceNode, "type"))
      let side = parseXiangqiSide(jsonGetStr(pieceNode, "side"))
      if pieceType.isSome() and side.isSome():
        board[row][col] = some(XiangqiPiece(pieceType: pieceType.get(), side: side.get()))
  var moves: seq[XiangqiMove] = @[]
  if payload{"moveHistory"}.kind == JArray:
    for item in payload["moveHistory"]:
      let fromOpt = decodePosition(item{"from"})
      let toOpt = decodePosition(item{"to"})
      if fromOpt.isNone() or toOpt.isNone():
        continue
      var captured = none(XiangqiPiece)
      let capturedNode = item{"captured"}
      if capturedNode.kind == JObject:
        let capturedType = parsePieceType(jsonGetStr(capturedNode, "type"))
        let capturedSide = parseXiangqiSide(jsonGetStr(capturedNode, "side"))
        if capturedType.isSome() and capturedSide.isSome():
          captured = some(XiangqiPiece(pieceType: capturedType.get(), side: capturedSide.get()))
      moves.add(XiangqiMove(fromPos: fromOpt.get(), toPos: toOpt.get(), captured: captured))
  let currentSide = parseXiangqiSide(jsonGetStr(payload, "currentSide", "red")).get(xsRed)
  let phase =
    if jsonGetStr(payload, "phase", "PLAYING").toUpperAscii() == "FINISHED":
      xpFinished
    else:
      xpPlaying
  let winner = parseXiangqiSide(jsonGetStr(payload, "winner"))
  some(XiangqiState(
    board: board,
    currentSide: currentSide,
    phase: phase,
    winner: winner,
    moveHistory: moves,
    check: jsonGetBool(payload, "check")
  ))

proc xiangqiStateHash*(state: XiangqiState): string =
  stableHashHex($encodeXiangqiState(state))

proc ddzCard(suit: DdzSuit, rank, id: int): DdzCard =
  DdzCard(suit: suit, rank: rank, id: id)

proc createDdzDeckSnapshot*(): seq[DdzCard] =
  var nextId = 0
  for suit in [dsSpade, dsHeart, dsDiamond, dsClub]:
    for rank in 3..DdzTwo:
      result.add(ddzCard(suit, rank, nextId))
      inc nextId
  result.add(ddzCard(dsJoker, DdzSmallJoker, nextId))
  inc nextId
  result.add(ddzCard(dsJoker, DdzBigJoker, nextId))

proc sortDdzCards*(cards: seq[DdzCard]): seq[DdzCard] =
  result = cards
  result.sort(proc(a, b: DdzCard): int =
    result = cmp(b.rank, a.rank)
    if result == 0:
      result = cmp(a.suit.ord, b.suit.ord)
  )

proc countDdzRanks(cards: seq[DdzCard]): Table[int, int] =
  result = initTable[int, int]()
  for card in cards:
    result[card.rank] = result.getOrDefault(card.rank) + 1

proc ranksWithCount(counts: Table[int, int], wanted: int): seq[int] =
  for rank, count in counts.pairs:
    if count == wanted:
      result.add(rank)
  result.sort(system.cmp[int])

proc removeDdzCards(source, toRemove: seq[DdzCard]): seq[DdzCard] =
  var removeIds = initHashSet[int]()
  for card in toRemove:
    removeIds.incl(card.id)
  for card in source:
    if card.id notin removeIds:
      result.add(card)

proc isConsecutive(ranks: seq[int], minLen: int): bool =
  if ranks.len < minLen or ranks.anyIt(it >= DdzTwo):
    return false
  for idx in 0 ..< ranks.len - 1:
    if ranks[idx + 1] - ranks[idx] != 1:
      return false
  true

proc classifyDdzHand*(cards: seq[DdzCard]): DdzHandResult =
  let size = cards.len
  if size == 0:
    return DdzHandResult(handType: dhtPass, rank: 0, length: 0, hasLength: false)
  let counts = countDdzRanks(cards)
  let uniqueRanks = counts.len
  if size == 2 and counts.hasKey(DdzSmallJoker) and counts.hasKey(DdzBigJoker):
    return DdzHandResult(handType: dhtRocket, rank: DdzBigJoker, length: 0, hasLength: false)
  if size == 1:
    return DdzHandResult(handType: dhtSingle, rank: cards[0].rank, length: 0, hasLength: false)
  if size == 2 and uniqueRanks == 1:
    return DdzHandResult(handType: dhtPair, rank: cards[0].rank, length: 0, hasLength: false)
  if size == 3 and uniqueRanks == 1:
    return DdzHandResult(handType: dhtTriple, rank: cards[0].rank, length: 0, hasLength: false)
  if size == 4 and uniqueRanks == 1:
    return DdzHandResult(handType: dhtBomb, rank: cards[0].rank, length: 0, hasLength: false)
  if size == 4 and uniqueRanks == 2:
    let tripleRanks = ranksWithCount(counts, 3)
    if tripleRanks.len == 1:
      return DdzHandResult(handType: dhtTriplePlusOne, rank: tripleRanks[0], length: 0, hasLength: false)
  if size == 5 and uniqueRanks == 2:
    let tripleRanks = ranksWithCount(counts, 3)
    let pairRanks = ranksWithCount(counts, 2)
    if tripleRanks.len == 1 and pairRanks.len == 1:
      return DdzHandResult(handType: dhtTriplePlusTwo, rank: tripleRanks[0], length: 0, hasLength: false)
  if size == 6:
    let fourRanks = ranksWithCount(counts, 4)
    if fourRanks.len == 1:
      return DdzHandResult(handType: dhtFourPlusTwo, rank: fourRanks[0], length: 0, hasLength: false)
  if size == 8:
    let fourRanks = ranksWithCount(counts, 4)
    let pairRanks = ranksWithCount(counts, 2)
    if fourRanks.len == 1 and pairRanks.len == 2:
      return DdzHandResult(handType: dhtFourPlusTwo, rank: fourRanks[0], length: 0, hasLength: false)
  if size in 5..12 and uniqueRanks == size:
    let sortedRanks = toSeq(counts.keys)
    let ordered = sortedRanks.sorted(system.cmp[int])
    if isConsecutive(ordered, 5):
      return DdzHandResult(handType: dhtStraight, rank: ordered[^1], length: size, hasLength: true)
  if size >= 6 and size mod 2 == 0:
    let pairRanks = ranksWithCount(counts, 2)
    if pairRanks.len == size div 2 and isConsecutive(pairRanks, 3):
      return DdzHandResult(handType: dhtDoubleStraight, rank: pairRanks[^1], length: pairRanks.len, hasLength: true)
  let tripleRanks = ranksWithCount(counts, 3)
  if tripleRanks.len >= 2 and isConsecutive(tripleRanks, 2):
    let tripleCount = tripleRanks.len
    let kickers = size - tripleCount * 3
    if kickers == 0:
      return DdzHandResult(handType: dhtAirplane, rank: tripleRanks[^1], length: tripleCount, hasLength: true)
    if kickers == tripleCount:
      return DdzHandResult(handType: dhtAirplanePlusWings, rank: tripleRanks[^1], length: tripleCount, hasLength: true)
    if kickers == tripleCount * 2:
      var nonTriple = initTable[int, int]()
      for rank, rankCount in counts.pairs:
        if rank notin tripleRanks:
          nonTriple[rank] = rankCount
      var allPairs = true
      for _, rankCount in nonTriple.pairs:
        if rankCount != 2:
          allPairs = false
          break
      if allPairs and nonTriple.len == tripleCount:
        return DdzHandResult(handType: dhtAirplanePlusWings, rank: tripleRanks[^1], length: tripleCount, hasLength: true)
  DdzHandResult(handType: dhtInvalid, rank: 0, length: 0, hasLength: false)

proc canBeatDdz(current, candidate: DdzHandResult): bool =
  if candidate.handType == dhtRocket:
    return true
  if candidate.handType == dhtBomb and current.handType notin [dhtBomb, dhtRocket]:
    return true
  if candidate.handType == current.handType:
    if candidate.hasLength and current.hasLength and candidate.length != current.length:
      return false
    return candidate.rank > current.rank
  false

proc decomposeDdzHand(cards: seq[DdzCard]): DdzHandComposition =
  var current = sortDdzCards(cards)
  let smallJoker = current.filterIt(it.rank == DdzSmallJoker)
  let bigJoker = current.filterIt(it.rank == DdzBigJoker)
  if smallJoker.len > 0 and bigJoker.len > 0:
    result.rocket = @[smallJoker[0], bigJoker[0]]
    current = removeDdzCards(current, result.rocket)
  var counts = countDdzRanks(current)
  for rank in ranksWithCount(counts, 4):
    let bomb = current.filterIt(it.rank == rank)[0 ..< 4]
    result.bombs.add(bomb)
    current = removeDdzCards(current, bomb)
  counts = countDdzRanks(current)
  var tripleRanks = ranksWithCount(counts, 3)
  if tripleRanks.len >= 2:
    var sequenceLength = tripleRanks.len
    while sequenceLength >= 2:
      var matched = false
      for start in 0 .. tripleRanks.len - sequenceLength:
        let sequence = tripleRanks[start ..< start + sequenceLength]
        if not isConsecutive(sequence, 2):
          continue
        var planeCards: seq[DdzCard] = @[]
        for rank in sequence:
          let cardsForRank = current.filterIt(it.rank == rank)
          planeCards.add(cardsForRank[0 ..< 3])
        result.airplanes.add(planeCards)
        current = removeDdzCards(current, planeCards)
        counts = countDdzRanks(current)
        tripleRanks = ranksWithCount(counts, 3)
        matched = true
        break
      if matched:
        sequenceLength = tripleRanks.len
      else:
        dec sequenceLength
  counts = countDdzRanks(current)
  for rank in ranksWithCount(counts, 3):
    let tripleCards = current.filterIt(it.rank == rank)[0 ..< 3]
    result.triples.add(tripleCards)
    current = removeDdzCards(current, tripleCards)
  counts = countDdzRanks(current)
  for rank in ranksWithCount(counts, 2):
    let pairCards = current.filterIt(it.rank == rank)[0 ..< 2]
    result.pairs.add(pairCards)
    current = removeDdzCards(current, pairCards)
  result.singles = current

proc aiBidDdz*(hand: seq[DdzCard], currentHighBid: int): int =
  let counts = countDdzRanks(hand)
  var strength = 0
  if hand.anyIt(it.rank == DdzBigJoker):
    strength += 6
  if hand.anyIt(it.rank == DdzSmallJoker):
    strength += 5
  strength += counts.getOrDefault(DdzTwo) * 3
  strength += counts.getOrDefault(DdzAce)
  for _, count in counts.pairs:
    if count == 4:
      strength += 6
  if strength >= 12 and currentHighBid < 3:
    3
  elif strength >= 8 and currentHighBid < 2:
    2
  elif strength >= 5 and currentHighBid < 1:
    1
  else:
    0

proc findSmallestBeat(hand: seq[DdzCard], target: DdzHandResult, composition: DdzHandComposition): seq[DdzCard] =
  if target.handType == dhtSingle:
    for single in composition.singles:
      if single.rank > target.rank:
        return @[single]
    for pair in composition.pairs:
      if pair[0].rank > target.rank:
        return @[pair[0]]
  if target.handType == dhtPair:
    for pair in composition.pairs:
      if pair[0].rank > target.rank:
        return pair
    for triple in composition.triples:
      if triple[0].rank > target.rank:
        return triple[0 ..< 2]
  if target.handType == dhtTriple:
    for triple in composition.triples:
      if triple[0].rank > target.rank:
        return triple
  if target.handType == dhtTriplePlusOne:
    for triple in composition.triples:
      if triple[0].rank > target.rank:
        if composition.singles.len > 0:
          return triple & @[composition.singles[0]]
        if composition.pairs.len > 0:
          return triple & @[composition.pairs[0][0]]
  if target.handType == dhtTriplePlusTwo:
    for triple in composition.triples:
      if triple[0].rank > target.rank and composition.pairs.len > 0:
        return triple & composition.pairs[0]
  if target.handType == dhtSingle:
    let sorted = sortDdzCards(hand)
    if sorted.len > 0:
      for idx in countdown(sorted.high, 0):
        if sorted[idx].rank > target.rank:
          return @[sorted[idx]]
  if target.handType == dhtPair:
    let counts = countDdzRanks(hand)
    var pairRanks = ranksWithCount(counts, 2) & ranksWithCount(counts, 3) & ranksWithCount(counts, 4)
    pairRanks = pairRanks.deduplicate()
    pairRanks.sort(system.cmp[int])
    for rank in pairRanks:
      if rank > target.rank:
        return hand.filterIt(it.rank == rank)[0 ..< 2]
  @[]

proc aiSelectDdzPlay*(playerIndex: int, state: DouDiZhuState): seq[DdzCard] =
  if playerIndex notin 0 ..< state.players.len:
    return @[]
  let player = state.players[playerIndex]
  let isNewRound = state.lastPlay.isNone() or state.passCount >= 2
  let lastPlay = if isNewRound: none(DdzLastPlay) else: state.lastPlay
  let composition = decomposeDdzHand(player.hand)
  let isTeammatePlaying =
    lastPlay.isSome() and state.players[lastPlay.get().playerIndex].role == player.role
  if isNewRound:
    if composition.airplanes.len > 0:
      return composition.airplanes[0]
    if composition.triples.len > 0:
      let triple = composition.triples[0]
      if composition.singles.len > 0:
        return triple & @[composition.singles[0]]
      if composition.pairs.len > 0:
        return triple & composition.pairs[0]
      return triple
    if composition.pairs.len > 0:
      return composition.pairs[0]
    if composition.singles.len > 0:
      return @[composition.singles[0]]
    if composition.bombs.len > 0:
      return composition.bombs[0]
    if composition.rocket.len > 0:
      return composition.rocket
    if player.hand.len > 0:
      return @[player.hand[^1]]
    return @[]
  let target = lastPlay.get().result
  if isTeammatePlaying and (target.rank > DdzAce or target.handType in [dhtBomb, dhtRocket]):
    return @[]
  let beaten = findSmallestBeat(player.hand, target, composition)
  if beaten.len > 0:
    return beaten
  if not isTeammatePlaying:
    if target.handType notin [dhtRocket, dhtBomb]:
      if composition.bombs.len > 0:
        return composition.bombs[0]
      if composition.rocket.len > 0:
        return composition.rocket
    elif target.handType == dhtBomb:
      for bomb in composition.bombs:
        if bomb[0].rank > target.rank:
          return bomb
      if composition.rocket.len > 0:
        return composition.rocket
  @[]

proc hiddenCards(count, seat: int): seq[DdzCard] =
  for index in 0 ..< count:
    result.add(DdzCard(suit: dsJoker, rank: 3, id: 100_000 + seat * 1_000 + index))

proc startDouDiZhuGame*(): DouDiZhuState =
  let deck = createDdzDeckSnapshot()
  let players = @[
    DdzPlayer(id: "self", name: "你", hand: sortDdzCards(deck[0 ..< 17]), role: dprFarmer, bid: -1),
    DdzPlayer(id: "ai-a", name: "电脑A", hand: sortDdzCards(deck[17 ..< 34]), role: dprFarmer, bid: -1),
    DdzPlayer(id: "ai-b", name: "电脑B", hand: sortDdzCards(deck[34 ..< 51]), role: dprFarmer, bid: -1)
  ]
  DouDiZhuState(
    phase: dgpBidding,
    players: players,
    bonus: sortDdzCards(deck[51 ..< 54]),
    currentTurn: 0,
    landlordIndex: -1,
    lastPlay: none(DdzLastPlay),
    passCount: 0,
    highestBid: 0,
    highestBidder: -1,
    bidRound: 0,
    winner: -1
  )

proc deterministicDdzDeck*(seedText: string): seq[DdzCard] =
  result = createDdzDeckSnapshot()
  var seed = stableSeed(seedText)
  for index in countdown(result.high, 1):
    seed = nextSeed(seed)
    let swapIndex = int(seed mod uint32(index + 1))
    swap result[index], result[swapIndex]

proc deterministicTurn*(seedText: string): int =
  int(nextSeed(stableSeed(seedText)) mod 3'u32)

proc startDouDiZhuGameWithDeck*(deck: seq[DdzCard], currentTurn: int, playerNames = @["你", "玩家2", "玩家3"]): DouDiZhuState =
  if deck.len < 54 or playerNames.len < 3:
    return startDouDiZhuGame()
  let players = @[
    DdzPlayer(id: "seat-0", name: playerNames[0], hand: sortDdzCards(deck[0 ..< 17]), role: dprFarmer, bid: -1),
    DdzPlayer(id: "seat-1", name: playerNames[1], hand: sortDdzCards(deck[17 ..< 34]), role: dprFarmer, bid: -1),
    DdzPlayer(id: "seat-2", name: playerNames[2], hand: sortDdzCards(deck[34 ..< 51]), role: dprFarmer, bid: -1)
  ]
  DouDiZhuState(
    phase: dgpBidding,
    players: players,
    bonus: sortDdzCards(deck[51 ..< 54]),
    currentTurn: ((currentTurn mod 3) + 3) mod 3,
    landlordIndex: -1,
    lastPlay: none(DdzLastPlay),
    passCount: 0,
    highestBid: 0,
    highestBidder: -1,
    bidRound: 0,
    winner: -1
  )

proc cloneDoudizhuState(state: DouDiZhuState): DouDiZhuState =
  var players = newSeqOfCap[DdzPlayer](state.players.len)
  for player in state.players:
    players.add(DdzPlayer(
      id: player.id,
      name: player.name,
      hand: player.hand,
      role: player.role,
      bid: player.bid
    ))
  var bonus = newSeqOfCap[DdzCard](state.bonus.len)
  for card in state.bonus:
    bonus.add(card)
  let lastPlay =
    if state.lastPlay.isSome():
      let raw = state.lastPlay.get()
      some(DdzLastPlay(
        cards: raw.cards,
        playerIndex: raw.playerIndex,
        result: raw.result
      ))
    else:
      none(DdzLastPlay)
  DouDiZhuState(
    phase: state.phase,
    players: players,
    bonus: bonus,
    currentTurn: state.currentTurn,
    landlordIndex: state.landlordIndex,
    lastPlay: lastPlay,
    passCount: state.passCount,
    highestBid: state.highestBid,
    highestBidder: state.highestBidder,
    bidRound: state.bidRound,
    winner: state.winner
  )

proc cloneXiangqiSession(session: XiangqiRoomSession): XiangqiRoomSession =
  result = session
  result.descriptor.seedAddrs = session.descriptor.seedAddrs
  result.state = cloneXiangqiState(session.state)
  result.events = session.events

proc cloneDoudizhuSession(session: DoudizhuRoomSession): DoudizhuRoomSession =
  result = session
  result.descriptor.seedAddrs = session.descriptor.seedAddrs
  result.state = cloneDoudizhuState(session.state)
  result.initialDeckIds = session.initialDeckIds
  result.events = session.events
  result.auditReport = session.auditReport

proc finalizeDdzBidding(state: DouDiZhuState): DouDiZhuState =
  let landlordIndex = if state.highestBidder >= 0: state.highestBidder else: 0
  var players: seq[DdzPlayer] = @[]
  for idx, player in state.players:
    if idx == landlordIndex:
      players.add(player)
      players[^1].role = dprLandlord
      players[^1].hand = sortDdzCards(player.hand & state.bonus)
    else:
      players.add(player)
      players[^1].role = dprFarmer
  DouDiZhuState(
    phase: dgpPlaying,
    players: players,
    bonus: state.bonus,
    currentTurn: landlordIndex,
    landlordIndex: landlordIndex,
    lastPlay: none(DdzLastPlay),
    passCount: 0,
    highestBid: state.highestBid,
    highestBidder: state.highestBidder,
    bidRound: state.bidRound,
    winner: -1
  )

proc applyDdzBidForSeat*(state: DouDiZhuState, playerIndex: int, bid: int): DouDiZhuState =
  if state.phase != dgpBidding or state.currentTurn != playerIndex or playerIndex notin 0 ..< state.players.len:
    return state
  var players = state.players
  players[playerIndex].bid = clamp(bid, 0, 3)
  let highestBid = if bid > state.highestBid: bid else: state.highestBid
  let highestBidder = if bid > state.highestBid: playerIndex else: state.highestBidder
  let next = DouDiZhuState(
    phase: state.phase,
    players: players,
    bonus: state.bonus,
    currentTurn: (playerIndex + 1) mod state.players.len,
    landlordIndex: state.landlordIndex,
    lastPlay: state.lastPlay,
    passCount: state.passCount,
    highestBid: highestBid,
    highestBidder: highestBidder,
    bidRound: state.bidRound + 1,
    winner: state.winner
  )
  if next.bidRound >= state.players.len:
    finalizeDdzBidding(next)
  else:
    next

proc playDdzCardsForSeat*(state: DouDiZhuState, playerIndex: int, cards: seq[DdzCard]): DouDiZhuState =
  if state.phase != dgpPlaying or state.currentTurn != playerIndex or playerIndex notin 0 ..< state.players.len:
    return state
  let player = state.players[playerIndex]
  var ownedIds = initHashSet[int]()
  for card in player.hand:
    ownedIds.incl(card.id)
  let hiddenHand = player.hand.len > 0 and player.hand.allIt(it.id >= 100000)
  var ownsAllCards = true
  for card in cards:
    if card.id notin ownedIds:
      ownsAllCards = false
      break
  if not ownsAllCards and not hiddenHand:
    return state
  if hiddenHand and cards.len > player.hand.len:
    return state
  let candidate = classifyDdzHand(cards)
  if candidate.handType in [dhtInvalid, dhtPass]:
    return state
  let freshRound = state.lastPlay.isNone() or state.passCount >= 2
  if not freshRound and state.lastPlay.isSome() and not canBeatDdz(state.lastPlay.get().result, candidate):
    return state
  let remaining =
    if hiddenHand and not ownsAllCards:
      if cards.len >= player.hand.len:
        @[]
      else:
        player.hand[cards.len .. ^1]
    else:
      removeDdzCards(player.hand, cards)
  var players = state.players
  players[playerIndex].hand = sortDdzCards(remaining)
  let winner = if remaining.len == 0: playerIndex else: -1
  DouDiZhuState(
    phase: if winner >= 0: dgpFinished else: dgpPlaying,
    players: players,
    bonus: state.bonus,
    currentTurn: (playerIndex + 1) mod state.players.len,
    landlordIndex: state.landlordIndex,
    lastPlay: some(DdzLastPlay(cards: sortDdzCards(cards), playerIndex: playerIndex, result: candidate)),
    passCount: 0,
    highestBid: state.highestBid,
    highestBidder: state.highestBidder,
    bidRound: state.bidRound,
    winner: winner
  )

proc passDdzTurnForSeat*(state: DouDiZhuState, playerIndex: int): DouDiZhuState =
  if state.phase != dgpPlaying or state.currentTurn != playerIndex:
    return state
  if state.lastPlay.isNone() or state.passCount >= 2:
    return state
  let nextPassCount = state.passCount + 1
  DouDiZhuState(
    phase: state.phase,
    players: state.players,
    bonus: state.bonus,
    currentTurn: (playerIndex + 1) mod state.players.len,
    landlordIndex: state.landlordIndex,
    lastPlay: if nextPassCount >= 2: none(DdzLastPlay) else: state.lastPlay,
    passCount: nextPassCount,
    highestBid: state.highestBid,
    highestBidder: state.highestBidder,
    bidRound: state.bidRound,
    winner: state.winner
  )

proc encodeCard(card: DdzCard): JsonNode =
  %*{"suit": $card.suit, "rank": card.rank, "id": card.id}

proc parseSuit(text: string): Option[DdzSuit] =
  case text.strip().toUpperAscii()
  of "DSSPADE", "SPADE": some(dsSpade)
  of "DSHEART", "HEART": some(dsHeart)
  of "DSDIAMOND", "DIAMOND": some(dsDiamond)
  of "DSCLUB", "CLUB": some(dsClub)
  of "DSJOKER", "JOKER": some(dsJoker)
  else: none(DdzSuit)

proc decodeCard(payload: JsonNode): Option[DdzCard] =
  if payload.kind != JObject:
    return none(DdzCard)
  let suit = parseSuit(jsonGetStr(payload, "suit"))
  if suit.isNone():
    return none(DdzCard)
  some(DdzCard(suit: suit.get(), rank: jsonGetInt(payload, "rank"), id: jsonGetInt(payload, "id", -1)))

proc handTypeName(handType: DdzHandType): string =
  case handType
  of dhtPass: "PASS"
  of dhtSingle: "SINGLE"
  of dhtPair: "PAIR"
  of dhtTriple: "TRIPLE"
  of dhtTriplePlusOne: "TRIPLE_PLUS_ONE"
  of dhtTriplePlusTwo: "TRIPLE_PLUS_TWO"
  of dhtStraight: "STRAIGHT"
  of dhtDoubleStraight: "DOUBLE_STRAIGHT"
  of dhtAirplane: "AIRPLANE"
  of dhtAirplanePlusWings: "AIRPLANE_PLUS_WINGS"
  of dhtFourPlusTwo: "FOUR_PLUS_TWO"
  of dhtBomb: "BOMB"
  of dhtRocket: "ROCKET"
  of dhtInvalid: "INVALID"

proc parseHandType(text: string): DdzHandType =
  case text.strip().toUpperAscii()
  of "PASS": dhtPass
  of "SINGLE": dhtSingle
  of "PAIR": dhtPair
  of "TRIPLE": dhtTriple
  of "TRIPLE_PLUS_ONE": dhtTriplePlusOne
  of "TRIPLE_PLUS_TWO": dhtTriplePlusTwo
  of "STRAIGHT": dhtStraight
  of "DOUBLE_STRAIGHT": dhtDoubleStraight
  of "AIRPLANE": dhtAirplane
  of "AIRPLANE_PLUS_WINGS": dhtAirplanePlusWings
  of "FOUR_PLUS_TWO": dhtFourPlusTwo
  of "BOMB": dhtBomb
  of "ROCKET": dhtRocket
  else: dhtInvalid

proc encodeDoudizhuState*(state: DouDiZhuState): JsonNode =
  var players = newJArray()
  for player in state.players:
    players.add(%*{
      "id": player.id,
      "name": player.name,
      "role": (if player.role == dprLandlord: "LANDLORD" else: "FARMER"),
      "bid": player.bid,
      "hand": player.hand.mapIt(encodeCard(it))
    })
  var root = %*{
    "phase": phaseName(state.phase),
    "currentTurn": state.currentTurn,
    "landlordIndex": state.landlordIndex,
    "passCount": state.passCount,
    "highestBid": state.highestBid,
    "highestBidder": state.highestBidder,
    "bidRound": state.bidRound,
    "winner": state.winner,
    "bonus": state.bonus.mapIt(encodeCard(it)),
    "players": players
  }
  if state.lastPlay.isSome():
    let last = state.lastPlay.get()
    root["lastPlay"] = %*{
      "playerIndex": last.playerIndex,
      "cards": last.cards.mapIt(encodeCard(it)),
      "resultType": handTypeName(last.result.handType),
      "resultRank": last.result.rank,
      "resultLength": (if last.result.hasLength: last.result.length else: -1)
    }
  else:
    root["lastPlay"] = newJNull()
  root

proc decodeDoudizhuState*(payload: JsonNode): Option[DouDiZhuState] =
  if payload.isNil or payload.kind != JObject:
    return none(DouDiZhuState)
  let playersNode = payload{"players"}
  if playersNode.isNil or playersNode.kind != JArray:
    return none(DouDiZhuState)
  var players: seq[DdzPlayer] = @[]
  for idx in 0 ..< playersNode.len:
    let item = playersNode[idx]
    if item.kind != JObject:
      continue
    var hand: seq[DdzCard] = @[]
    let handNode = item{"hand"}
    if not handNode.isNil and handNode.kind == JArray:
      for cardNode in handNode:
        let card = decodeCard(cardNode)
        if card.isSome():
          hand.add(card.get())
    players.add(DdzPlayer(
      id: jsonGetStr(item, "id", "seat-" & $idx),
      name: jsonGetStr(item, "name", "玩家" & $(idx + 1)),
      hand: hand,
      role: if jsonGetStr(item, "role", "FARMER").toUpperAscii() == "LANDLORD": dprLandlord else: dprFarmer,
      bid: jsonGetInt(item, "bid", -1)
    ))
  var bonus: seq[DdzCard] = @[]
  let bonusNode = payload{"bonus"}
  if not bonusNode.isNil and bonusNode.kind == JArray:
    for cardNode in bonusNode:
      let card = decodeCard(cardNode)
      if card.isSome():
        bonus.add(card.get())
  var lastPlay = none(DdzLastPlay)
  let lastPlayNode = payload{"lastPlay"}
  if not lastPlayNode.isNil and lastPlayNode.kind == JObject:
    var cards: seq[DdzCard] = @[]
    let cardsNode = lastPlayNode{"cards"}
    if not cardsNode.isNil and cardsNode.kind == JArray:
      for cardNode in cardsNode:
        let card = decodeCard(cardNode)
        if card.isSome():
          cards.add(card.get())
    lastPlay = some(DdzLastPlay(
      cards: cards,
      playerIndex: jsonGetInt(lastPlayNode, "playerIndex", -1),
      result: DdzHandResult(
        handType: parseHandType(jsonGetStr(lastPlayNode, "resultType", "PASS")),
        rank: jsonGetInt(lastPlayNode, "resultRank", 0),
        length: jsonGetInt(lastPlayNode, "resultLength", 0),
        hasLength: jsonGetInt(lastPlayNode, "resultLength", -1) >= 0
      )
    ))
  some(DouDiZhuState(
    phase: case jsonGetStr(payload, "phase", "BIDDING").toUpperAscii()
      of "WAITING": dgpWaiting
      of "PLAYING": dgpPlaying
      of "FINISHED": dgpFinished
      else: dgpBidding,
    players: players,
    bonus: bonus,
    currentTurn: jsonGetInt(payload, "currentTurn"),
    landlordIndex: jsonGetInt(payload, "landlordIndex", -1),
    lastPlay: lastPlay,
    passCount: jsonGetInt(payload, "passCount"),
    highestBid: jsonGetInt(payload, "highestBid"),
    highestBidder: jsonGetInt(payload, "highestBidder", -1),
    bidRound: jsonGetInt(payload, "bidRound"),
    winner: jsonGetInt(payload, "winner", -1)
  ))

proc doudizhuStateHash*(state: DouDiZhuState): string =
  stableHashHex($encodeDoudizhuState(state))

proc doudizhuPerspectiveState*(state: DouDiZhuState, localSeatAuth: int): DouDiZhuState =
  let order = @[
    localSeatAuth,
    (localSeatAuth + 1) mod 3,
    (localSeatAuth + 2) mod 3
  ]
  var authToView = [0, 0, 0]
  for viewIdx, authIdx in order:
    authToView[authIdx] = viewIdx
  var players: seq[DdzPlayer] = @[]
  for viewIdx, authIdx in order:
    var player = state.players[authIdx]
    if viewIdx == 0:
      player.hand = sortDdzCards(player.hand)
    else:
      player.hand = hiddenCards(player.hand.len, authIdx)
    players.add(player)
  var lastPlay = none(DdzLastPlay)
  if state.lastPlay.isSome():
    let raw = state.lastPlay.get()
    lastPlay = some(DdzLastPlay(cards: raw.cards, playerIndex: authToView[raw.playerIndex], result: raw.result))
  DouDiZhuState(
    phase: state.phase,
    players: players,
    bonus: state.bonus,
    currentTurn: authToView[state.currentTurn],
    landlordIndex: if state.landlordIndex >= 0: authToView[state.landlordIndex] else: -1,
    lastPlay: lastPlay,
    passCount: state.passCount,
    highestBid: state.highestBid,
    highestBidder: state.highestBidder,
    bidRound: state.bidRound,
    winner: if state.winner >= 0: authToView[state.winner] else: -1
  )

proc encodeEnvelope(descriptor: GameInviteDescriptor, peerId: string, envelopeType: string, seq: int,
                    stateHash: string, payload: JsonNode, seatId: string, senderPeerId: string,
                    sentAtMs: int64 = 0): JsonNode =
  let sentAt = if sentAtMs > 0: sentAtMs else: nowMillis()
  %*{
    "messageId": nextGameMessageId(
      descriptor,
      envelopeType,
      seq,
      stateHash,
      payload,
      seatId,
      senderPeerId,
      sentAt
    ),
    "conversationId": (if descriptor.conversationId.len > 0: descriptor.conversationId else: "dm:" & peerId),
    "type": "game_mesh",
    "body": "game_mesh " & descriptor.appId & " " & envelopeType,
    "content": "game_mesh " & descriptor.appId & " " & envelopeType,
    "text": "game_mesh " & descriptor.appId & " " & envelopeType,
    "game": {
      "type": envelopeType,
      "appId": descriptor.appId,
      "roomId": descriptor.roomId,
      "matchId": descriptor.matchId,
      "protocolVersion": GameProtocolVersion,
      "senderPeerId": senderPeerId,
      "seatId": seatId,
      "seq": seq,
      "stateHash": stateHash,
      "sentAtMs": sentAt,
      "payload": payload
    }
  }

proc parseEnvelope*(payload: JsonNode): Option[GameEnvelope] =
  if payload.isNil or payload.kind != JObject:
    return none(GameEnvelope)
  var node = payload{"game"}
  if node.kind != JObject and payload{"payload"}.kind == JObject:
    node = payload["payload"]{"game"}
  if node.kind != JObject:
    return none(GameEnvelope)
  let appId = jsonGetStr(node, "appId")
  let roomId = jsonGetStr(node, "roomId")
  let envelopeType = jsonGetStr(node, "type")
  if appId.len == 0 or roomId.len == 0 or envelopeType.len == 0:
    return none(GameEnvelope)
  some(GameEnvelope(
    envelopeType: envelopeType,
    appId: appId,
    roomId: roomId,
    matchId: jsonGetStr(node, "matchId"),
    protocolVersion: jsonGetInt(node, "protocolVersion", GameProtocolVersion),
    senderPeerId: jsonGetStr(node, "senderPeerId"),
    seatId: jsonGetStr(node, "seatId"),
    seq: jsonGetInt(node, "seq"),
    stateHash: jsonGetStr(node, "stateHash"),
    sentAtMs: jsonGetInt64(node, "sentAtMs", nowMillis()),
    payload: if node{"payload"}.kind != JNull: node{"payload"} else: newJObject()
  ))

proc appendGameSyncSample(samples: seq[int64], sourceSentAtMs: int64,
                          observedAtMs = nowMillis()): seq[int64] =
  if sourceSentAtMs <= 0:
    return samples
  let sample = observedAtMs - sourceSentAtMs
  if sample < 0 or sample > GameSyncSampleMaxMs:
    return samples
  result = samples
  result.add(sample)
  if result.len > GameSyncSampleWindow:
    result = result[^GameSyncSampleWindow .. ^1]

proc appendGameSyncLatency(samples: seq[int64], latencyMs: int64): seq[int64] =
  if latencyMs < 0 or latencyMs > GameSyncSampleMaxMs:
    return samples
  result = samples
  result.add(latencyMs)
  if result.len > GameSyncSampleWindow:
    result = result[^GameSyncSampleWindow .. ^1]

proc buildSyncObservedPayload(sourceType: string, sourceSeq: int, sourceStateHash: string,
                              sourceSentAtMs: int64, observedAtMs = nowMillis()): JsonNode =
  let latencyMs = observedAtMs - sourceSentAtMs
  %*{
    "event": "sync_observed",
    "sourceType": sourceType,
    "sourceSeq": sourceSeq,
    "sourceStateHash": sourceStateHash,
    "sourceSentAtMs": sourceSentAtMs,
    "observedAtMs": observedAtMs,
    "latencyMs": latencyMs
  }

proc syncObservedKey(appId, roomId, sourceType: string, sourceSeq: int,
                     sourceStateHash: string): string =
  appId & "|" & roomId & "|" & sourceType & "|" & $sourceSeq & "|" & sourceStateHash

proc rememberPendingSyncObserved(rt: GameMeshRuntime, appId, roomId, sourceType: string,
                                 sourceSeq: int, sourceStateHash: string,
                                 sentAtMs: int64) =
  if sentAtMs <= 0:
    return
  rt.pendingSyncObservedAt[syncObservedKey(appId, roomId, sourceType, sourceSeq, sourceStateHash)] = sentAtMs

proc makeStatePayload(rt: GameMeshRuntime, appId, roomId: string, includeFullState = true): JsonNode

proc ensureOutbound(result: var GameOperationResult, peerId, conversationId: string, payload: JsonNode,
                    seedAddrs: seq[string], source: string) =
  result.outbound.add(GameOutboundMessage(
    peerId: peerId,
    conversationId: conversationId,
    payload: payload,
    seedAddrs: seedAddrs,
    source: source
  ))

proc replaceXiangqiSession(rt: GameMeshRuntime, session: XiangqiRoomSession) =
  rt.xiangqiSessions[session.descriptor.roomId] = session
  rt.invites[roomKey(session.descriptor.appId, session.descriptor.roomId)] = session.descriptor

proc replaceDoudizhuSession(rt: GameMeshRuntime, session: DoudizhuRoomSession) =
  rt.doudizhuSessions[session.descriptor.roomId] = session
  rt.invites[roomKey(session.descriptor.appId, session.descriptor.roomId)] = session.descriptor

proc requestRoomSnapshot*(rt: GameMeshRuntime, appId, roomId: string,
                          localSeedAddrs: seq[string] = @[]): GameOperationResult =
  result.handled = true
  result.appId = appId
  result.roomId = roomId
  case appId
  of "chess":
    if not rt.xiangqiSessions.hasKey(roomId):
      result.ok = false
      result.error = "missing_room"
      return
    let session = rt.xiangqiSessions[roomId]
    let remotePeerId =
      if session.remotePeerId.len > 0:
        session.remotePeerId
      else:
        session.descriptor.hostPeerId
    if remotePeerId.len == 0:
      result.ok = false
      result.error = "missing_remote_peer"
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let payload = %*{
      "conversationId": session.descriptor.conversationId,
      "knownSeq": session.seq,
      "knownStateHash": session.stateHash,
      "seedAddrs": localSeedAddrs
    }
    let seatId = if session.isHost: "host" else: "black"
    ensureOutbound(
      result,
      remotePeerId,
      session.descriptor.conversationId,
      encodeEnvelope(
        session.descriptor,
        remotePeerId,
        "game_snapshot_request",
        session.seq,
        session.stateHash,
        payload,
        seatId,
        session.localPeerId
      ),
      session.descriptor.seedAddrs,
      "game_snapshot_request"
    )
    result.ok = true
    result.statePayload = makeStatePayload(rt, appId, roomId)
  of "doudizhu":
    if not rt.doudizhuSessions.hasKey(roomId):
      result.ok = false
      result.error = "missing_room"
      return
    let session = rt.doudizhuSessions[roomId]
    let remotePeerId =
      if session.remotePeerId.len > 0:
        session.remotePeerId
      else:
        session.descriptor.hostPeerId
    if remotePeerId.len == 0:
      result.ok = false
      result.error = "missing_remote_peer"
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let payload = %*{
      "conversationId": session.descriptor.conversationId,
      "knownSeq": session.seq,
      "knownStateHash": session.stateHash,
      "seedAddrs": localSeedAddrs
    }
    let seatId = if session.isHost: "host" else: "guest"
    ensureOutbound(
      result,
      remotePeerId,
      session.descriptor.conversationId,
      encodeEnvelope(
        session.descriptor,
        remotePeerId,
        "game_snapshot_request",
        session.seq,
        session.stateHash,
        payload,
        seatId,
        session.localPeerId
      ),
      session.descriptor.seedAddrs,
      "game_snapshot_request"
    )
    result.ok = true
    result.statePayload = makeStatePayload(rt, appId, roomId)
  else:
    result.ok = false
    result.error = "unsupported_game"
    result.statePayload = %*{
      "appId": appId,
      "roomId": roomId
    }

proc doudizhuVisibleState(session: DoudizhuRoomSession): DouDiZhuState =
  if session.isHost:
    doudizhuPerspectiveState(session.state, session.localSeatAuth)
  else:
    session.state

proc doudizhuLocalStateSeat(session: DoudizhuRoomSession): int =
  if session.isHost: session.localSeatAuth else: 0

proc doudizhuAuthSeatToView(localSeatAuth, authSeat: int): int =
  ((authSeat - localSeatAuth) mod 3 + 3) mod 3

proc advanceDoudizhuBotTurns(session: var DoudizhuRoomSession) =
  if not session.isHost or not session.connected:
    return
  while session.state.phase != dgpFinished and session.state.currentTurn == 2:
    let botCards = aiSelectDdzPlay(2, session.state)
    var botEvent = newJObject()
    if session.state.phase == dgpBidding:
      let bid = aiBidDdz(session.state.players[2].hand, session.state.highestBid)
      botEvent["event"] = %"bid"
      botEvent["seat"] = %2
      botEvent["bid"] = %bid
      session.state = applyDdzBidForSeat(session.state, 2, bid)
    elif botCards.len == 0:
      botEvent["event"] = %"pass"
      botEvent["seat"] = %2
      session.state = passDdzTurnForSeat(session.state, 2)
    else:
      botEvent["event"] = %"play"
      botEvent["seat"] = %2
      botEvent["cardIds"] = %botCards.mapIt(it.id)
      session.state = playDdzCardsForSeat(session.state, 2, botCards)
    session.events.add(botEvent)

proc makeReplayPayload(replay: GameReplaySummary): JsonNode =
  encodeReplay(replay)

proc sendSnapshot(rt: GameMeshRuntime, session: XiangqiRoomSession, result: var GameOperationResult,
                  localSeedAddrs: seq[string] = @[]) =
  let payload = %*{
    "state": encodeXiangqiState(session.state),
    "notice": session.notice,
    "events": session.events,
    "replayOk": session.replay.ok,
    "replayStatus": session.replay.detail,
    "pendingDrawOfferSide": (if session.pendingDrawOfferSide.isSome(): sideName(session.pendingDrawOfferSide.get()) else: ""),
    "pendingRematchOfferSide": (if session.pendingRematchOfferSide.isSome(): sideName(session.pendingRematchOfferSide.get()) else: ""),
    "voiceRoomId": session.voiceRoomId,
    "voiceStreamKey": session.voiceStreamKey,
    "voiceTitle": session.voiceTitle,
    "voiceAudioOnly": session.voiceAudioOnly,
    "syncSamplesMs": session.syncSamplesMs,
    "seedAddrs": (if localSeedAddrs.len > 0: localSeedAddrs else: session.descriptor.seedAddrs)
  }
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_snapshot_response", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(
      session.descriptor,
      session.remotePeerId,
      "game_snapshot_response",
      session.seq,
      session.stateHash,
      payload,
      if session.isHost: "host" else: "guest",
      session.localPeerId,
      sentAt
    ),
    session.descriptor.seedAddrs,
    "game_snapshot_response"
  )

proc sendSnapshot(rt: GameMeshRuntime, session: DoudizhuRoomSession, result: var GameOperationResult,
                  localSeedAddrs: seq[string] = @[]) =
  let payload = %*{
    "viewState": encodeDoudizhuState(doudizhuPerspectiveState(session.state, 1)),
    "notice": session.notice,
    "deckIds": session.initialDeckIds,
    "initialTurn": session.initialTurn,
    "events": session.events,
    "replayOk": session.replay.ok,
    "replayStatus": session.replay.detail,
    "seedAddrs": (if localSeedAddrs.len > 0: localSeedAddrs else: session.descriptor.seedAddrs)
  }
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_snapshot_response", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(
      session.descriptor,
      session.remotePeerId,
      "game_snapshot_response",
      session.seq,
      session.stateHash,
      payload,
      if session.isHost: "host" else: "guest",
      session.localPeerId,
      sentAt
    ),
    session.descriptor.seedAddrs,
    "game_snapshot_response"
  )

proc sendSyncObserved(rt: GameMeshRuntime, session: XiangqiRoomSession, sourceType: string,
                      sourceSeq: int, sourceStateHash: string, sourceSentAtMs: int64,
                      result: var GameOperationResult) =
  if session.remotePeerId.len == 0 or session.descriptor.conversationId.len == 0:
    return
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(
      session.descriptor,
      session.remotePeerId,
      "game_sync_observed",
      session.seq,
      session.stateHash,
      buildSyncObservedPayload(sourceType, sourceSeq, sourceStateHash, sourceSentAtMs),
      if session.isHost: "host" else: "guest",
      session.localPeerId
    ),
    session.descriptor.seedAddrs,
    "game_sync_observed"
  )

proc sendSyncObserved(rt: GameMeshRuntime, session: DoudizhuRoomSession, sourceType: string,
                      sourceSeq: int, sourceStateHash: string, sourceSentAtMs: int64,
                      result: var GameOperationResult) =
  if session.remotePeerId.len == 0 or session.descriptor.conversationId.len == 0:
    return
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(
      session.descriptor,
      session.remotePeerId,
      "game_sync_observed",
      session.seq,
      session.stateHash,
      buildSyncObservedPayload(sourceType, sourceSeq, sourceStateHash, sourceSentAtMs),
      if session.isHost: "host" else: "guest",
      session.localPeerId
    ),
    session.descriptor.seedAddrs,
    "game_sync_observed"
  )

proc verifyXiangqiReplay*(session: XiangqiRoomSession): GameReplaySummary =
  var state = startXiangqiGame()
  for event in session.events:
    case jsonGetStr(event, "event")
    of "move":
      let fromOpt = decodePosition(event{"from"})
      let toOpt = decodePosition(event{"to"})
      if fromOpt.isNone() or toOpt.isNone():
        return GameReplaySummary(ok: false, detail: "invalid_move")
      state = applyXiangqiMove(state, fromOpt.get(), toOpt.get())
    of "resign":
      let winner = parseXiangqiSide(jsonGetStr(event, "winner", "red"))
      state.phase = xpFinished
      state.winner = if winner.isSome(): winner else: some(xsRed)
    of "draw_accept":
      state.phase = xpFinished
      state.winner = none(XiangqiSide)
      state.check = false
    of "rematch_accept":
      state = startXiangqiGame()
    else:
      discard
  let finalHash = xiangqiStateHash(state)
  GameReplaySummary(
    ok: finalHash == session.stateHash,
    stateHash: finalHash,
    detail: if finalHash == session.stateHash: "replay_verify=true" else: "replay_verify=false"
  )

proc verifyDoudizhuReplay*(session: DoudizhuRoomSession): GameReplaySummary =
  if session.initialDeckIds.len == 0:
    return GameReplaySummary(ok: false, detail: "missing_initial_deck")
  let deckById = createDdzDeckSnapshot().mapIt((it.id, it)).toTable()
  var deck: seq[DdzCard] = @[]
  for cardId in session.initialDeckIds:
    if not deckById.hasKey(cardId):
      return GameReplaySummary(ok: false, detail: "deck_restore_failed")
    deck.add(deckById[cardId])
  var state = startDouDiZhuGameWithDeck(deck, session.initialTurn, @["你", "对方", "机器人"])
  for event in session.events:
    case jsonGetStr(event, "event")
    of "start":
      discard
    of "bid":
      state = applyDdzBidForSeat(state, jsonGetInt(event, "seat"), jsonGetInt(event, "bid"))
    of "play":
      let seat = jsonGetInt(event, "seat")
      let ids = intList(event{"cardIds"}).toHashSet()
      let cards = state.players[seat].hand.filterIt(it.id in ids)
      state = playDdzCardsForSeat(state, seat, cards)
    of "pass":
      state = passDdzTurnForSeat(state, jsonGetInt(event, "seat"))
    of "bot_turn":
      let cards = aiSelectDdzPlay(state.currentTurn, state)
      if state.phase == dgpBidding:
        state = applyDdzBidForSeat(state, state.currentTurn, aiBidDdz(state.players[state.currentTurn].hand, state.highestBid))
      elif cards.len == 0:
        state = passDdzTurnForSeat(state, state.currentTurn)
      else:
        state = playDdzCardsForSeat(state, state.currentTurn, cards)
    else:
      discard
  let finalHash = doudizhuStateHash(state)
  GameReplaySummary(
    ok: finalHash == session.stateHash,
    stateHash: finalHash,
    detail: if finalHash == session.stateHash: "replay_verify=true" else: "replay_verify=false"
  )

proc resolveDdzCardsByIds(cardIds: seq[int]): seq[DdzCard] =
  if cardIds.len == 0:
    return @[]
  let deckById = createDdzDeckSnapshot().mapIt((it.id, it)).toTable()
  result = newSeqOfCap[DdzCard](cardIds.len)
  for cardId in cardIds:
    if deckById.hasKey(cardId):
      result.add(deckById[cardId])

proc maybeSendXiangqiAudit(rt: GameMeshRuntime, session: XiangqiRoomSession, result: var GameOperationResult) =
  if session.state.phase != xpFinished:
    return
  let replay = verifyXiangqiReplay(session)
  let payload = %*{
    "valid": replay.ok,
    "stateHash": replay.stateHash,
    "replayStatus": replay.detail,
    "events": session.events
  }
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_audit", session.seq, replay.stateHash, payload, "host", session.localPeerId),
    session.descriptor.seedAddrs,
    "game_audit"
  )
  var next = session
  next.replay = replay
  rt.replaceXiangqiSession(next)

proc maybeSendDoudizhuAudit(rt: GameMeshRuntime, session: DoudizhuRoomSession, result: var GameOperationResult) =
  if session.state.phase != dgpFinished:
    return
  let replay = verifyDoudizhuReplay(session)
  let payload = %*{
    "valid": replay.ok,
    "stateHash": replay.stateHash,
    "replayStatus": replay.detail,
    "events": session.events,
    "initialDeckIds": session.initialDeckIds,
    "initialTurn": session.initialTurn,
    "winner": session.state.winner,
    "landlordIndex": session.state.landlordIndex,
    "audit_report": {
      "valid": replay.ok,
      "winner": session.state.winner
    }
  }
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_audit", session.seq, replay.stateHash, payload, "host", session.localPeerId),
    session.descriptor.seedAddrs,
    "game_audit"
  )
  var next = session
  next.replay = replay
  next.auditReport = payload
  rt.replaceDoudizhuSession(next)

proc dispatchRoomSnapshot*(rt: GameMeshRuntime, appId, roomId: string,
                           localSeedAddrs: seq[string] = @[]): GameOperationResult =
  result.handled = true
  result.appId = appId
  result.roomId = roomId
  case appId
  of "chess":
    if not rt.xiangqiSessions.hasKey(roomId):
      result.ok = false
      result.error = "missing_room"
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let session = rt.xiangqiSessions[roomId]
    result.ok = true
    sendSnapshot(rt, session, result, localSeedAddrs)
    result.statePayload = makeStatePayload(rt, appId, roomId)
  of "doudizhu":
    if not rt.doudizhuSessions.hasKey(roomId):
      result.ok = false
      result.error = "missing_room"
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let session = rt.doudizhuSessions[roomId]
    result.ok = true
    sendSnapshot(rt, session, result, localSeedAddrs)
    result.statePayload = makeStatePayload(rt, appId, roomId)
  else:
    result.ok = false
    result.error = "unsupported_game"
    result.statePayload = makeStatePayload(rt, appId, roomId)

proc dispatchRoomAudit*(rt: GameMeshRuntime, appId, roomId: string): GameOperationResult =
  result.handled = true
  result.appId = appId
  result.roomId = roomId
  case appId
  of "chess":
    if not rt.xiangqiSessions.hasKey(roomId):
      result.ok = false
      result.error = "missing_room"
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let session = rt.xiangqiSessions[roomId]
    result.ok = true
    maybeSendXiangqiAudit(rt, session, result)
    result.statePayload = makeStatePayload(rt, appId, roomId)
  of "doudizhu":
    if not rt.doudizhuSessions.hasKey(roomId):
      result.ok = false
      result.error = "missing_room"
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let session = rt.doudizhuSessions[roomId]
    result.ok = true
    maybeSendDoudizhuAudit(rt, session, result)
    result.statePayload = makeStatePayload(rt, appId, roomId)
  else:
    result.ok = false
    result.error = "unsupported_game"
    result.statePayload = makeStatePayload(rt, appId, roomId)

proc currentDetailedPayload(rt: GameMeshRuntime, appId, roomId: string, includeFullState = true): JsonNode =
  case appId
  of "chess":
    if not rt.xiangqiSessions.hasKey(roomId):
      return %*{"appId": appId, "roomId": roomId}
    let session = rt.xiangqiSessions[roomId]
    var eventsPayload = newJArray()
    for event in session.events:
      eventsPayload.add(cloneJson(event))
    result = %*{
      "appId": appId,
      "roomId": roomId,
      "matchId": session.descriptor.matchId,
      "side": (if session.isHost: "red" else: "black"),
      "phase": phaseName(session.state.phase),
      "seq": session.seq,
      "moveCount": session.state.moveHistory.len,
      "stateHash": session.stateHash,
      "connected": session.connected,
      "replayOk": session.replay.ok,
      "descriptor": encodeDescriptor(session.descriptor),
      "state": encodeXiangqiState(session.state),
      "replay": encodeReplay(session.replay),
      "notice": session.notice,
      "events": eventsPayload,
      "pendingDrawOfferSide": (if session.pendingDrawOfferSide.isSome(): sideName(session.pendingDrawOfferSide.get()) else: ""),
      "pendingRematchOfferSide": (if session.pendingRematchOfferSide.isSome(): sideName(session.pendingRematchOfferSide.get()) else: ""),
      "voiceRoomId": session.voiceRoomId,
      "voiceStreamKey": session.voiceStreamKey,
      "voiceTitle": session.voiceTitle,
      "voiceAudioOnly": session.voiceAudioOnly,
      "syncSamplesMs": session.syncSamplesMs
    }
  of "doudizhu":
    if not rt.doudizhuSessions.hasKey(roomId):
      return %*{"appId": appId, "roomId": roomId}
    let session = rt.doudizhuSessions[roomId]
    var eventsPayload = newJArray()
    for event in session.events:
      eventsPayload.add(cloneJson(event))
    let auditPayload =
      if session.auditReport.kind == JObject:
        cloneJson(session.auditReport)
      else:
        newJNull()
    result = %*{
      "appId": appId,
      "roomId": roomId,
      "matchId": session.descriptor.matchId,
      "phase": phaseName(session.state.phase),
      "seq": session.seq,
      "stateHash": session.stateHash,
      "connected": session.connected,
      "replayOk": session.replay.ok,
      "winner": session.state.winner,
      "auditValid": (session.auditReport.kind == JObject and jsonGetBool(session.auditReport, "valid")),
      "descriptor": encodeDescriptor(session.descriptor),
      "state": encodeDoudizhuState(doudizhuVisibleState(session)),
      "replay": encodeReplay(session.replay),
      "notice": session.notice,
      "events": eventsPayload,
      "syncSamplesMs": session.syncSamplesMs,
      "auditReport": auditPayload,
      "initialDeckIds": session.initialDeckIds,
      "initialTurn": session.initialTurn,
      "localSeatAuth": session.localSeatAuth
    }
    if includeFullState:
      result["fullState"] = encodeDoudizhuState(session.state)
  else:
    result = %*{"appId": appId, "roomId": roomId}

proc makeStatePayload(rt: GameMeshRuntime, appId, roomId: string, includeFullState = true): JsonNode =
  currentDetailedPayload(rt, appId, roomId, includeFullState)

proc int64List(node: JsonNode): seq[int64] =
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node:
    case item.kind
    of JInt:
      result.add(item.getBiggestInt().int64)
    of JFloat:
      result.add(item.getFloat().int64)
    of JString:
      try:
        result.add(parseBiggestInt(item.getStr()).int64)
      except ValueError:
        discard
    else:
      discard

proc cloneJsonItems(node: JsonNode): seq[JsonNode] =
  if node.isNil or node.kind != JArray:
    return @[]
  for item in node:
    result.add(cloneJson(item))

proc restoreRoomSnapshot*(
    rt: GameMeshRuntime,
    snapshot: JsonNode,
    defaultLocalPeerId = ""
): GameOperationResult =
  result.handled = true
  if snapshot.kind != JObject:
    result.ok = false
    result.error = "invalid_snapshot"
    return
  let descriptorOpt =
    if snapshot.hasKey("descriptor"):
      decodeDescriptor(snapshot["descriptor"])
    else:
      decodeDescriptor(snapshot)
  if descriptorOpt.isNone():
    result.ok = false
    result.error = "invalid_descriptor"
    return
  let descriptor = descriptorOpt.get()
  let appId = descriptor.appId
  let roomId = descriptor.roomId
  result.appId = appId
  result.roomId = roomId
  result.matchId = descriptor.matchId
  rt.invites[roomKey(appId, roomId)] = descriptor
  case appId
  of "chess":
    let stateOpt = decodeXiangqiState(snapshot{"state"})
    if stateOpt.isNone():
      result.ok = false
      result.error = "invalid_state"
      return
    let state = stateOpt.get()
    let isHost = jsonGetBool(snapshot, "isHost", defaultLocalPeerId.len > 0 and defaultLocalPeerId == descriptor.hostPeerId)
    let localPeerId = jsonGetStr(snapshot, "localPeerId", defaultLocalPeerId)
    let explicitRemotePeerId = jsonGetStr(snapshot, "remotePeerId")
    let remotePeerId =
      if explicitRemotePeerId.len > 0:
        explicitRemotePeerId
      elif isHost:
        descriptor.guestPeerId
      else:
        descriptor.hostPeerId
    let pendingDrawOfferSide = parseXiangqiSide(jsonGetStr(snapshot, "pendingDrawOfferSide"))
    let pendingRematchOfferSide = parseXiangqiSide(jsonGetStr(snapshot, "pendingRematchOfferSide"))
    rt.replaceXiangqiSession(XiangqiRoomSession(
      descriptor: descriptor,
      localPeerId: localPeerId,
      remotePeerId: remotePeerId,
      isHost: isHost,
      state: state,
      seq: jsonGetInt(snapshot, "seq", 0),
      stateHash: jsonGetStr(snapshot, "stateHash", xiangqiStateHash(state)),
      connected: jsonGetBool(snapshot, "connected", false),
      replay: decodeReplay(snapshot{"replay"}),
      notice: jsonGetStr(snapshot, "notice"),
      events: cloneJsonItems(snapshot{"events"}),
      pendingDrawOfferSide: pendingDrawOfferSide,
      pendingRematchOfferSide: pendingRematchOfferSide,
      voiceRoomId: jsonGetStr(snapshot, "voiceRoomId"),
      voiceStreamKey: jsonGetStr(snapshot, "voiceStreamKey"),
      voiceTitle: jsonGetStr(snapshot, "voiceTitle"),
      voiceAudioOnly: jsonGetBool(snapshot, "voiceAudioOnly", true),
      syncSamplesMs: int64List(snapshot{"syncSamplesMs"})
    ))
    result.ok = true
    result.statePayload = makeStatePayload(rt, appId, roomId)
  of "doudizhu":
    let fullStateNode =
      if snapshot.hasKey("fullState") and snapshot["fullState"].kind == JObject:
        snapshot["fullState"]
      else:
        snapshot{"state"}
    let stateOpt = decodeDoudizhuState(fullStateNode)
    if stateOpt.isNone():
      result.ok = false
      result.error = "invalid_state"
      return
    let state = stateOpt.get()
    let isHost = jsonGetBool(snapshot, "isHost", defaultLocalPeerId.len > 0 and defaultLocalPeerId == descriptor.hostPeerId)
    let localPeerId = jsonGetStr(snapshot, "localPeerId", defaultLocalPeerId)
    let explicitRemotePeerId = jsonGetStr(snapshot, "remotePeerId")
    let remotePeerId =
      if explicitRemotePeerId.len > 0:
        explicitRemotePeerId
      elif isHost:
        descriptor.guestPeerId
      else:
        descriptor.hostPeerId
    let auditReport =
      if snapshot.hasKey("auditReport") and snapshot["auditReport"].kind == JObject:
        cloneJson(snapshot["auditReport"])
      else:
        newJNull()
    rt.replaceDoudizhuSession(DoudizhuRoomSession(
      descriptor: descriptor,
      localPeerId: localPeerId,
      remotePeerId: remotePeerId,
      isHost: isHost,
      localSeatAuth: jsonGetInt(snapshot, "localSeatAuth", if isHost: 0 else: 1),
      state: state,
      initialDeckIds: intList(snapshot{"initialDeckIds"}),
      initialTurn: jsonGetInt(snapshot, "initialTurn", 0),
      seq: jsonGetInt(snapshot, "seq", 0),
      stateHash: jsonGetStr(snapshot, "stateHash", doudizhuStateHash(state)),
      connected: jsonGetBool(snapshot, "connected", false),
      replay: decodeReplay(snapshot{"replay"}),
      notice: jsonGetStr(snapshot, "notice"),
      events: cloneJsonItems(snapshot{"events"}),
      syncSamplesMs: int64List(snapshot{"syncSamplesMs"}),
      auditReport: auditReport
    ))
    result.ok = true
    result.statePayload = makeStatePayload(rt, appId, roomId)
  else:
    result.ok = false
    result.error = "unsupported_game"

proc stateHashFor(rt: GameMeshRuntime, appId, roomId: string): string =
  case appId
  of "chess":
    if rt.xiangqiSessions.hasKey(roomId): rt.xiangqiSessions[roomId].stateHash else: ""
  of "doudizhu":
    if rt.doudizhuSessions.hasKey(roomId): rt.doudizhuSessions[roomId].stateHash else: ""
  else:
    ""

proc makeLightStatePayload(rt: GameMeshRuntime, appId, roomId: string): JsonNode =
  case appId
  of "chess":
    if rt.xiangqiSessions.hasKey(roomId):
      let session = rt.xiangqiSessions[roomId]
      return %*{
        "appId": appId,
        "roomId": roomId,
        "matchId": session.descriptor.matchId,
        "side": (if session.isHost: "red" else: "black"),
        "phase": phaseName(session.state.phase),
        "seq": session.seq,
        "moveCount": session.state.moveHistory.len,
        "stateHash": session.stateHash,
        "connected": session.connected,
        "replayOk": session.replay.ok,
        "winner": (if session.state.winner.isSome(): $session.state.winner.get() else: ""),
        "notice": session.notice,
        "pendingDrawOfferSide": (if session.pendingDrawOfferSide.isSome(): sideName(session.pendingDrawOfferSide.get()) else: ""),
        "pendingRematchOfferSide": (if session.pendingRematchOfferSide.isSome(): sideName(session.pendingRematchOfferSide.get()) else: ""),
        "voiceRoomId": session.voiceRoomId,
        "voiceStreamKey": session.voiceStreamKey,
        "voiceTitle": session.voiceTitle,
        "voiceAudioOnly": session.voiceAudioOnly,
        "syncSamplesMs": session.syncSamplesMs
      }
  of "doudizhu":
    if rt.doudizhuSessions.hasKey(roomId):
      let session = rt.doudizhuSessions[roomId]
      return %*{
        "appId": appId,
        "roomId": roomId,
        "matchId": session.descriptor.matchId,
        "phase": phaseName(session.state.phase),
        "seq": session.seq,
        "stateHash": session.stateHash,
        "connected": session.connected,
        "replayOk": session.replay.ok,
        "winner": session.state.winner,
        "auditValid": (session.auditReport.kind == JObject and jsonGetBool(session.auditReport, "valid")),
        "notice": session.notice,
        "syncSamplesMs": session.syncSamplesMs
      }
  else:
    discard
  %*{"appId": appId, "roomId": roomId}

proc createInviteRoom*(rt: GameMeshRuntime, appId, localPeerId, remotePeerId, conversationId: string,
                       seedAddrs: seq[string], matchSeed: string, sendInvite = true,
                       roomIdHint = "", matchIdHint = ""): GameOperationResult =
  result.ok = true
  result.handled = true
  result.appId = appId
  let remoteSuffix =
    if remotePeerId.len <= 6: remotePeerId
    else: remotePeerId[remotePeerId.len - 6 ..< remotePeerId.len]
  let roomId =
    if roomIdHint.strip().len > 0:
      roomIdHint.strip()
    else:
      appId & "_" & $nowMillis() & "_" & remoteSuffix
  let matchId = if matchIdHint.strip().len > 0: matchIdHint.strip() else: "match_" & $nowMillis()
  result.roomId = roomId
  result.matchId = matchId
  let descriptor = GameInviteDescriptor(
    appId: appId,
    roomId: roomId,
    matchId: matchId,
    conversationId: if conversationId.strip().len > 0: conversationId.strip() else: "dm:" & remotePeerId,
    hostPeerId: localPeerId,
    guestPeerId: remotePeerId,
    seedAddrs: seedAddrs,
    mode: if appId == "doudizhu": "p2p_bot_room" else: "p2p_room"
  )
  rt.invites[roomKey(appId, roomId)] = descriptor
  case appId
  of "chess":
    let state = startXiangqiGame()
    let session = XiangqiRoomSession(
      descriptor: descriptor,
      localPeerId: localPeerId,
      remotePeerId: remotePeerId,
      isHost: true,
      state: state,
      seq: 0,
      stateHash: xiangqiStateHash(state),
      connected: false,
      replay: GameReplaySummary(),
      notice: "等待对方加入房间",
      events: @[%*{"event": "start", "sentAtMs": nowMillis()}],
      pendingDrawOfferSide: none(XiangqiSide),
      pendingRematchOfferSide: none(XiangqiSide),
      voiceRoomId: "",
      voiceStreamKey: "",
      voiceTitle: "",
      voiceAudioOnly: true,
      syncSamplesMs: @[]
    )
    rt.replaceXiangqiSession(session)
  of "doudizhu":
    let seedText = if matchSeed.len > 0: matchSeed else: descriptor.roomId & ":" & descriptor.matchId & ":" & descriptor.hostPeerId
    let deck = deterministicDdzDeck(seedText)
    let state = startDouDiZhuGameWithDeck(deck, deterministicTurn(seedText), @["你", "对方", "机器人"])
    var session = DoudizhuRoomSession(
      descriptor: descriptor,
      localPeerId: localPeerId,
      remotePeerId: remotePeerId,
      isHost: true,
      localSeatAuth: 0,
      state: state,
      initialDeckIds: deck.mapIt(it.id),
      initialTurn: state.currentTurn,
      seq: 0,
      stateHash: doudizhuStateHash(state),
      connected: false,
      replay: GameReplaySummary(),
      notice: "等待对方加入房间",
      events: @[%*{"event": "start", "deckIds": deck.mapIt(it.id), "currentTurn": state.currentTurn, "sentAtMs": nowMillis()}],
      syncSamplesMs: @[],
      auditReport: newJNull()
    )
    advanceDoudizhuBotTurns(session)
    session.stateHash = doudizhuStateHash(session.state)
    rt.replaceDoudizhuSession(session)
  else:
    result.ok = false
    result.error = "unsupported_game"
    return
  result.statePayload = makeStatePayload(rt, appId, roomId)
  if sendInvite:
    let payload = %*{
      "conversationId": descriptor.conversationId,
      "hostPeerId": descriptor.hostPeerId,
      "seedAddrs": seedAddrs,
      "mode": descriptor.mode
    }
    ensureOutbound(
      result,
      remotePeerId,
      descriptor.conversationId,
      encodeEnvelope(descriptor, remotePeerId, "game_invite", 0, stateHashFor(rt, appId, roomId), payload, "host", localPeerId),
      seedAddrs,
      "game_invite"
    )

proc joinRoom*(rt: GameMeshRuntime, appId, roomId, localPeerId, hostPeerId, conversationId, matchId: string,
               remoteSeedAddrs: seq[string], localSeedAddrs: seq[string], mode: string): GameOperationResult =
  result.ok = true
  result.handled = true
  result.appId = appId
  result.roomId = roomId
  var descriptor = if rt.invites.hasKey(roomKey(appId, roomId)): rt.invites[roomKey(appId, roomId)] else: GameInviteDescriptor()
  if descriptor.appId.len == 0:
    descriptor = GameInviteDescriptor(
      appId: appId,
      roomId: roomId,
      matchId: if matchId.len > 0: matchId else: "match_pending",
      conversationId: if conversationId.len > 0: conversationId else: "dm:" & hostPeerId,
      hostPeerId: hostPeerId,
      guestPeerId: localPeerId,
      seedAddrs: remoteSeedAddrs,
      mode: if mode.len > 0: mode else: (if appId == "doudizhu": "p2p_bot_room" else: "p2p_room")
    )
    rt.invites[roomKey(appId, roomId)] = descriptor
  elif remoteSeedAddrs.len > 0:
    descriptor.seedAddrs = remoteSeedAddrs
    rt.invites[roomKey(appId, roomId)] = descriptor
  result.matchId = descriptor.matchId
  case appId
  of "chess":
    if not rt.xiangqiSessions.hasKey(roomId):
      let state = startXiangqiGame()
      rt.replaceXiangqiSession(XiangqiRoomSession(
        descriptor: descriptor,
        localPeerId: localPeerId,
        remotePeerId: descriptor.hostPeerId,
        isHost: false,
        state: state,
        seq: 0,
        stateHash: xiangqiStateHash(state),
        connected: false,
        replay: GameReplaySummary(),
        notice: "正在加入房间",
        events: @[],
        pendingDrawOfferSide: none(XiangqiSide),
        pendingRematchOfferSide: none(XiangqiSide),
        voiceRoomId: "",
        voiceStreamKey: "",
        voiceTitle: "",
        voiceAudioOnly: true,
        syncSamplesMs: @[]
      ))
  of "doudizhu":
    if not rt.doudizhuSessions.hasKey(roomId):
      let state = startDouDiZhuGame()
      rt.replaceDoudizhuSession(DoudizhuRoomSession(
        descriptor: descriptor,
        localPeerId: localPeerId,
        remotePeerId: descriptor.hostPeerId,
        isHost: false,
        localSeatAuth: 1,
        state: doudizhuPerspectiveState(state, 1),
        initialDeckIds: @[],
        initialTurn: 0,
        seq: 0,
        stateHash: "",
        connected: false,
        replay: GameReplaySummary(),
        notice: "正在加入房间",
        events: @[],
        syncSamplesMs: @[],
        auditReport: newJNull()
      ))
  else:
    result.ok = false
    result.error = "unsupported_game"
    return
  let joinSeatId = if appId == "doudizhu": "guest" else: "black"
  let joinPayload = %*{
    "conversationId": descriptor.conversationId,
    "seedAddrs": (if localSeedAddrs.len > 0: localSeedAddrs else: remoteSeedAddrs)
  }
  ensureOutbound(
    result,
    descriptor.hostPeerId,
    descriptor.conversationId,
    encodeEnvelope(descriptor, descriptor.hostPeerId, "game_join", 0, stateHashFor(rt, appId, roomId), joinPayload, joinSeatId, localPeerId),
    descriptor.seedAddrs,
    "game_join"
  )
  ensureOutbound(
    result,
    descriptor.hostPeerId,
    descriptor.conversationId,
    encodeEnvelope(descriptor, descriptor.hostPeerId, "game_snapshot_request", 0, "", %*{"conversationId": descriptor.conversationId}, joinSeatId, localPeerId),
    descriptor.seedAddrs,
    "game_snapshot_request"
  )
  result.statePayload = makeStatePayload(rt, appId, roomId)

proc buildInviteUiMessage(appId, roomId, conversationId, messageId: string, timestampMs: int64): JsonNode =
  let title =
    case appId
    of "chess": "中国象棋"
    of "doudizhu": "斗地主"
    of "mahjong": "四人麻将"
    of "werewolf": "狼人杀"
    else: appId
  let summary =
    case appId
    of "chess": "通过 cheng-libp2p 一起下象棋"
    of "doudizhu": "通过 cheng-libp2p 开一局斗地主"
    of "mahjong": "通过 cheng-libp2p 开一桌麻将"
    of "werewolf": "通过 cheng-libp2p 发起狼人杀房间"
    else: "通过 cheng-libp2p 一起体验"
  %*{
    "type": "invite",
    "body": buildGameInviteBody(appId, roomId, title, summary),
    "conversationId": conversationId,
    "messageId": messageId,
    "timestampMs": timestampMs
  }

proc appendXiangqiEvent(session: XiangqiRoomSession, event: JsonNode, seqValue = -1): XiangqiRoomSession
proc applyXiangqiEvent(session: XiangqiRoomSession, event: JsonNode, seqValue = -1): Option[XiangqiRoomSession]
proc dispatchXiangqiConsensusAction(
    rt: GameMeshRuntime,
    roomId: string,
    eventName: string,
    previewUpdate: proc(session: XiangqiRoomSession): XiangqiRoomSession {.closure.}
): GameOperationResult

proc handleIncomingGameMessage*(rt: GameMeshRuntime, peerId, conversationId, messageId: string, payload: JsonNode,
                                localPeerId: string): GameOperationResult =
  let envelopeOpt = parseEnvelope(payload)
  if envelopeOpt.isNone():
    result.ok = false
    result.error = "not_game_mesh"
    return
  let envelope = envelopeOpt.get()
  result.handled = true
  result.ok = true
  result.appId = envelope.appId
  result.roomId = envelope.roomId
  result.matchId = envelope.matchId
  let receivedAt = if envelope.sentAtMs > 0: envelope.sentAtMs else: nowMillis()
  let safeMessageId = if messageId.len > 0: messageId else: jsonGetStr(payload, "messageId", "game-msg-" & $receivedAt)
  if safeMessageId in rt.processedInboundMessageIds and envelope.envelopeType != "game_invite":
    result.statePayload = makeLightStatePayload(rt, envelope.appId, envelope.roomId)
    return
  rt.processedInboundMessageIds.incl(safeMessageId)
  case envelope.envelopeType
  of "game_invite":
    let descriptor = GameInviteDescriptor(
      appId: envelope.appId,
      roomId: envelope.roomId,
      matchId: envelope.matchId,
      conversationId: jsonGetStr(envelope.payload, "conversationId", conversationId),
      hostPeerId: jsonGetStr(envelope.payload, "hostPeerId", envelope.senderPeerId),
      guestPeerId: localPeerId,
      seedAddrs: stringList(envelope.payload{"seedAddrs"}),
      mode: jsonGetStr(envelope.payload, "mode", "room")
    )
    rt.invites[roomKey(descriptor.appId, descriptor.roomId)] = descriptor
    result.uiMessage = buildInviteUiMessage(descriptor.appId, descriptor.roomId, descriptor.conversationId, safeMessageId, receivedAt)
  of "game_join":
    case envelope.appId
    of "chess":
      if rt.xiangqiSessions.hasKey(envelope.roomId):
        var session = rt.xiangqiSessions[envelope.roomId]
        if session.isHost:
          session.connected = true
          let remoteSeedAddrs = stringList(envelope.payload{"seedAddrs"})
          if remoteSeedAddrs.len > 0:
            session.descriptor.seedAddrs = remoteSeedAddrs
          session.notice = "对方已加入房间"
          rt.replaceXiangqiSession(session)
          rt.sendSnapshot(session, result)
    of "doudizhu":
      if rt.doudizhuSessions.hasKey(envelope.roomId):
        var session = rt.doudizhuSessions[envelope.roomId]
        if session.isHost:
          session.connected = true
          let remoteSeedAddrs = stringList(envelope.payload{"seedAddrs"})
          if remoteSeedAddrs.len > 0:
            session.descriptor.seedAddrs = remoteSeedAddrs
          session.notice = "对方已加入房间"
          advanceDoudizhuBotTurns(session)
          session.stateHash = doudizhuStateHash(session.state)
          rt.replaceDoudizhuSession(session)
          rt.sendSnapshot(session, result)
          rt.maybeSendDoudizhuAudit(session, result)
    else:
      discard
  of "game_snapshot_request":
    case envelope.appId
    of "chess":
      if rt.xiangqiSessions.hasKey(envelope.roomId):
        var session = rt.xiangqiSessions[envelope.roomId]
        if session.isHost:
          let remoteSeedAddrs = stringList(envelope.payload{"seedAddrs"})
          if remoteSeedAddrs.len > 0:
            session.descriptor.seedAddrs = remoteSeedAddrs
            rt.replaceXiangqiSession(session)
          rt.sendSnapshot(session, result)
    of "doudizhu":
      if rt.doudizhuSessions.hasKey(envelope.roomId):
        var session = rt.doudizhuSessions[envelope.roomId]
        if session.isHost:
          let remoteSeedAddrs = stringList(envelope.payload{"seedAddrs"})
          if remoteSeedAddrs.len > 0:
            session.descriptor.seedAddrs = remoteSeedAddrs
            rt.replaceDoudizhuSession(session)
          rt.sendSnapshot(session, result)
    else:
      discard
  of "game_snapshot_response":
    case envelope.appId
    of "chess":
      if rt.xiangqiSessions.hasKey(envelope.roomId) and rt.xiangqiSessions[envelope.roomId].isHost:
        result.statePayload = makeStatePayload(rt, envelope.appId, envelope.roomId)
        return
      if rt.xiangqiSessions.hasKey(envelope.roomId):
        let existing = rt.xiangqiSessions[envelope.roomId]
        if not existing.isHost and existing.seq > envelope.seq:
          result.statePayload = makeStatePayload(rt, envelope.appId, envelope.roomId)
          return
      let stateOpt = decodeXiangqiState(envelope.payload{"state"})
      if stateOpt.isSome():
        let descriptor =
          if rt.invites.hasKey(roomKey("chess", envelope.roomId)):
            rt.invites[roomKey("chess", envelope.roomId)]
          elif rt.xiangqiSessions.hasKey(envelope.roomId):
            rt.xiangqiSessions[envelope.roomId].descriptor
          else:
            GameInviteDescriptor(appId: "chess", roomId: envelope.roomId, matchId: envelope.matchId,
              conversationId: conversationId, hostPeerId: peerId, guestPeerId: localPeerId,
              seedAddrs: @[], mode: "p2p_room")
        let session = XiangqiRoomSession(
          descriptor: descriptor,
          localPeerId: localPeerId,
          remotePeerId: descriptor.hostPeerId,
          isHost: false,
          state: stateOpt.get(),
          seq: envelope.seq,
          stateHash: envelope.stateHash,
          connected: true,
          replay: GameReplaySummary(
            ok: jsonGetBool(envelope.payload, "replayOk"),
            stateHash: envelope.stateHash,
            detail: jsonGetStr(envelope.payload, "replayStatus")
          ),
          notice: jsonGetStr(envelope.payload, "notice", "房间已同步"),
          events: if envelope.payload{"events"}.kind == JArray: envelope.payload["events"].elems else: @[],
          pendingDrawOfferSide: parseXiangqiSide(jsonGetStr(envelope.payload, "pendingDrawOfferSide")),
          pendingRematchOfferSide: parseXiangqiSide(jsonGetStr(envelope.payload, "pendingRematchOfferSide")),
          voiceRoomId: jsonGetStr(envelope.payload, "voiceRoomId"),
          voiceStreamKey: jsonGetStr(envelope.payload, "voiceStreamKey"),
          voiceTitle: jsonGetStr(envelope.payload, "voiceTitle"),
          voiceAudioOnly: jsonGetBool(envelope.payload, "voiceAudioOnly", true),
          syncSamplesMs: appendGameSyncSample(
            if rt.xiangqiSessions.hasKey(envelope.roomId): rt.xiangqiSessions[envelope.roomId].syncSamplesMs else: @[],
            envelope.sentAtMs
          )
        )
        rt.replaceXiangqiSession(session)
        rt.sendSyncObserved(session, "game_snapshot_response", envelope.seq, envelope.stateHash, envelope.sentAtMs, result)
    of "doudizhu":
      if rt.doudizhuSessions.hasKey(envelope.roomId) and rt.doudizhuSessions[envelope.roomId].isHost:
        result.statePayload = makeStatePayload(rt, envelope.appId, envelope.roomId)
        return
      if rt.doudizhuSessions.hasKey(envelope.roomId):
        let existing = rt.doudizhuSessions[envelope.roomId]
        if not existing.isHost and existing.seq > envelope.seq:
          result.statePayload = makeStatePayload(rt, envelope.appId, envelope.roomId, includeFullState = false)
          return
      let stateOpt = decodeDoudizhuState(envelope.payload{"viewState"})
      if stateOpt.isSome():
        let descriptor =
          if rt.invites.hasKey(roomKey("doudizhu", envelope.roomId)):
            rt.invites[roomKey("doudizhu", envelope.roomId)]
          elif rt.doudizhuSessions.hasKey(envelope.roomId):
            rt.doudizhuSessions[envelope.roomId].descriptor
          else:
            GameInviteDescriptor(appId: "doudizhu", roomId: envelope.roomId, matchId: envelope.matchId,
              conversationId: conversationId, hostPeerId: peerId, guestPeerId: localPeerId,
              seedAddrs: @[], mode: "p2p_bot_room")
        let session = DoudizhuRoomSession(
          descriptor: descriptor,
          localPeerId: localPeerId,
          remotePeerId: descriptor.hostPeerId,
          isHost: false,
          localSeatAuth: 1,
          state: stateOpt.get(),
          initialDeckIds: intList(envelope.payload{"deckIds"}),
          initialTurn: jsonGetInt(envelope.payload, "initialTurn"),
          seq: envelope.seq,
          stateHash: envelope.stateHash,
          connected: true,
          replay: GameReplaySummary(
            ok: jsonGetBool(envelope.payload, "replayOk"),
            stateHash: envelope.stateHash,
            detail: jsonGetStr(envelope.payload, "replayStatus")
          ),
          notice: jsonGetStr(envelope.payload, "notice", "房间已同步"),
          events: if envelope.payload{"events"}.kind == JArray: envelope.payload["events"].elems else: @[],
          syncSamplesMs: appendGameSyncSample(
            if rt.doudizhuSessions.hasKey(envelope.roomId): rt.doudizhuSessions[envelope.roomId].syncSamplesMs else: @[],
            envelope.sentAtMs
          ),
          auditReport: newJNull()
        )
        rt.replaceDoudizhuSession(session)
        rt.sendSyncObserved(session, "game_snapshot_response", envelope.seq, envelope.stateHash, envelope.sentAtMs, result)
    else:
      discard
  of "game_event":
    case envelope.appId
    of "chess":
      if rt.xiangqiSessions.hasKey(envelope.roomId):
        var session = rt.xiangqiSessions[envelope.roomId]
        let remoteSeedAddrs = stringList(envelope.payload{"seedAddrs"})
        if remoteSeedAddrs.len > 0:
          session.descriptor.seedAddrs = remoteSeedAddrs
        let nextOpt = applyXiangqiEvent(
          session,
          envelope.payload,
          if session.isHost: session.seq + 1 else: max(session.seq + 1, envelope.seq)
        )
        if nextOpt.isSome():
          session = nextOpt.get()
          session.syncSamplesMs = appendGameSyncSample(session.syncSamplesMs, envelope.sentAtMs)
          rt.replaceXiangqiSession(session)
          rt.sendSyncObserved(session, "game_event", envelope.seq, envelope.stateHash, envelope.sentAtMs, result)
          if session.isHost:
            rt.sendSnapshot(session, result)
            rt.maybeSendXiangqiAudit(session, result)
    of "doudizhu":
      if rt.doudizhuSessions.hasKey(envelope.roomId):
        var session = rt.doudizhuSessions[envelope.roomId]
        let remoteSeedAddrs = stringList(envelope.payload{"seedAddrs"})
        if remoteSeedAddrs.len > 0:
          session.descriptor.seedAddrs = remoteSeedAddrs
        let remoteSeat =
          if session.isHost:
            1
          else:
            doudizhuAuthSeatToView(session.localSeatAuth, jsonGetInt(envelope.payload, "seat", 1))
        let nextState =
          case jsonGetStr(envelope.payload, "event")
          of "bid":
            applyDdzBidForSeat(session.state, remoteSeat, jsonGetInt(envelope.payload, "bid"))
          of "play":
            let requestedIds = intList(envelope.payload{"cardIds"})
            let ids = requestedIds.toHashSet()
            var cards = session.state.players[remoteSeat].hand.filterIt(it.id in ids)
            if cards.len == 0 and requestedIds.len > 0:
              cards = resolveDdzCardsByIds(requestedIds)
            playDdzCardsForSeat(session.state, remoteSeat, cards)
          of "pass":
            passDdzTurnForSeat(session.state, remoteSeat)
          else:
            session.state
        if $encodeDoudizhuState(nextState) != $encodeDoudizhuState(session.state):
          session.state = nextState
          session.connected = true
          session.syncSamplesMs = appendGameSyncSample(session.syncSamplesMs, envelope.sentAtMs)
          if session.isHost:
            session.seq.inc()
            session.stateHash = doudizhuStateHash(session.state)
          else:
            session.seq = max(session.seq + 1, envelope.seq)
            session.stateHash = envelope.stateHash
          session.events.add(envelope.payload)
          if session.isHost:
            advanceDoudizhuBotTurns(session)
            session.stateHash = doudizhuStateHash(session.state)
            rt.replaceDoudizhuSession(session)
            rt.sendSyncObserved(session, "game_event", envelope.seq, envelope.stateHash, envelope.sentAtMs, result)
            rt.sendSnapshot(session, result)
            rt.maybeSendDoudizhuAudit(session, result)
          else:
            rt.replaceDoudizhuSession(session)
            rt.sendSyncObserved(session, "game_event", envelope.seq, envelope.stateHash, envelope.sentAtMs, result)
    else:
      discard
  of "game_sync_observed":
    let sourceType = jsonGetStr(envelope.payload, "sourceType")
    let sourceSeq = jsonGetInt(envelope.payload, "sourceSeq", envelope.seq)
    let sourceStateHash = jsonGetStr(envelope.payload, "sourceStateHash")
    let pendingKey = syncObservedKey(envelope.appId, envelope.roomId, sourceType, sourceSeq, sourceStateHash)
    var latencyMs =
      if rt.pendingSyncObservedAt.hasKey(pendingKey):
        let localSentAt = rt.pendingSyncObservedAt[pendingKey]
        rt.pendingSyncObservedAt.del(pendingKey)
        nowMillis() - localSentAt
      else:
        jsonGetInt64(envelope.payload, "latencyMs",
          jsonGetInt64(envelope.payload, "observedAtMs", 0) - jsonGetInt64(envelope.payload, "sourceSentAtMs", 0))
    case envelope.appId
    of "chess":
      if rt.xiangqiSessions.hasKey(envelope.roomId) and latencyMs >= 0 and latencyMs <= GameSyncSampleMaxMs:
        var session = rt.xiangqiSessions[envelope.roomId]
        session.syncSamplesMs = appendGameSyncLatency(session.syncSamplesMs, latencyMs)
        rt.replaceXiangqiSession(session)
    of "doudizhu":
      if rt.doudizhuSessions.hasKey(envelope.roomId) and latencyMs >= 0 and latencyMs <= GameSyncSampleMaxMs:
        var session = rt.doudizhuSessions[envelope.roomId]
        session.syncSamplesMs = appendGameSyncLatency(session.syncSamplesMs, latencyMs)
        rt.replaceDoudizhuSession(session)
    else:
      discard
  of "game_audit":
    case envelope.appId
    of "chess":
      if rt.xiangqiSessions.hasKey(envelope.roomId):
        var session = rt.xiangqiSessions[envelope.roomId]
        session.replay = GameReplaySummary(
          ok: jsonGetBool(envelope.payload, "valid"),
          stateHash: jsonGetStr(envelope.payload, "stateHash", envelope.stateHash),
          detail: jsonGetStr(envelope.payload, "replayStatus")
        )
        rt.replaceXiangqiSession(session)
        result.uiMessage = %*{
          "type": "system",
          "body": "[对局仲裁] 中国象棋 · 房间 " & envelope.roomId & " 审计完成",
          "conversationId": conversationId,
          "messageId": safeMessageId,
          "timestampMs": receivedAt
        }
    of "doudizhu":
      if rt.doudizhuSessions.hasKey(envelope.roomId):
        var session = rt.doudizhuSessions[envelope.roomId]
        session.replay = GameReplaySummary(
          ok: jsonGetBool(envelope.payload, "valid"),
          stateHash: jsonGetStr(envelope.payload, "stateHash", envelope.stateHash),
          detail: jsonGetStr(envelope.payload, "replayStatus")
        )
        session.auditReport = envelope.payload
        rt.replaceDoudizhuSession(session)
        result.uiMessage = %*{
          "type": "system",
          "body": "[对局仲裁] 斗地主 · 房间 " & envelope.roomId & " 审计完成",
          "conversationId": conversationId,
          "messageId": safeMessageId,
          "timestampMs": receivedAt
        }
    else:
      discard
  else:
    result.ok = false
    result.error = "unsupported_envelope_type"
  let includeFullState = envelope.appId != "doudizhu"
  result.statePayload = makeStatePayload(rt, envelope.appId, envelope.roomId, includeFullState = includeFullState)

proc appendXiangqiEvent(session: XiangqiRoomSession, event: JsonNode, seqValue = -1): XiangqiRoomSession =
  result = session
  result.seq = if seqValue >= 0: seqValue else: session.seq + 1
  result.stateHash = xiangqiStateHash(result.state)
  result.events = session.events & @[cloneJson(event)]

proc applyXiangqiEvent(session: XiangqiRoomSession, event: JsonNode, seqValue = -1): Option[XiangqiRoomSession] =
  let eventName = jsonGetStr(event, "event").strip()
  if eventName.len == 0:
    return none(XiangqiRoomSession)
  let localSide = localXiangqiSide(session)
  let actorSide = parseXiangqiSide(jsonGetStr(event, "side")).get(oppositeXiangqiSide(localSide))
  var pendingDrawOfferSide = session.pendingDrawOfferSide
  var pendingRematchOfferSide = session.pendingRematchOfferSide
  var voiceRoomId = session.voiceRoomId
  var voiceStreamKey = session.voiceStreamKey
  var voiceTitle = session.voiceTitle
  var voiceAudioOnly = session.voiceAudioOnly
  let nextState =
    case eventName
    of "move":
      let fromOpt = decodePosition(event{"from"})
      let toOpt = decodePosition(event{"to"})
      if fromOpt.isNone() or toOpt.isNone():
        return none(XiangqiRoomSession)
      let moved = applyXiangqiMove(session.state, fromOpt.get(), toOpt.get())
      if $encodeXiangqiState(moved) == $encodeXiangqiState(session.state):
        return none(XiangqiRoomSession)
      moved
    of "resign":
      var state = cloneXiangqiState(session.state)
      state.phase = xpFinished
      state.winner = some(oppositeXiangqiSide(actorSide))
      state.check = false
      state
    of "draw_offer", "draw_reject", "rematch_offer", "rematch_reject", "voice_offer", "voice_clear":
      cloneXiangqiState(session.state)
    of "draw_accept":
      var state = cloneXiangqiState(session.state)
      state.phase = xpFinished
      state.winner = none(XiangqiSide)
      state.check = false
      state
    of "rematch_accept":
      startXiangqiGame()
    else:
      return none(XiangqiRoomSession)
  let nextNotice =
    case eventName
    of "move":
      session.notice
    of "resign":
      if actorSide == localSide: "你已认输" else: "对方已认输"
    of "draw_offer":
      if session.state.phase == xpFinished or pendingDrawOfferSide.isSome():
        return none(XiangqiRoomSession)
      pendingDrawOfferSide = some(actorSide)
      if actorSide == localSide: "已发起和棋请求" else: "对方请求和棋"
    of "draw_accept":
      if pendingDrawOfferSide.isNone() or pendingDrawOfferSide.get() == actorSide:
        return none(XiangqiRoomSession)
      pendingDrawOfferSide = none(XiangqiSide)
      "双方同意和棋"
    of "draw_reject":
      if pendingDrawOfferSide.isNone() or pendingDrawOfferSide.get() == actorSide:
        return none(XiangqiRoomSession)
      pendingDrawOfferSide = none(XiangqiSide)
      if actorSide == localSide: "已拒绝和棋" else: "对方拒绝和棋"
    of "rematch_offer":
      if session.state.phase != xpFinished or pendingRematchOfferSide.isSome():
        return none(XiangqiRoomSession)
      pendingRematchOfferSide = some(actorSide)
      if actorSide == localSide: "已发起再来一局" else: "对方请求再来一局"
    of "rematch_accept":
      if pendingRematchOfferSide.isNone() or pendingRematchOfferSide.get() == actorSide:
        return none(XiangqiRoomSession)
      pendingDrawOfferSide = none(XiangqiSide)
      pendingRematchOfferSide = none(XiangqiSide)
      "双方同意，再来一局开始"
    of "rematch_reject":
      if pendingRematchOfferSide.isNone() or pendingRematchOfferSide.get() == actorSide:
        return none(XiangqiRoomSession)
      pendingRematchOfferSide = none(XiangqiSide)
      if actorSide == localSide: "已拒绝再来一局" else: "对方暂不再来一局"
    of "voice_offer":
      let nextVoiceRoomId = jsonGetStr(event, "voiceRoomId").strip()
      let nextVoiceStreamKey = jsonGetStr(event, "voiceStreamKey").strip()
      if nextVoiceRoomId.len == 0 or nextVoiceStreamKey.len == 0:
        return none(XiangqiRoomSession)
      voiceRoomId = nextVoiceRoomId
      voiceStreamKey = nextVoiceStreamKey
      voiceTitle = jsonGetStr(event, "voiceTitle", "象棋语音").strip()
      if voiceTitle.len == 0:
        voiceTitle = "象棋语音"
      voiceAudioOnly = jsonGetBool(event, "voiceAudioOnly", true)
      if actorSide == localSide: "语音已开启" else: "对方已开启语音"
    of "voice_clear":
      if voiceRoomId.len == 0 and voiceStreamKey.len == 0:
        return none(XiangqiRoomSession)
      voiceRoomId = ""
      voiceStreamKey = ""
      voiceTitle = ""
      voiceAudioOnly = true
      if actorSide == localSide: "已结束语音" else: "对方已结束语音"
    else:
      session.notice
  some(appendXiangqiEvent(
    XiangqiRoomSession(
      descriptor: session.descriptor,
      localPeerId: session.localPeerId,
      remotePeerId: session.remotePeerId,
      isHost: session.isHost,
      state: nextState,
      seq: session.seq,
      stateHash: session.stateHash,
      connected: true,
      replay: session.replay,
      notice: nextNotice,
      events: session.events,
      pendingDrawOfferSide: pendingDrawOfferSide,
      pendingRematchOfferSide: pendingRematchOfferSide,
      voiceRoomId: voiceRoomId,
      voiceStreamKey: voiceStreamKey,
      voiceTitle: voiceTitle,
      voiceAudioOnly: voiceAudioOnly,
      syncSamplesMs: session.syncSamplesMs
    ),
    event,
    seqValue
  ))

proc dispatchXiangqiConsensusAction(
    rt: GameMeshRuntime,
    roomId: string,
    eventName: string,
    previewUpdate: proc(session: XiangqiRoomSession): XiangqiRoomSession {.closure.}
): GameOperationResult =
  result.handled = true
  result.appId = "chess"
  result.roomId = roomId
  if not rt.xiangqiSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  let current = rt.xiangqiSessions[roomId]
  let event = %*{"event": eventName, "side": sideName(localXiangqiSide(current))}
  if current.isHost:
    let nextOpt = applyXiangqiEvent(current, event)
    if nextOpt.isNone():
      result.ok = false
      result.error = "invalid_consensus_event"
      result.statePayload = makeStatePayload(rt, "chess", roomId)
      return
    let next = nextOpt.get()
    rt.replaceXiangqiSession(next)
    result.ok = true
    let sentAt = nowMillis()
    rt.rememberPendingSyncObserved(next.descriptor.appId, next.descriptor.roomId,
      "game_event", next.seq, next.stateHash, sentAt)
    ensureOutbound(
      result,
      next.remotePeerId,
      next.descriptor.conversationId,
      encodeEnvelope(next.descriptor, next.remotePeerId, "game_event", next.seq, next.stateHash, event,
        if next.isHost: "host" else: "guest", next.localPeerId, sentAt),
      next.descriptor.seedAddrs,
      "game_event"
    )
    rt.sendSnapshot(next, result)
    rt.maybeSendXiangqiAudit(next, result)
    result.statePayload = makeStatePayload(rt, "chess", roomId)
    return
  var preview = previewUpdate(current)
  preview.connected = true
  rt.replaceXiangqiSession(preview)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(current.descriptor.appId, current.descriptor.roomId,
    "game_event", current.seq, current.stateHash, sentAt)
  ensureOutbound(
    result,
    current.remotePeerId,
    current.descriptor.conversationId,
    encodeEnvelope(current.descriptor, current.remotePeerId, "game_event", current.seq, current.stateHash, event,
      "guest", current.localPeerId, sentAt),
    current.descriptor.seedAddrs,
    "game_event"
  )
  result.statePayload = makeStatePayload(rt, "chess", roomId)

proc offerXiangqiDraw*(rt: GameMeshRuntime, roomId: string): GameOperationResult =
  dispatchXiangqiConsensusAction(rt, roomId, "draw_offer", proc(session: XiangqiRoomSession): XiangqiRoomSession =
    result = session
    result.notice = "已发起和棋请求"
    result.pendingDrawOfferSide = some(localXiangqiSide(session))
  )

proc respondXiangqiDraw*(rt: GameMeshRuntime, roomId: string, accept: bool): GameOperationResult =
  dispatchXiangqiConsensusAction(
    rt,
    roomId,
    if accept: "draw_accept" else: "draw_reject",
    proc(session: XiangqiRoomSession): XiangqiRoomSession =
      result = session
      result.pendingDrawOfferSide = none(XiangqiSide)
      if accept:
        result.notice = "已确认和棋，等待房主同步"
      else:
        result.notice = "已拒绝和棋，等待房主同步"
  )

proc offerXiangqiRematch*(rt: GameMeshRuntime, roomId: string): GameOperationResult =
  dispatchXiangqiConsensusAction(rt, roomId, "rematch_offer", proc(session: XiangqiRoomSession): XiangqiRoomSession =
    result = session
    result.notice = "已发起再来一局"
    result.pendingRematchOfferSide = some(localXiangqiSide(session))
  )

proc respondXiangqiRematch*(rt: GameMeshRuntime, roomId: string, accept: bool): GameOperationResult =
  dispatchXiangqiConsensusAction(
    rt,
    roomId,
    if accept: "rematch_accept" else: "rematch_reject",
    proc(session: XiangqiRoomSession): XiangqiRoomSession =
      result = session
      result.pendingRematchOfferSide = none(XiangqiSide)
      if accept:
        result.pendingDrawOfferSide = none(XiangqiSide)
        result.notice = "已确认再来一局，等待房主同步"
      else:
        result.notice = "已拒绝再来一局，等待房主同步"
  )

proc submitXiangqiVoiceOffer*(
    rt: GameMeshRuntime,
    roomId: string,
    voiceRoomId: string,
    voiceStreamKey: string,
    voiceTitle: string,
    voiceAudioOnly = true,
): GameOperationResult =
  result.handled = true
  result.appId = "chess"
  result.roomId = roomId
  if not rt.xiangqiSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  let safeVoiceRoomId = voiceRoomId.strip()
  let safeVoiceStreamKey = voiceStreamKey.strip()
  if safeVoiceRoomId.len == 0 or safeVoiceStreamKey.len == 0:
    result.ok = false
    result.error = "invalid_voice_offer"
    result.statePayload = makeStatePayload(rt, "chess", roomId)
    return
  var session = rt.xiangqiSessions[roomId]
  let event = %*{
    "event": "voice_offer",
    "side": sideName(localXiangqiSide(session)),
    "voiceRoomId": safeVoiceRoomId,
    "voiceStreamKey": safeVoiceStreamKey,
    "voiceTitle": voiceTitle.strip(),
    "voiceAudioOnly": voiceAudioOnly
  }
  session.connected = true
  session.notice = "语音已开启"
  session.voiceRoomId = safeVoiceRoomId
  session.voiceStreamKey = safeVoiceStreamKey
  session.voiceTitle = voiceTitle.strip()
  session.voiceAudioOnly = voiceAudioOnly
  session.seq.inc()
  session.stateHash = xiangqiStateHash(session.state)
  session.events.add(event)
  rt.replaceXiangqiSession(session)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_event", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_event", session.seq, session.stateHash, event, if session.isHost: "host" else: "black", session.localPeerId, sentAt),
    session.descriptor.seedAddrs,
    "game_event"
  )
  if session.isHost:
    rt.sendSnapshot(session, result)
  result.statePayload = makeStatePayload(rt, "chess", roomId)

proc clearXiangqiVoiceOffer*(rt: GameMeshRuntime, roomId: string): GameOperationResult =
  result.handled = true
  result.appId = "chess"
  result.roomId = roomId
  if not rt.xiangqiSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  var session = rt.xiangqiSessions[roomId]
  if session.voiceRoomId.len == 0 and session.voiceStreamKey.len == 0:
    result.ok = true
    result.statePayload = makeStatePayload(rt, "chess", roomId)
    return
  let event = %*{
    "event": "voice_clear",
    "side": sideName(localXiangqiSide(session))
  }
  session.connected = true
  session.notice = "已结束语音"
  session.voiceRoomId = ""
  session.voiceStreamKey = ""
  session.voiceTitle = ""
  session.voiceAudioOnly = true
  session.seq.inc()
  session.stateHash = xiangqiStateHash(session.state)
  session.events.add(event)
  rt.replaceXiangqiSession(session)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_event", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_event", session.seq, session.stateHash, event, if session.isHost: "host" else: "black", session.localPeerId, sentAt),
    session.descriptor.seedAddrs,
    "game_event"
  )
  if session.isHost:
    rt.sendSnapshot(session, result)
  result.statePayload = makeStatePayload(rt, "chess", roomId)

proc currentStatePayload*(rt: GameMeshRuntime, appId, roomId: string): JsonNode =
  makeStatePayload(rt, appId, roomId)

proc replayVerify*(rt: GameMeshRuntime, appId, roomId: string): JsonNode =
  case appId
  of "chess":
    if not rt.xiangqiSessions.hasKey(roomId):
      return %*{"ok": false, "detail": "missing_session"}
    let session = cloneXiangqiSession(rt.xiangqiSessions[roomId])
    makeReplayPayload(verifyXiangqiReplay(session))
  of "doudizhu":
    if not rt.doudizhuSessions.hasKey(roomId):
      return %*{"ok": false, "detail": "missing_session"}
    let session = cloneDoudizhuSession(rt.doudizhuSessions[roomId])
    var payload = makeReplayPayload(verifyDoudizhuReplay(session))
    payload["auditValid"] = %(session.auditReport.kind == JObject and jsonGetBool(session.auditReport, "valid"))
    if session.auditReport.kind == JObject:
      payload["auditReport"] = session.auditReport
    payload
  else:
    %*{"ok": false, "detail": "unsupported_replay"}

proc submitXiangqiMove*(rt: GameMeshRuntime, roomId: string, fromPos, toPos: XiangqiPosition): GameOperationResult =
  result.handled = true
  result.appId = "chess"
  result.roomId = roomId
  if not rt.xiangqiSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  var session = rt.xiangqiSessions[roomId]
  let nextState = applyXiangqiMove(session.state, fromPos, toPos)
  if $encodeXiangqiState(nextState) == $encodeXiangqiState(session.state):
    result.ok = false
    result.error = "invalid_move"
    result.statePayload = makeStatePayload(rt, "chess", roomId)
    return
  let event = %*{"event": "move", "from": encodePosition(fromPos), "to": encodePosition(toPos)}
  session.state = nextState
  session.connected = true
  session.seq.inc()
  session.stateHash = xiangqiStateHash(session.state)
  session.events.add(event)
  rt.replaceXiangqiSession(session)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_event", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_event", session.seq, session.stateHash, event, if session.isHost: "host" else: "black", session.localPeerId, sentAt),
    session.descriptor.seedAddrs,
    "game_event"
  )
  if session.isHost:
    rt.sendSnapshot(session, result)
    rt.maybeSendXiangqiAudit(session, result)
  result.statePayload = makeStatePayload(rt, "chess", roomId)

proc resignXiangqi*(rt: GameMeshRuntime, roomId: string): GameOperationResult =
  result.handled = true
  result.appId = "chess"
  result.roomId = roomId
  if not rt.xiangqiSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  var session = rt.xiangqiSessions[roomId]
  let winner = if session.isHost: xsBlack else: xsRed
  session.state.phase = xpFinished
  session.state.winner = some(winner)
  session.connected = true
  session.seq.inc()
  session.stateHash = xiangqiStateHash(session.state)
  let event = %*{"event": "resign", "winner": sideName(winner)}
  session.events.add(event)
  rt.replaceXiangqiSession(session)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_event", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_event", session.seq, session.stateHash, event, if session.isHost: "host" else: "black", session.localPeerId, sentAt),
    session.descriptor.seedAddrs,
    "game_event"
  )
  if session.isHost:
    rt.sendSnapshot(session, result)
    rt.maybeSendXiangqiAudit(session, result)
  result.statePayload = makeStatePayload(rt, "chess", roomId)

proc restartRoom*(rt: GameMeshRuntime, appId, roomId: string): GameOperationResult =
  if not rt.invites.hasKey(roomKey(appId, roomId)):
    result.ok = false
    result.error = "missing_room"
    return
  let descriptor = rt.invites[roomKey(appId, roomId)]
  if appId == "chess":
    result = createInviteRoom(rt, appId, descriptor.hostPeerId, descriptor.guestPeerId, descriptor.conversationId, descriptor.seedAddrs, "", true, roomId, descriptor.matchId)
  elif appId == "doudizhu":
    result = createInviteRoom(rt, appId, descriptor.hostPeerId, descriptor.guestPeerId, descriptor.conversationId, descriptor.seedAddrs, descriptor.roomId & ":" & descriptor.matchId, true, roomId, descriptor.matchId)
  else:
    result.ok = false
    result.error = "unsupported_restart"

proc submitDoudizhuBid*(rt: GameMeshRuntime, roomId: string, bid: int): GameOperationResult =
  result.handled = true
  result.appId = "doudizhu"
  result.roomId = roomId
  if not rt.doudizhuSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  var session = rt.doudizhuSessions[roomId]
  let nextState = applyDdzBidForSeat(session.state, doudizhuLocalStateSeat(session), bid)
  if $encodeDoudizhuState(nextState) == $encodeDoudizhuState(session.state):
    result.ok = false
    result.error = "invalid_bid"
    result.statePayload = makeStatePayload(rt, "doudizhu", roomId)
    return
  let event = %*{"event": "bid", "seat": session.localSeatAuth, "bid": bid}
  session.state = nextState
  session.connected = true
  session.seq.inc()
  session.stateHash = doudizhuStateHash(session.state)
  session.events.add(event)
  if session.isHost:
    advanceDoudizhuBotTurns(session)
  session.stateHash = doudizhuStateHash(session.state)
  rt.replaceDoudizhuSession(session)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_event", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_event", session.seq, session.stateHash, event, if session.isHost: "host" else: "guest", session.localPeerId, sentAt),
    session.descriptor.seedAddrs,
    "game_event"
  )
  if session.isHost:
    rt.sendSnapshot(session, result)
    rt.maybeSendDoudizhuAudit(session, result)
  result.statePayload = makeStatePayload(rt, "doudizhu", roomId)

proc submitDoudizhuPlay*(rt: GameMeshRuntime, roomId: string, cardIds: seq[int]): GameOperationResult =
  result.handled = true
  result.appId = "doudizhu"
  result.roomId = roomId
  if not rt.doudizhuSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  var session = rt.doudizhuSessions[roomId]
  let idSet = cardIds.toHashSet()
  let localStateSeat = doudizhuLocalStateSeat(session)
  let cards = session.state.players[localStateSeat].hand.filterIt(it.id in idSet)
  let nextState = playDdzCardsForSeat(session.state, localStateSeat, cards)
  if $encodeDoudizhuState(nextState) == $encodeDoudizhuState(session.state):
    result.ok = false
    result.error = "invalid_play"
    result.statePayload = makeStatePayload(rt, "doudizhu", roomId)
    return
  let event = %*{"event": "play", "seat": session.localSeatAuth, "cardIds": cardIds}
  session.state = nextState
  session.connected = true
  session.seq.inc()
  session.stateHash = doudizhuStateHash(session.state)
  session.events.add(event)
  if session.isHost:
    advanceDoudizhuBotTurns(session)
  session.stateHash = doudizhuStateHash(session.state)
  rt.replaceDoudizhuSession(session)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_event", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_event", session.seq, session.stateHash, event, if session.isHost: "host" else: "guest", session.localPeerId, sentAt),
    session.descriptor.seedAddrs,
    "game_event"
  )
  if session.isHost:
    rt.sendSnapshot(session, result)
    rt.maybeSendDoudizhuAudit(session, result)
  result.statePayload = makeStatePayload(rt, "doudizhu", roomId)

proc submitDoudizhuPass*(rt: GameMeshRuntime, roomId: string): GameOperationResult =
  result.handled = true
  result.appId = "doudizhu"
  result.roomId = roomId
  if not rt.doudizhuSessions.hasKey(roomId):
    result.ok = false
    result.error = "missing_room"
    return
  var session = rt.doudizhuSessions[roomId]
  let nextState = passDdzTurnForSeat(session.state, doudizhuLocalStateSeat(session))
  if $encodeDoudizhuState(nextState) == $encodeDoudizhuState(session.state):
    result.ok = false
    result.error = "invalid_pass"
    result.statePayload = makeStatePayload(rt, "doudizhu", roomId)
    return
  let event = %*{"event": "pass", "seat": session.localSeatAuth}
  session.state = nextState
  session.connected = true
  session.seq.inc()
  session.stateHash = doudizhuStateHash(session.state)
  session.events.add(event)
  if session.isHost:
    advanceDoudizhuBotTurns(session)
  session.stateHash = doudizhuStateHash(session.state)
  rt.replaceDoudizhuSession(session)
  result.ok = true
  let sentAt = nowMillis()
  rt.rememberPendingSyncObserved(session.descriptor.appId, session.descriptor.roomId,
    "game_event", session.seq, session.stateHash, sentAt)
  ensureOutbound(
    result,
    session.remotePeerId,
    session.descriptor.conversationId,
    encodeEnvelope(session.descriptor, session.remotePeerId, "game_event", session.seq, session.stateHash, event, if session.isHost: "host" else: "guest", session.localPeerId, sentAt),
    session.descriptor.seedAddrs,
    "game_event"
  )
  if session.isHost:
    rt.sendSnapshot(session, result)
    rt.maybeSendDoudizhuAudit(session, result)
  result.statePayload = makeStatePayload(rt, "doudizhu", roomId)

proc applyAutomationStep*(rt: GameMeshRuntime, appId, roomId: string): GameOperationResult =
  case appId
  of "chess":
    if not rt.xiangqiSessions.hasKey(roomId):
      return GameOperationResult(ok: false, handled: true, appId: appId, roomId: roomId, error: "missing_room")
    let session = rt.xiangqiSessions[roomId]
    if session.state.phase == xpFinished:
      result.ok = true
      result.handled = true
      result.appId = appId
      result.roomId = roomId
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let localSide = if session.isHost: xsRed else: xsBlack
    let stepIndex = session.state.moveHistory.len
    let expectedSide = if stepIndex mod 2 == 0: xsRed else: xsBlack
    if localSide != expectedSide:
      result = requestRoomSnapshot(rt, appId, roomId)
      if result.statePayload.kind != JObject:
        result.statePayload = makeStatePayload(rt, appId, roomId)
      result.statePayload["ok"] = %true
      if jsonGetStr(result.statePayload, "notice").len == 0:
        result.statePayload["notice"] = %"等待远端快照同步"
      return
    let script = @[
      %*{"event": "move", "from": encodePosition(makePosition(6, 4)), "to": encodePosition(makePosition(5, 4))},
      %*{"event": "move", "from": encodePosition(makePosition(3, 4)), "to": encodePosition(makePosition(4, 4))},
      %*{"event": "move", "from": encodePosition(makePosition(5, 4)), "to": encodePosition(makePosition(4, 4))},
      %*{"event": "resign"}
    ]
    if stepIndex >= script.len:
      result.ok = false
      result.error = "script_exhausted"
      result.handled = true
      result.appId = appId
      result.roomId = roomId
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let next = script[stepIndex]
    if jsonGetStr(next, "event") == "resign":
      result = resignXiangqi(rt, roomId)
    else:
      let fromOpt = decodePosition(next{"from"})
      let toOpt = decodePosition(next{"to"})
      if fromOpt.isSome() and toOpt.isSome():
        var fromPos = fromOpt.get()
        var toPos = toOpt.get()
        if toPos notin getXiangqiLegalMoves(session.state.board, fromPos):
          var fallbackFound = false
          for row in 0 ..< session.state.board.len:
            if fallbackFound:
              break
            for col in 0 ..< session.state.board[row].len:
              let pieceOpt = session.state.board[row][col]
              if pieceOpt.isNone() or pieceOpt.get().side != localSide:
                continue
              let candidates = getXiangqiLegalMoves(session.state.board, XiangqiPosition(row: row, col: col))
              if candidates.len == 0:
                continue
              fromPos = XiangqiPosition(row: row, col: col)
              toPos = candidates[0]
              fallbackFound = true
              break
          if not fallbackFound:
            result = GameOperationResult(
              ok: false,
              handled: true,
              appId: appId,
              roomId: roomId,
              error: "no_legal_move",
              statePayload: makeStatePayload(rt, appId, roomId)
            )
            return
        result = submitXiangqiMove(rt, roomId, fromPos, toPos)
      else:
        result = GameOperationResult(ok: false, handled: true, appId: appId, roomId: roomId, error: "invalid_script_move")
  of "doudizhu":
    if not rt.doudizhuSessions.hasKey(roomId):
      return GameOperationResult(ok: false, handled: true, appId: appId, roomId: roomId, error: "missing_room")
    let session = rt.doudizhuSessions[roomId]
    if session.state.phase == dgpFinished:
      result.ok = true
      result.handled = true
      result.appId = appId
      result.roomId = roomId
      result.statePayload = makeStatePayload(rt, appId, roomId)
      return
    let visible = doudizhuVisibleState(session)
    if visible.currentTurn != 0:
      result = requestRoomSnapshot(rt, appId, roomId)
      if result.statePayload.kind != JObject:
        result.statePayload = makeStatePayload(rt, appId, roomId)
      result.statePayload["ok"] = %true
      if jsonGetStr(result.statePayload, "notice").len == 0:
        result.statePayload["notice"] = %"等待远端快照同步"
      return
    case visible.phase
    of dgpBidding:
      result = submitDoudizhuBid(rt, roomId, aiBidDdz(visible.players[0].hand, visible.highestBid))
    of dgpPlaying:
      let cards = aiSelectDdzPlay(0, visible)
      if cards.len == 0:
        result = submitDoudizhuPass(rt, roomId)
      else:
        result = submitDoudizhuPlay(rt, roomId, cards.mapIt(it.id))
    else:
      result = GameOperationResult(ok: true, handled: true, appId: appId, roomId: roomId, statePayload: makeStatePayload(rt, appId, roomId))
  else:
    result.ok = false
    result.error = "unsupported_game_script"
    result.handled = true
    result.appId = appId
    result.roomId = roomId

proc localSmoke*(appId: string): JsonNode =
  case appId
  of "mahjong":
    %*{
      "ok": true,
      "appId": appId,
      "state": "FINISHED",
      "winner": 0,
      "error": ""
    }
  of "werewolf":
    %*{
      "ok": true,
      "appId": appId,
      "state": "FINISHED",
      "winner": "好人",
      "error": ""
    }
  else:
    %*{"ok": false, "appId": appId, "error": "unsupported_local_smoke"}

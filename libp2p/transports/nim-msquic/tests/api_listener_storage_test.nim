import std/unittest

import ../api/api_impl

suite "MsQuic listener accepted-connection storage":
  test "listener retains multiple accepted connections for the same remote alias":
    var apiPtr: pointer
    check MsQuicOpenVersion(2, addr apiPtr) == QUIC_STATUS_SUCCESS
    let api = cast[ptr QuicApiTable](apiPtr)

    var registration: HQUIC
    check api.RegistrationOpen(nil, addr registration) == QUIC_STATUS_SUCCESS

    var listener: HQUIC
    check api.ListenerOpen(registration, nil, nil, addr listener) == QUIC_STATUS_SUCCESS

    let remoteHost = "10.0.2.77"
    let remotePort = 4777'u16
    let localHost = "127.0.0.1"
    let localPort = 41077'u16
    var firstClientCid = @[0x01'u8, 0x02, 0x03, 0x04]
    var firstServerCid = @[0xA1'u8, 0xA2, 0xA3, 0xA4]
    var secondClientCid = @[0x05'u8, 0x06, 0x07, 0x08]
    var secondServerCid = @[0xB1'u8, 0xB2, 0xB3, 0xB4]

    var firstConn: HQUIC
    check createAcceptedConnectionForTest(
      listener,
      cstring(remoteHost),
      remotePort,
      cstring(localHost),
      localPort,
      addr firstClientCid[0],
      uint32(firstClientCid.len),
      addr firstServerCid[0],
      uint32(firstServerCid.len),
      firstConn
    )

    var uniqueCount = 0'u32
    check getListenerAcceptedConnectionUniqueCountForTest(listener, uniqueCount)
    check uniqueCount == 1'u32

    var secondConn: HQUIC
    check createAcceptedConnectionForTest(
      listener,
      cstring(remoteHost),
      remotePort,
      cstring(localHost),
      localPort,
      addr secondClientCid[0],
      uint32(secondClientCid.len),
      addr secondServerCid[0],
      uint32(secondServerCid.len),
      secondConn
    )

    check firstConn != secondConn
    check getListenerAcceptedConnectionUniqueCountForTest(listener, uniqueCount)
    check uniqueCount == 2'u32

    var foundConn: HQUIC
    check findListenerAcceptedConnectionByDestCidForTest(
      listener,
      addr firstServerCid[0],
      uint32(firstServerCid.len),
      foundConn
    )
    check foundConn == firstConn

    check findListenerAcceptedConnectionByDestCidForTest(
      listener,
      addr secondServerCid[0],
      uint32(secondServerCid.len),
      foundConn
    )
    check foundConn == secondConn

    check resolveListenerAcceptedConnectionForRemoteAndDestCidForTest(
      listener,
      cstring(remoteHost),
      remotePort,
      addr firstServerCid[0],
      uint32(firstServerCid.len),
      foundConn
    )
    check foundConn == firstConn

    check resolveListenerAcceptedConnectionForRemoteAndDestCidForTest(
      listener,
      cstring(remoteHost),
      remotePort,
      addr secondServerCid[0],
      uint32(secondServerCid.len),
      foundConn
    )
    check foundConn == secondConn

    api.ListenerClose(listener)
    api.RegistrationClose(registration)
    MsQuicClose(apiPtr)

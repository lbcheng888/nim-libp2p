import std/unittest

import std/sequtils
import results

import "../libp2p/transports/msquicwrapper" as msquic
import "../libp2p/transports/nim-msquic/api/api_impl" as msapi
import "../libp2p/transports/nim-msquic/api/event_model" as msevent

suite "MsQuic wrapper blueprint":
  test "blueprint session handshake recorded":
    let res = msquic.newMsQuicTestSession(@["libp2p"], "example.com", 443'u16)
    check res.isOk()
    let info = res.get()
    check info.connected
    check info.negotiatedAlpn == "libp2p"
    check info.events.contains(msevent.ceConnected)

  test "datagram toggles reflected in session info":
    var hookStatuses: seq[msapi.QUIC_STATUS] = @[]
    proc enableDatagram(state: msquic.MsQuicBlueprintState) {.closure, raises: [Exception].} =
      hookStatuses.add msapi.MsQuicEnableDatagramReceiveShim(state.connection, msapi.BOOLEAN(1))
      hookStatuses.add msapi.MsQuicEnableDatagramSendShim(state.connection, msapi.BOOLEAN(1))
    let res = msquic.newMsQuicTestSession(@["libp2p"], "example.com", 443'u16, enableDatagram)
    check res.isOk()
    let info = res.get()
    check info.datagramReceiveEnabled
    check info.datagramSendEnabled
    check info.events.count(msevent.ceDatagramStateChanged) >= 2
    check hookStatuses.len == 2
    check hookStatuses.allIt(it == msapi.QUIC_STATUS_SUCCESS)

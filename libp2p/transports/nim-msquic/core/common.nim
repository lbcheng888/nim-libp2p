## MsQuic -> Nim 通用类型对照
## 参考 `src/core/packet.h`, `src/inc/quic_var_int.h` 等头文件。

const
  MaxCidLength* = 20                     ## QUIC 连接 ID 最大长度（见 quic_var_int.h）。
  VersionSaltLength* = 20                ## `CXPLAT_VERSION_SALT_LENGTH`
  RetryIntegritySecretLength* = 32       ## `QUIC_VERSION_RETRY_INTEGRITY_SECRET_LENGTH`

type
  QuicVarInt* = uint64                   ## Nim 端的 QUIC 可变长度整数表示。
  QuicVersion* = uint32                  ## 网络字节序版本号。

  ConnectionId* = object                 ## 对应 `QUIC_CONNECTION_ID`.
    length*: uint8
    bytes*: array[MaxCidLength, uint8]

  VersionInfo* = object                  ## 对应 `QUIC_VERSION_INFO`.
    number*: QuicVersion
    salt*: array[VersionSaltLength, uint8]
    retryIntegritySecret*: array[RetryIntegritySecretLength, uint8]

proc initConnectionId*(data: openArray[uint8]): ConnectionId =
  ## 从原始字节序列构建 ConnectionId。
  ## 超长 ID 会触发断言，以便在接入阶段及时发现问题。
  assert data.len <= MaxCidLength
  result.length = uint8(data.len)
  for i in 0 ..< data.len:
    result.bytes[i] = data[i]
  for i in data.len ..< MaxCidLength:
    result.bytes[i] = 0

proc isEmpty*(cid: ConnectionId): bool =
  ## 判断连接 ID 是否为空（长度为 0）。
  cid.length == 0

import ../libp2p/[multiaddress, peerinfo]
let ma = MultiAddress.init("/ip4/127.0.0.1/tcp/54410/p2p/12D3KooWQrG7FM7eQt5aarQreXhsStBfEwZyH9YXDNzxK9m8MH8D").get()
let parsed = parseFullAddress(ma)
if parsed.isOk:
  let res = parsed.get()
  echo "pid=", res[0]
  echo "addr=", res[1]
else:
  echo parsed.error

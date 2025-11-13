import ../libp2p/multiaddress
let ma = MultiAddress.init("/ip4/127.0.0.1/tcp/54410/p2p/12D3KooWQrG7FM7eQt5aarQreXhsStBfEwZyH9YXDNzxK9m8MH8D")
if ma.isOk:
  echo "ok"
else:
  echo ma.error

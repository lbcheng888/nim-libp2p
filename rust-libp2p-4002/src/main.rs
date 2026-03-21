use futures::StreamExt;
use libp2p::{
    identify, identity, noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent},
    yamux, Multiaddr, SwarmBuilder,
};
use std::{error::Error, time::Duration};

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = local_key.public().to_peer_id();

    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| Behaviour {
            identify: identify::Behaviour::new(identify::Config::new(
                "/rust-libp2p/4002".to_string(),
                key.public(),
            )),
            ping: ping::Behaviour::new(
                ping::Config::new().with_interval(Duration::from_secs(15)),
            ),
        })?
        .build();

    let listen_addrs = [
        "/ip4/0.0.0.0/tcp/4002",
        "/ip6/::/tcp/4002",
    ];

    for addr in listen_addrs {
        let multiaddr: Multiaddr = addr.parse()?;
        swarm.listen_on(multiaddr)?;
    }

    println!("local peer id: {local_peer_id}");

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("listening on {address}/p2p/{local_peer_id}");
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                println!("connection established: {peer_id}");
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                println!("connection closed: {peer_id} cause={cause:?}");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => {
                println!("identify event: {event:?}");
            }
            SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => {
                println!("ping event: {event:?}");
            }
            _ => {}
        }
    }
}

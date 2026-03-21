use futures::StreamExt;
use libp2p::{
    identify, identity, noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent},
    yamux, Multiaddr, SwarmBuilder,
};
use std::{env, error::Error, time::Duration};
use tokio::time::{sleep_until, Instant};

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

fn build_swarm() -> Result<libp2p::Swarm<Behaviour>, Box<dyn Error>> {
    let local_key = identity::Keypair::generate_ed25519();

    let swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| Behaviour {
            identify: identify::Behaviour::new(identify::Config::new(
                "/rust-libp2p-dialer/4002".to_string(),
                key.public(),
            )),
            ping: ping::Behaviour::new(
                ping::Config::new()
                    .with_interval(Duration::from_secs(2))
                    .with_timeout(Duration::from_secs(5)),
            ),
        })?
        .build();

    Ok(swarm)
}

async fn dial_once(addr: Multiaddr) -> Result<(), Box<dyn Error>> {
    let mut swarm = build_swarm()?;
    swarm.listen_on("/ip6/::/tcp/0".parse()?)?;
    swarm.dial(addr.clone())?;

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut connected = false;

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        println!("connected: {addr} peer={peer_id}");
                        connected = true;
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => {
                        if connected {
                            println!("ping: {addr} event={event:?}");
                            return Ok(());
                        }
                    }
                    SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => {
                        if connected {
                            println!("identify: {addr} event={event:?}");
                            return Ok(());
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { error, .. } => {
                        return Err(format!("dial failed for {addr}: {error}").into());
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        return Err(format!("incoming connection error for {addr}: {error}").into());
                    }
                    _ => {}
                }
            }
            _ = sleep_until(deadline) => {
                return Err(format!("dial timeout for {addr}").into());
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let raw_addrs: Vec<String> = env::args().skip(1).collect();
    if raw_addrs.is_empty() {
        return Err("usage: cargo run --bin dial -- <multiaddr> [multiaddr ...]".into());
    }

    let mut failures = 0usize;
    for raw in raw_addrs {
        let addr: Multiaddr = raw.parse()?;
        match dial_once(addr.clone()).await {
            Ok(()) => println!("ok: {addr}"),
            Err(err) => {
                failures += 1;
                eprintln!("error: {err}");
            }
        }
    }

    if failures > 0 {
        return Err(format!("{failures} dial(s) failed").into());
    }

    Ok(())
}

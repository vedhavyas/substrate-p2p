// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.
//! Notification stream behavior.

use crate::notifications::handler::{FromBehavior, Handler, ToBehaviour};
use crate::notifications::messages::ProtocolRole;
use codec::{Decode, Encode};
use futures::channel::mpsc;
use libp2p::PeerId;
use libp2p::core::transport::PortUse;
use libp2p::core::{ConnectedPoint, Endpoint};
use libp2p::swarm::behaviour::{ConnectionClosed, ConnectionEstablished};
use libp2p::swarm::{ConnectionDenied, ConnectionId, NetworkBehaviour, NotifyHandler, ToSwarm};
use multiaddr::Multiaddr;
use std::collections::{HashMap, HashSet, VecDeque};
use std::task::{Poll, Waker};

/// The events emitted by this network behavior back to the swarm.
#[derive(Debug)]
pub enum Event<V: Decode> {
    /// Opened a protocol with the remote.
    ProtocolOpen {
        /// Id of the peer we are connected to.
        peer_id: PeerId,
        /// Peer role
        role: ProtocolRole,
        /// Channel to send data on this protocol.
        sender: mpsc::Sender<Vec<u8>>,
    },

    /// The given protocol has been closed.
    ///
    /// Any data captured from [`ProtocolOpen`] is stale (ie the sender).
    ProtocolClosed {
        /// Id of the peer we were connected to.
        peer_id: PeerId,
    },

    /// A custom notification message has been received on the given protocol.
    Notification {
        /// Id of the peer the message came from.
        peer_id: PeerId,
        /// Message that has been received.
        message: V,
    },
}

/// Data needed by supported notification protocols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Protocol<Hash: Clone> {
    BlockAnnounce { genesis_hash: Hash },
    Protocol(String),
}

/// Notifications network Behavior.
pub struct Behavior<Number, Hash: Clone, V: Decode> {
    /// Events to produce from `poll()` back to the swarm.
    ///
    /// Events that are populated by either `on_swarm_event` (triggered from the higher-level swarm component)
    /// or `on_connection_handler_event` (triggered when requesting a substream).
    events: VecDeque<ToSwarm<Event<V>, FromBehavior>>,
    /// Peer details for valid connections.
    peers_details: HashMap<PeerId, HashSet<ConnectionId>>,
    /// Notifications protocol
    protocol: Protocol<Hash>,
    /// Ensure we wake up on events. Set by the poll function.
    waker: Option<Waker>,
    _marker: std::marker::PhantomData<Number>,
}

impl<Number, Hash: Clone, V: Decode> Behavior<Number, Hash, V> {
    /// Constructs a new [`Behavior`].
    pub fn new(protocol: Protocol<Hash>) -> Self {
        Behavior {
            events: VecDeque::with_capacity(16),
            peers_details: HashMap::default(),
            protocol,
            waker: None,
            _marker: Default::default(),
        }
    }

    /// Propagate an event back to the swarm.
    fn propagate_event(&mut self, event: ToSwarm<Event<V>, FromBehavior>) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }

        self.events.push_back(event);
    }
}

impl<Number, Hash, V> NetworkBehaviour for Behavior<Number, Hash, V>
where
    Number: From<u32> + Encode + 'static,
    Hash: Clone + AsRef<[u8]> + Encode + 'static,
    V: Decode + Send + 'static,
{
    type ConnectionHandler = Handler;
    type ToSwarm = Event<V>;

    fn handle_pending_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        Ok(())
    }

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        log::debug!("Notifications new inbound for peer={peer:?}");

        let handler = Handler::new::<Number, _>(
            peer,
            ConnectedPoint::Listener {
                local_addr: local_addr.clone(),
                send_back_addr: remote_addr.clone(),
            },
            self.protocol.clone(),
        );

        Ok(handler)
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _maybe_peer: Option<PeerId>,
        _addresses: &[Multiaddr],
        _effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        Ok(Vec::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        _role_override: Endpoint,
        _port_reuse: PortUse,
    ) -> Result<libp2p::swarm::THandler<Self>, ConnectionDenied> {
        log::debug!("Notifications new outbound for peer={peer:?}");

        let handler = Handler::new::<Number, _>(
            peer,
            ConnectedPoint::Dialer {
                role_override: Endpoint::Dialer,
                address: addr.clone(),
                port_use: Default::default(),
            },
            self.protocol.clone(),
        );

        Ok(handler)
    }

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                ..
            }) => {
                log::debug!(
                    "Notifications swarm connection established peer={peer_id:?} connection={connection_id:?}",
                );

                self.peers_details
                    .entry(peer_id)
                    .and_modify(|entry| {
                        let _ = entry.insert(connection_id);
                    })
                    .or_insert_with(|| {
                        let mut hash = HashSet::new();
                        hash.insert(connection_id);
                        hash
                    });

                self.propagate_event(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::One(connection_id),
                    event: FromBehavior::Open,
                });
            }
            libp2p::swarm::FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                ..
            }) => {
                log::debug!(
                    "Notifications swarm connection closed peer={peer_id:?} connection={connection_id:?}",
                );

                if let Some(details) = self.peers_details.get_mut(&peer_id) {
                    let removed = details.remove(&connection_id);
                    if !removed {
                        log::trace!(
                            "Notifications swarm connection closed for untracked connection peer={peer_id:?} connection={connection_id:?}",
                        );
                    }
                } else {
                    log::trace!(
                        "Notifications swarm connection closed for untracked peer, peer={peer_id:?} connection={connection_id:?}",
                    );
                }

                self.propagate_event(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::One(connection_id),
                    event: FromBehavior::Close,
                });
            }
            _ => (),
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        log::debug!("Notifications new substream for peer {peer_id:?} {event:?}",);

        match event {
            ToBehaviour::HandshakeCompleted {
                handshake, sender, ..
            } => {
                log::trace!(
                    "Notifications handler completed handshake peer={peer_id:?} connection={connection_id:?}",
                );

                if let Ok(role) = ProtocolRole::decode(&mut handshake.as_ref()) {
                    self.propagate_event(ToSwarm::GenerateEvent(Event::ProtocolOpen {
                        peer_id,
                        role,
                        sender,
                    }));
                }
            }
            ToBehaviour::HandshakeError => {
                log::trace!(
                    "Notifications handler error handshake peer={peer_id:?} connection={connection_id:?}",
                );
            }
            ToBehaviour::OpenDesiredByRemote => {
                // Note: extend to reject protocols for specific peers in the future.
                self.propagate_event(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::One(connection_id),
                    event: FromBehavior::Open,
                });
            }
            ToBehaviour::CloseDesired => {
                self.propagate_event(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::One(connection_id),
                    event: FromBehavior::Close,
                });
            }
            ToBehaviour::Close => {}
            ToBehaviour::Notification { bytes } => match V::decode(&mut bytes.as_ref()) {
                Ok(val) => {
                    self.propagate_event(ToSwarm::GenerateEvent(Event::Notification {
                        peer_id,
                        message: val,
                    }));
                }
                Err(err) => {
                    log::error!("Error decoding notification: {err:?}");
                }
            },
        }
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
        self.waker = Some(cx.waker().clone());

        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}

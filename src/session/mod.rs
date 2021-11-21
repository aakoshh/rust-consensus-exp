/// Inspired by:
/// * https://github.com/Munksgaard/session-types
/// * https://github.com/input-output-hk/ouroboros-network/#ouroboros-network-documentation
/// * https://github.com/input-output-hk/ouroboros-network/blob/master/typed-protocols/src/Network/TypedProtocol/Core.hs
///
/// The following changes from the original Rust library:
/// * Send dynamic types in the channel that can be downcast into specific types the protocol expects,
///   so that we can handle mismatches gracefully, without segfaults, assuming that they are coming over the network
///   and cannot be trusted to be correct. The compiler is there to help get the protocol right for honest participants,
///   but in itself cannot guarantee what the right content will arrive.
/// * Do away without having to send `true` or `false` to indicate choice in `Choose` and `Offer`.
///   That means `Choose<Send, Recv>` will no longer be possibl, because the other side wouldn't know what we decided;
///   there has to be an explicit message communicating the choice, and transfering the agency to the opposite peer.
/// * Returning `Result` type so errors can be inspected. An error closes the channel to be closed.
use std::{
    any::Any,
    error::Error,
    marker,
    sync::mpsc::{RecvError, RecvTimeoutError},
};

// More like the original session_types library, encoding the protocol as nested
// static types of receives and sends. Thus it doesn't require explicit agency,
// since it shows exactly who is going to send the message.
pub mod types;

// More like the Cardano typed protocols, where messages are transitions between
// state machines where either the client or the server has agency.
pub mod states;

type DynMessage = Box<dyn Any + marker::Send + 'static>;

#[derive(Debug)]
pub enum SessionError {
    /// Wrong message type was sent.
    UnexpectedMessage(DynMessage),
    /// The other end of the channel is closed.
    Disconnected,
    /// Did not receive a message within the timeout.
    Timeout,
}

impl From<RecvError> for SessionError {
    fn from(_: RecvError) -> Self {
        SessionError::Disconnected
    }
}

impl From<RecvTimeoutError> for SessionError {
    fn from(e: RecvTimeoutError) -> Self {
        match e {
            RecvTimeoutError::Disconnected => SessionError::Disconnected,
            RecvTimeoutError::Timeout => SessionError::Timeout,
        }
    }
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for SessionError {}

pub type SessionResult<T> = Result<T, SessionError>;

pub fn ok<T>(value: T) -> SessionResult<T> {
    Ok(value)
}

fn downcast<T: 'static>(msg: DynMessage) -> SessionResult<T> {
    match msg.downcast::<T>() {
        Ok(data) => Ok(*data),
        Err(invalid) => Err(SessionError::UnexpectedMessage(invalid)),
    }
}

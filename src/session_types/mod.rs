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
    marker::PhantomData,
    mem::ManuallyDrop,
    sync::mpsc::{self, Receiver, RecvError, RecvTimeoutError, Sender},
    thread,
    time::Duration,
};

type DynMessage = Box<dyn Any + marker::Send + 'static>;

#[derive(Debug)]
pub enum SessionError {
    /// Wrong message type was sent.
    UnexpectedMessage(DynMessage),
    /// The other end of the channel is closed.
    Disconnected,
    /// Did not receive a message within the timeout.
    Timeout,
    /// Abort due to the a violation of some protocol constraints.
    Abort(Box<dyn Error + marker::Send + 'static>),
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

pub use Branch::*;

/// A session typed channel. `P` is the protocol and `E` is the environment,
/// containing potential recursion targets.
pub struct Chan<P, E> {
    tx: ManuallyDrop<Sender<DynMessage>>,
    rx: ManuallyDrop<Receiver<DynMessage>>,
    stash: ManuallyDrop<Option<DynMessage>>,
    _phantom: PhantomData<(P, E)>,
}

impl<P, E> Chan<P, E> {
    fn new(tx: Sender<DynMessage>, rx: Receiver<DynMessage>) -> Chan<P, E> {
        Chan {
            tx: ManuallyDrop::new(tx),
            rx: ManuallyDrop::new(rx),
            stash: ManuallyDrop::new(None),
            _phantom: PhantomData,
        }
    }
}

fn write_chan<T: marker::Send + 'static, P, E>(chan: &Chan<P, E>, v: T) -> SessionResult<()> {
    chan.tx
        .send(Box::new(v))
        .map_err(|_| SessionError::Disconnected)
}

fn read_chan<T: marker::Send + 'static, P, E>(
    chan: &mut Chan<P, E>,
    timeout: Duration,
) -> SessionResult<T> {
    let msg = read_chan_dyn(chan, timeout)?;
    downcast(msg)
}

fn read_chan_dyn<P, E>(chan: &mut Chan<P, E>, timeout: Duration) -> SessionResult<DynMessage> {
    match chan.stash.take() {
        Some(msg) => Ok(msg),
        None => Ok(chan.rx.recv_timeout(timeout)?),
    }
}

fn close_chan<P, E>(chan: Chan<P, E>) {
    // This method cleans up the channel without running the panicky destructor
    // In essence, it calls the drop glue bypassing the `Drop::drop` method.
    let mut this = ManuallyDrop::new(chan);
    unsafe {
        ManuallyDrop::drop(&mut this.tx);
        ManuallyDrop::drop(&mut this.rx);
        ManuallyDrop::drop(&mut this.stash);
    }
}

/// Peano numbers: Zero
pub struct Z;

/// Peano numbers: Increment
pub struct S<N>(PhantomData<N>);

/// End of communication session (epsilon)
pub struct Eps;

/// Receive `T`, then resume with protocol `P`.
pub struct Recv<T, P>(PhantomData<(T, P)>);

/// Send `T`, then resume with protocol `P`.
pub struct Send<T, P>(PhantomData<(T, P)>);

/// Active choice between `P` and `Q`
pub struct Choose<P: Outgoing, Q: Outgoing>(PhantomData<(P, Q)>);

/// Passive choice (offer) between `P` and `Q`
pub struct Offer<P: Incoming, Q: Incoming>(PhantomData<(P, Q)>);

/// Enter a recursive environment.
pub struct Rec<P>(PhantomData<P>);

/// Recurse. N indicates how many layers of the recursive environment we recurse out of.
pub struct Var<N>(PhantomData<N>);

/// Indicate that a protocol will receive a message..
pub trait Incoming {}

impl<T, P> Incoming for Recv<T, P> {}
impl<P: Incoming, Q: Incoming> Incoming for Offer<P, Q> {}

/// Indicate that a protocol will send a message.
pub trait Outgoing {}

impl<T, P> Outgoing for Send<T, P> {}
impl<P: Outgoing, Q: Outgoing> Outgoing for Choose<P, Q> {}

/// The HasDual trait defines the dual relationship between protocols.
///
/// Any valid protocol has a corresponding dual.
pub trait HasDual {
    type Dual;
}

impl HasDual for Eps {
    type Dual = Eps;
}

impl<A, P: HasDual> HasDual for Send<A, P> {
    type Dual = Recv<A, P::Dual>;
}

impl<A, P: HasDual> HasDual for Recv<A, P> {
    type Dual = Send<A, P::Dual>;
}

impl<P: HasDual, Q: HasDual> HasDual for Choose<P, Q>
where
    P: Outgoing,
    Q: Outgoing,
    P::Dual: Incoming,
    Q::Dual: Incoming,
{
    type Dual = Offer<P::Dual, Q::Dual>;
}

impl<P: HasDual, Q: HasDual> HasDual for Offer<P, Q>
where
    P: Incoming,
    Q: Incoming,
    P::Dual: Outgoing,
    Q::Dual: Outgoing,
{
    type Dual = Choose<P::Dual, Q::Dual>;
}

impl HasDual for Var<Z> {
    type Dual = Var<Z>;
}

impl<N> HasDual for Var<S<N>> {
    type Dual = Var<S<N>>;
}

impl<P: HasDual> HasDual for Rec<P> {
    type Dual = Rec<P::Dual>;
}

pub enum Branch<L, R> {
    Left(L),
    Right(R),
}

/// A sanity check destructor that kicks in if we abandon the channel by
/// returning `Ok(_)` without closing it first.
impl<P, E> Drop for Chan<P, E> {
    fn drop(&mut self) {
        if !thread::panicking() {
            panic!("Session channel prematurely dropped. Must call `.close()`.");
        }
    }
}

impl<E> Chan<Eps, E> {
    /// Close a channel. Should always be used at the end of your program.
    pub fn close(self) -> SessionResult<()> {
        close_chan(self);
        Ok(())
    }
}

impl<P, E> Chan<P, E> {
    fn cast<P2, E2>(self) -> Chan<P2, E2> {
        let mut this = ManuallyDrop::new(self);
        unsafe {
            Chan {
                tx: ManuallyDrop::new(ManuallyDrop::take(&mut this.tx)),
                rx: ManuallyDrop::new(ManuallyDrop::take(&mut this.rx)),
                stash: ManuallyDrop::new(ManuallyDrop::take(&mut this.stash)),
                _phantom: PhantomData,
            }
        }
    }

    /// Close the channel and return an error due to some business logic violation.
    pub fn abort<T, F: Error + marker::Send + 'static>(self, e: F) -> SessionResult<T> {
        close_chan(self);
        Err(SessionError::Abort(Box::new(e)))
    }
}

impl<P, E, T: marker::Send + 'static> Chan<Send<T, P>, E> {
    /// Send a value of type `T` over the channel. Returns a channel with protocol `P`.
    pub fn send(self, v: T) -> SessionResult<Chan<P, E>> {
        match write_chan(&self, v) {
            Ok(()) => Ok(self.cast()),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<P, E, T: marker::Send + 'static> Chan<Recv<T, P>, E> {
    /// Receives a value of type `T` from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    pub fn recv(mut self, timeout: Duration) -> SessionResult<(Chan<P, E>, T)> {
        match read_chan(&mut self, timeout) {
            Ok(v) => Ok((self.cast(), v)),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<P: Outgoing, Q: Outgoing, E> Chan<Choose<P, Q>, E> {
    /// Perform an active choice, selecting protocol `P`.
    /// We haven't sent any value yet, so the agency stays on our side.
    pub fn sel1(self) -> Chan<P, E> {
        self.cast()
    }

    /// Perform an active choice, selecting protocol `Q`.
    /// We haven't sent any value yet, so the agency stays on our side.
    pub fn sel2(self) -> Chan<Q, E> {
        self.cast()
    }
}

impl<T: 'static, P, Q: Incoming, E> Chan<Offer<Recv<T, P>, Q>, E> {
    /// Put the value we pulled from the channel back,
    /// so the next protocol step can read it and use it.
    fn stash(mut self, msg: DynMessage) -> Self {
        self.stash = ManuallyDrop::new(Some(msg));
        self
    }

    /// Passive choice. This allows the other end of the channel to select one
    /// of two options for continuing the protocol: either `P` or `Q`.
    /// Both options mean they will have to send a message to us,
    /// the agency is on their side.
    pub fn offer(mut self, t: Duration) -> SessionResult<Branch<Chan<Recv<T, P>, E>, Chan<Q, E>>> {
        // The next message we read from the channel decides
        // which protocol we go with.
        let msg = read_chan_dyn(&mut self, t)?;

        if msg.downcast_ref::<T>().is_some() {
            Ok(Left(self.stash(msg).cast()))
        } else {
            Ok(Right(self.stash(msg).cast()))
        }
    }
}

impl<P, E> Chan<Rec<P>, E> {
    /// Enter a recursive environment, putting the current environment on the
    /// top of the environment stack.
    pub fn enter(self) -> Chan<P, (P, E)> {
        self.cast()
    }
}

impl<P, E> Chan<Var<Z>, (P, E)> {
    /// Recurse to the environment on the top of the environment stack.
    /// The agency must be kept, since there's no message exchange here,
    /// we just start from the top as a continuation of where we are.
    pub fn zero(self) -> Chan<P, (P, E)> {
        self.cast()
    }
}

impl<P, E, N> Chan<Var<S<N>>, (P, E)> {
    /// Pop the top environment from the environment stack.
    pub fn succ(self) -> Chan<Var<N>, E> {
        self.cast()
    }
}

pub fn session_channel<P: HasDual>() -> (Chan<P, ()>, Chan<P::Dual, ()>) {
    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    let c1 = Chan::new(tx1, rx2);
    let c2 = Chan::new(tx2, rx1);

    (c1, c2)
}

/// This macro is convenient for server-like protocols of the form:
///
/// `Offer<A, Offer<B, Offer<C, ... >>>`
///
/// # Examples
///
/// Assume we have a protocol `Offer<Recv<u64, Eps>, Offer<Recv<String, Eps>,Eps>>>`
/// we can use the `offer!` macro as follows:
///
/// ```rust
/// use paxos::offer;
/// use paxos::session_types::*;
/// use std::thread::spawn;
/// use std::time::Duration;
///
/// struct Bye;
///
/// fn srv(c: Chan<Offer<Recv<u64, Eps>, Offer<Recv<String, Eps>, Recv<Bye, Eps>>>, ()>) -> SessionResult<()>{
///     let t = Duration::from_secs(1);
///     offer! { c, t,
///         Number => {
///             let (c, n) = c.recv(t)?;
///             assert_eq!(42, n);
///             c.close()
///         },
///         String => {
///             c.recv(t)?.0.close()
///         },
///         Quit => {
///             c.recv(t)?.0.close()
///         }
///     }
/// }
///
/// fn cli(c: Chan<Choose<Send<u64, Eps>, Choose<Send<String, Eps>, Send<Bye, Eps>>>, ()>) -> SessionResult<()>{
///     c.sel1().send(42)?.close()
/// }
///
/// fn main() {
///     let (s, c) = session_channel();
///     spawn(move|| cli(c).unwrap());
///     srv(s).unwrap();
/// }
/// ```
///
/// The identifiers on the left-hand side of the arrows have no semantic
/// meaning, they only provide a meaningful name for the reader.
#[macro_export]
macro_rules! offer {
    (
        $id:ident, $timeout:expr, $branch:ident => $code:expr, $($t:tt)+
    ) => (
        match $id.offer($timeout)? {
            $crate::session_types::Left($id) => $code,
            $crate::session_types::Right($id) => offer!{ $id, $timeout, $($t)+ }
        }
    );
    (
        $id:ident, $timeout:expr, $branch:ident => $code:expr
    ) => (
        $code
    )
}

#[cfg(test)]
mod test;

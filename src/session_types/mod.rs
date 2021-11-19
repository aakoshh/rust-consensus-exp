// Inspired by https://github.com/Munksgaard/session-types and
// https://github.com/input-output-hk/ouroboros-network/blob/master/typed-protocols/src/Network/TypedProtocol/Core.hs
// with the following goals:
// * Express the messages as an ADT, so every protocol has a single overall message type enum.
// * Send dynamic types in the channel that can be downcast into specific types the protocol expects,
//   so that we can handle mismatches gracefully, without segfaults, assuming they are coming over the network.
// * Do away without having to send `true` or `false` to indicate choice in `Choose` and `Offer`.
//   Instead, the channel state will have an `Agency` associated with it that expresses who can send the next message.
//   That means `Choose<Send, Recv>` will no longer be possible.

use std::{
    any::Any,
    error::Error,
    marker,
    marker::PhantomData,
    mem::ManuallyDrop,
    sync::mpsc::{self, Receiver, RecvError, RecvTimeoutError, Sender},
};

use Branch::*;

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

/// Client role.
pub struct AsClient;

/// Server role.
pub struct AsServer;

/// Indicates who is in a position to send the next message.
///
/// `WeHaveAgency`   is `Agency<Send = R>`
/// `TheyHaveAgency` is `Agency<Recv = R>`
pub trait Agency {
    /// The role which currently has agency.
    type Send;
    /// The type of the opposite agency.
    type Recv;
    /// Keep agency means the right to send stays with the sender.
    type Keep = Self::Send;
    /// Flipping agency means the right to send goes over to the receiver.
    type Flip = Self::Recv;
}

/// Agency for protocol states where the client sends the next message.
impl Agency for AsClient {
    type Send = AsClient;
    type Recv = AsServer;
}

/// Role for protocol states where the server sends the next message.
impl Agency for AsServer {
    type Send = AsServer;
    type Recv = AsClient;
}
/// Marker trait for messages that keep the agency on the sender's side.
pub trait KeepAgency {}

/// Marker trait for messages that flip the agency to the receiver's side.
pub trait FlipAgency {}

/// A session typed channel. `P` is the protocol and `E` is the environment,
/// containing potential recursion targets.
///
/// `R` is the role of the owner of the channel and it stays constant during
/// the lifetime of the channel. It is used to decide whether this side has
/// agency.
///
/// `A` is the agency; the channel can only be written to when its agency
/// allows its role to do so. The agency also knows what the flip side is,
/// and the right send/read method must be called depending on whether
/// the message causes the agency to flip to the other side.
pub struct Chan<R, A, E, P> {
    tx: ManuallyDrop<Sender<DynMessage>>,
    rx: ManuallyDrop<Receiver<DynMessage>>,
    stash: ManuallyDrop<Option<DynMessage>>,
    _phantom: PhantomData<(R, A, E, P)>,
}

impl<R, A, E, P> Chan<R, A, E, P> {
    fn new(tx: Sender<DynMessage>, rx: Receiver<DynMessage>) -> Chan<R, A, E, P> {
        Chan {
            tx: ManuallyDrop::new(tx),
            rx: ManuallyDrop::new(rx),
            stash: ManuallyDrop::new(None),
            _phantom: PhantomData,
        }
    }
}

fn downcast<T: 'static>(msg: DynMessage) -> SessionResult<T> {
    match msg.downcast::<T>() {
        Ok(data) => Ok(*data),
        Err(invalid) => Err(SessionError::UnexpectedMessage(invalid)),
    }
}

fn write_chan<R, A: Agency<Send = R>, T: marker::Send + 'static, E, P>(
    chan: &Chan<R, A, E, P>,
    v: T,
) -> SessionResult<()> {
    chan.tx
        .send(Box::new(v))
        .map_err(|_| SessionError::Disconnected)
}

fn read_chan<R, A: Agency<Recv = R>, T: marker::Send + 'static, E, P>(
    chan: &mut Chan<R, A, E, P>,
) -> SessionResult<T> {
    let msg = read_chan_dyn(chan)?;
    downcast(msg)
}

fn read_chan_dyn<R, A: Agency<Recv = R>, E, P>(
    chan: &mut Chan<R, A, E, P>,
) -> SessionResult<DynMessage> {
    match chan.stash.take() {
        Some(msg) => Ok(msg),
        None => Ok(chan.rx.recv()?),
    }
}

fn close_chan<R, A, E, P>(chan: Chan<R, A, E, P>) {
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
pub struct Choose<P, Q>(PhantomData<(P, Q)>);

/// Passive choice (offer) between `P` and `Q`
pub struct Offer<P, Q>(PhantomData<(P, Q)>);

/// Enter a recursive environment.
pub struct Rec<P>(PhantomData<P>);

/// Recurse. N indicates how many layers of the recursive environment we recurse out of.
pub struct Var<N>(PhantomData<N>);

/// Indicate what type of message a protocol expects next.
pub trait ExpectsMessage {
    type Message;
}

impl<T, P> ExpectsMessage for Recv<T, P> {
    type Message = T;
}

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

impl<P: HasDual, Q: HasDual> HasDual for Choose<P, Q> {
    type Dual = Offer<P::Dual, Q::Dual>;
}

impl<P: HasDual, Q: HasDual> HasDual for Offer<P, Q> {
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

impl<R, A, E, P> Drop for Chan<R, A, E, P> {
    fn drop(&mut self) {
        panic!("Session channel prematurely dropped. Must call `.close()`.");
    }
}

impl<R, A, E> Chan<R, A, E, Eps> {
    /// Close a channel. Should always be used at the end of your program.
    pub fn close(self) -> SessionResult<()> {
        close_chan(self);
        Ok(())
    }
}

impl<R, A, E, P> Chan<R, A, E, P> {
    fn cast<A2, E2, P2>(self) -> Chan<R, A2, E2, P2> {
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
}

impl<R, A, E, P, T> Chan<R, A, E, Send<T, P>>
where
    A: Agency<Send = R>,
    T: marker::Send + 'static + KeepAgency,
{
    /// Send a value of type `T` over the channel. Returns a channel with protocol `P`.
    /// The next message will again have to be sent by this side.
    pub fn send_keep(self, v: T) -> SessionResult<Chan<R, A::Keep, E, P>> {
        match write_chan(&self, v) {
            Ok(()) => Ok(self.cast()),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<R, A, E, P, T> Chan<R, A, E, Send<T, P>>
where
    A: Agency<Send = R>,
    T: marker::Send + 'static + FlipAgency,
{
    /// Send a value of type `T` over the channel. Returns a channel with protocol `P`.
    /// The next message will have to be sent by the other side.
    pub fn send_flip(self, v: T) -> SessionResult<Chan<R, A::Flip, E, P>> {
        match write_chan(&self, v) {
            Ok(()) => Ok(self.cast()),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<R, A, E, P, T> Chan<R, A, E, Recv<T, P>>
where
    A: Agency<Recv = R>,
    T: marker::Send + 'static + KeepAgency,
{
    /// Receives a value of type `T` from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    /// The agency stays on the sender's side.
    pub fn recv_keep(mut self) -> SessionResult<(Chan<R, A::Keep, E, P>, T)> {
        match read_chan(&mut self) {
            Ok(v) => Ok((self.cast(), v)),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<R, A, E, P, T> Chan<R, A, E, Recv<T, P>>
where
    A: Agency<Recv = R>,
    T: marker::Send + 'static + FlipAgency,
{
    /// Receives a value of type `T` from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    /// The agency is transferred to the receiver's side.
    pub fn recv_flip(mut self) -> SessionResult<(Chan<R, A::Flip, E, P>, T)> {
        match read_chan(&mut self) {
            Ok(v) => Ok((self.cast(), v)),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<R, A, E, P, Q> Chan<R, A, E, Choose<P, Q>>
where
    A: Agency<Send = R>,
{
    /// Perform an active choice, selecting protocol `P`.
    /// We haven't sent any value yet, so the agency stays on our side.
    pub fn sel1(self) -> Chan<R, A, E, P> {
        self.cast()
    }

    /// Perform an active choice, selecting protocol `Q`.
    /// We haven't sent any value yet, so the agency stays on our side.
    pub fn sel2(self) -> Chan<R, A, E, Q> {
        self.cast()
    }
}

impl<R, A, E, P, Q> Chan<R, A, E, Offer<P, Q>>
where
    A: Agency<Recv = R>,
    P: ExpectsMessage,
    P::Message: 'static,
{
    /// Put the value we pulled from the channel back,
    /// so the next protocol step can read it and use it.
    fn stash(mut self, msg: DynMessage) -> Chan<R, A, E, Offer<P, Q>> {
        self.stash = ManuallyDrop::new(Some(msg));
        self
    }

    /// Passive choice. This allows the other end of the channel to select one
    /// of two options for continuing the protocol: either `P` or `Q`.
    /// Both options mean they will have to send a message to us,
    /// the agency is on their side.
    pub fn offer(mut self) -> SessionResult<Branch<Chan<R, A, E, P>, Chan<R, A, E, Q>>> {
        // The next message we read from the channel decides
        // which protocol we go with.
        let msg = read_chan_dyn(&mut self)?;

        if msg.downcast_ref::<P::Message>().is_some() {
            Ok(Left(self.stash(msg).cast()))
        } else {
            Ok(Right(self.stash(msg).cast()))
        }
    }
}

impl<R, A, E, P> Chan<R, A, E, Rec<P>> {
    /// Enter a recursive environment, putting the current environment on the
    /// top of the environment stack.
    pub fn enter(self) -> Chan<R, A, (P, E), P> {
        self.cast()
    }
}

impl<R, A, E, P> Chan<R, A, (P, E), Var<Z>> {
    /// Recurse to the environment on the top of the environment stack.
    /// The agency must be kept, since there's no message exchange here,
    /// we just start from the top as a continuation of where we are.
    pub fn zero(self) -> Chan<R, A, (P, E), P> {
        self.cast()
    }
}

impl<R, A, E, P, N> Chan<R, A, (P, E), Var<S<N>>> {
    /// Pop the top environment from the environment stack.
    pub fn succ(self) -> Chan<R, A, E, Var<N>> {
        self.cast()
    }
}

pub fn session_channel<A: Agency, P: HasDual>(
) -> (Chan<AsServer, A, (), P>, Chan<AsClient, A, (), P::Dual>) {
    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    let c1 = Chan::new(tx1, rx2);
    let c2 = Chan::new(tx2, rx1);

    (c1, c2)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;

    mod ping_pong {
        use super::*;
        pub struct Ping;
        pub struct Pong;

        impl FlipAgency for Ping {}
        impl FlipAgency for Pong {}

        pub type Server = Recv<Ping, Send<Pong, Eps>>;
        pub type Client = <Server as HasDual>::Dual;
    }

    #[test]
    fn ping_pong_basics() {
        use ping_pong::*;

        fn srv(c: Chan<AsServer, AsClient, (), Server>) -> SessionResult<()> {
            let (c, _ping) = c.recv_flip()?;
            c.send_flip(Pong)?.close()
        }

        fn cli(c: Chan<AsClient, AsClient, (), Client>) -> SessionResult<()> {
            let c = c.send_flip(Ping)?;
            let (c, _pong) = c.recv_flip()?;
            c.close()
        }

        let (server_chan, client_chan) = session_channel::<AsClient, Server>();

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(client_chan));

        let _ = (srv_t.join().unwrap(), cli_t.join().unwrap());
    }

    #[test]
    fn ping_pong_error() {
        use ping_pong::*;

        type WrongClient = Send<String, Recv<u64, Eps>>;

        impl FlipAgency for String {}
        impl FlipAgency for u64 {}

        fn srv(c: Chan<AsServer, AsClient, (), Server>) -> SessionResult<()> {
            let (c, _ping) = c.recv_flip()?;
            c.send_flip(Pong)?.close()
        }

        fn cli(c: Chan<AsClient, AsClient, (), WrongClient>) -> SessionResult<()> {
            let c = c.send_flip("Hello".into())?;
            let (c, _n) = c.recv_flip()?;
            c.close()
        }

        let (server_chan, client_chan) = session_channel::<AsClient, Server>();
        let wrong_client_chan = client_chan.cast::<AsClient, (), WrongClient>();

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(wrong_client_chan));

        let sr = srv_t.join().unwrap();
        let cr = cli_t.join().unwrap();

        assert!(sr.is_err());
        assert!(cr.is_err());
    }
}

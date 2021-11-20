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
///   Instead, the channel state will have an `Agency` associated with it that expresses who can send the next message.
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
    time::Duration,
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

/// Define where the agency is transferred after the message is sent/received.
pub trait Message: marker::Send + 'static {
    type Next: Agency;
}

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

fn write_chan<R, A: Agency<Send = R>, T: Message, E, P>(
    chan: &Chan<R, A, E, P>,
    v: T,
) -> SessionResult<()> {
    chan.tx
        .send(Box::new(v))
        .map_err(|_| SessionError::Disconnected)
}

fn read_chan<R, A: Agency<Recv = R>, T: Message, E, P>(
    chan: &mut Chan<R, A, E, P>,
    timeout: Duration,
) -> SessionResult<T> {
    let msg = read_chan_dyn(chan, timeout)?;
    downcast(msg)
}

fn read_chan_dyn<R, A: Agency<Recv = R>, E, P>(
    chan: &mut Chan<R, A, E, P>,
    timeout: Duration,
) -> SessionResult<DynMessage> {
    match chan.stash.take() {
        Some(msg) => Ok(msg),
        None => Ok(chan.rx.recv_timeout(timeout)?),
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
    type Message: Message;
}

impl<T: Message, P> ExpectsMessage for Recv<T, P> {
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

/// A sanity check destructor that kicks in if we abandon the channel by
/// returning `Ok(_)` without closing it first.
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
    T: Message,
{
    /// Send a value of type `T` over the channel. Returns a channel with protocol `P`.
    pub fn send(self, v: T) -> SessionResult<Chan<R, T::Next, E, P>> {
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
    T: Message,
{
    /// Receives a value of type `T` from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    pub fn recv(mut self, timeout: Duration) -> SessionResult<(Chan<R, T::Next, E, P>, T)> {
        match read_chan(&mut self, timeout) {
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
    pub fn offer(
        mut self,
        t: Duration,
    ) -> SessionResult<Branch<Chan<R, A, E, P>, Chan<R, A, E, Q>>> {
        // The next message we read from the channel decides
        // which protocol we go with.
        let msg = read_chan_dyn(&mut self, t)?;

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
/// extern crate session_types;
/// use session_types::*;
/// use std::thread::spawn;
///
/// fn srv(c: Chan<(), Offer<Recv<u64, Eps>, Offer<Recv<String, Eps>, Eps>>>) {
///     offer! { c,
///         Number => {
///             let (c, n) = c.recv();
///             assert_eq!(42, n);
///             c.close();
///         },
///         String => {
///             c.recv().0.close();
///         },
///         Quit => {
///             c.close();
///         }
///     }
/// }
///
/// fn cli(c: Chan<(), Choose<Send<u64, Eps>, Choose<Send<String, Eps>, Eps>>>) {
///     c.sel1().send(42).close();
/// }
///
/// fn main() {
///     let (s, c) = session_channel();
///     spawn(move|| cli(c));
///     srv(s);
/// }
/// ```
///
/// The identifiers on the left-hand side of the arrows have no semantic
/// meaning, they only provide a meaningful name for the reader.
#[macro_export]
macro_rules! offer {
    (
        $id:ident, $branch:ident => $code:expr, $($t:tt)+
    ) => (
        match $id.offer() {
            $crate::Left($id) => $code,
            $crate::Right($id) => offer!{ $id, $($t)+ }
        }
    );
    (
        $id:ident, $branch:ident => $code:expr
    ) => (
        $code
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{thread, time::Instant};

    mod ping_pong {
        use super::*;
        pub struct Ping;
        pub struct Pong;

        impl Message for Ping {
            type Next = AsServer;
        }
        impl Message for Pong {
            type Next = AsClient;
        }

        pub type Server = Recv<Ping, Send<Pong, Eps>>;
        pub type Client = <Server as HasDual>::Dual;
    }

    #[test]
    fn ping_pong_basics() {
        use ping_pong::*;
        let t = Duration::from_millis(100);

        let srv = move |c: Chan<AsServer, AsClient, (), Server>| {
            let (c, _ping) = c.recv(t)?;
            c.send(Pong)?.close()
        };

        let cli = move |c: Chan<AsClient, AsClient, (), Client>| {
            let c = c.send(Ping)?;
            let (c, _pong) = c.recv(t)?;
            c.close()
        };

        let (server_chan, client_chan) = session_channel::<AsClient, Server>();

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(client_chan));

        let _ = (srv_t.join().unwrap(), cli_t.join().unwrap());
    }

    #[test]
    fn ping_pong_error() {
        use ping_pong::*;
        let t = Duration::from_secs(10);

        type WrongClient = Send<String, Recv<u64, Eps>>;

        impl Message for String {
            type Next = AsServer;
        }
        impl Message for u64 {
            type Next = AsClient;
        }

        let srv = move |c: Chan<AsServer, AsClient, (), Server>| {
            let (c, _ping) = c.recv(t)?;
            c.send(Pong)?.close()
        };

        let cli = move |c: Chan<AsClient, AsClient, (), WrongClient>| {
            let c = c.send("Hello".into())?;
            let (c, _n) = c.recv(t)?;
            c.close()
        };

        let (server_chan, client_chan) = session_channel::<AsClient, Server>();
        let wrong_client_chan = client_chan.cast::<AsClient, (), WrongClient>();

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(wrong_client_chan));

        let sr = srv_t.join().unwrap();
        let cr = cli_t.join().unwrap();

        assert!(sr.is_err());
        assert!(cr.is_err());
    }

    #[test]
    fn greetings() {
        struct Hail(String);
        struct Greetings(String);
        struct TimeRequest;
        struct TimeResponse(Instant);
        struct AddRequest1(u32);
        struct AddRequest2(u32);
        struct AddResponse(u32);
        struct Quit;

        type TimeProtocol = Recv<TimeRequest, Send<TimeResponse, Var<Z>>>;
        type AddProtocol = Recv<AddRequest1, Recv<AddRequest2, Send<AddResponse, Var<Z>>>>;
        type QuitProtocol = Recv<Quit, Eps>;

        type Server =
            Recv<Hail, Send<Greetings, Rec<Offer<TimeProtocol, Offer<AddProtocol, QuitProtocol>>>>>;

        type Client = <Server as HasDual>::Dual;
    }
}

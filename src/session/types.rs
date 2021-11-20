/// Inspired by https://github.com/Munksgaard/session-types
/// See the parent module for more description.
use std::{
    marker,
    marker::PhantomData,
    mem::ManuallyDrop,
    sync::mpsc::{self, Receiver, Sender},
    time::Duration,
};

pub use Branch::*;

use super::{downcast, DynMessage};
pub use super::{ok, SessionError, SessionResult};

/// A session typed channel. `P` is the protocol and `E` is the environment,
/// containing potential recursion targets.
pub struct Chan<E, P> {
    tx: ManuallyDrop<Sender<DynMessage>>,
    rx: ManuallyDrop<Receiver<DynMessage>>,
    stash: ManuallyDrop<Option<DynMessage>>,
    _phantom: PhantomData<(E, P)>,
}

impl<E, P> Chan<E, P> {
    fn new(tx: Sender<DynMessage>, rx: Receiver<DynMessage>) -> Chan<E, P> {
        Chan {
            tx: ManuallyDrop::new(tx),
            rx: ManuallyDrop::new(rx),
            stash: ManuallyDrop::new(None),
            _phantom: PhantomData,
        }
    }
}

fn write_chan<T: marker::Send + 'static, E, P>(chan: &Chan<E, P>, v: T) -> SessionResult<()> {
    chan.tx
        .send(Box::new(v))
        .map_err(|_| SessionError::Disconnected)
}

fn read_chan<T: marker::Send + 'static, E, P>(
    chan: &mut Chan<E, P>,
    timeout: Duration,
) -> SessionResult<T> {
    let msg = read_chan_dyn(chan, timeout)?;
    downcast(msg)
}

fn read_chan_dyn<E, P>(chan: &mut Chan<E, P>, timeout: Duration) -> SessionResult<DynMessage> {
    match chan.stash.take() {
        Some(msg) => Ok(msg),
        None => Ok(chan.rx.recv_timeout(timeout)?),
    }
}

fn close_chan<E, P>(chan: Chan<E, P>) {
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
impl<E, P> Drop for Chan<E, P> {
    fn drop(&mut self) {
        panic!("Session channel prematurely dropped. Must call `.close()`.");
    }
}

impl<E> Chan<E, Eps> {
    /// Close a channel. Should always be used at the end of your program.
    pub fn close(self) -> SessionResult<()> {
        close_chan(self);
        Ok(())
    }
}

impl<E, P> Chan<E, P> {
    fn cast<E2, P2>(self) -> Chan<E2, P2> {
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

impl<E, P, T: marker::Send + 'static> Chan<E, Send<T, P>> {
    /// Send a value of type `T` over the channel. Returns a channel with protocol `P`.
    pub fn send(self, v: T) -> SessionResult<Chan<E, P>> {
        match write_chan(&self, v) {
            Ok(()) => Ok(self.cast()),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<E, P, T: marker::Send + 'static> Chan<E, Recv<T, P>> {
    /// Receives a value of type `T` from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    pub fn recv(mut self, timeout: Duration) -> SessionResult<(Chan<E, P>, T)> {
        match read_chan(&mut self, timeout) {
            Ok(v) => Ok((self.cast(), v)),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<E, P: Outgoing, Q: Outgoing> Chan<E, Choose<P, Q>> {
    /// Perform an active choice, selecting protocol `P`.
    /// We haven't sent any value yet, so the agency stays on our side.
    pub fn sel1(self) -> Chan<E, P> {
        self.cast()
    }

    /// Perform an active choice, selecting protocol `Q`.
    /// We haven't sent any value yet, so the agency stays on our side.
    pub fn sel2(self) -> Chan<E, Q> {
        self.cast()
    }
}

impl<T: 'static, E, P, Q: Incoming> Chan<E, Offer<Recv<T, P>, Q>> {
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
    pub fn offer(mut self, t: Duration) -> SessionResult<Branch<Chan<E, Recv<T, P>>, Chan<E, Q>>> {
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

impl<E, P> Chan<E, Rec<P>> {
    /// Enter a recursive environment, putting the current environment on the
    /// top of the environment stack.
    pub fn enter(self) -> Chan<(P, E), P> {
        self.cast()
    }
}

impl<E, P> Chan<(P, E), Var<Z>> {
    /// Recurse to the environment on the top of the environment stack.
    /// The agency must be kept, since there's no message exchange here,
    /// we just start from the top as a continuation of where we are.
    pub fn zero(self) -> Chan<(P, E), P> {
        self.cast()
    }
}

impl<E, P, N> Chan<(P, E), Var<S<N>>> {
    /// Pop the top environment from the environment stack.
    pub fn succ(self) -> Chan<E, Var<N>> {
        self.cast()
    }
}

pub fn session_channel<P: HasDual>() -> (Chan<(), P>, Chan<(), P::Dual>) {
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
/// use paxos::session::types::*;
/// use std::thread::spawn;
/// use std::time::Duration;
///
/// fn srv(c: Chan<(), Offer<Recv<u64, Eps>, Offer<Recv<String, Eps>, Eps>>>) -> SessionResult<()>{
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
///             c.close()
///         }
///     }
/// }
///
/// fn cli(c: Chan<(), Choose<Send<u64, Eps>, Choose<Send<String, Eps>, Eps>>>) -> SessionResult<()>{
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
        $id:ident, $timeout:ident, $branch:ident => $code:expr, $($t:tt)+
    ) => (
        match $id.offer($timeout)? {
            $crate::session::types::Left($id) => $code,
            $crate::session::types::Right($id) => offer!{ $id, $timeout, $($t)+ }
        }
    );
    (
        $id:ident, $timeout:ident, $branch:ident => $code:expr
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

        pub type Server = Recv<Ping, Send<Pong, Eps>>;
        pub type Client = <Server as HasDual>::Dual;
    }

    #[test]
    fn ping_pong_basics() {
        use ping_pong::*;
        let t = Duration::from_millis(100);

        let srv = move |c: Chan<(), Server>| {
            let (c, _ping) = c.recv(t)?;
            c.send(Pong)?.close()
        };

        let cli = move |c: Chan<(), Client>| {
            let c = c.send(Ping)?;
            let (c, _pong) = c.recv(t)?;
            c.close()
        };

        let (server_chan, client_chan) = session_channel();

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(client_chan));

        let _ = (srv_t.join().unwrap(), cli_t.join().unwrap());
    }

    #[test]
    fn ping_pong_error() {
        use ping_pong::*;
        let t = Duration::from_secs(10);

        type WrongClient = Send<String, Recv<u64, Eps>>;

        let srv = move |c: Chan<(), Server>| {
            let (c, _ping) = c.recv(t)?;
            c.send(Pong)?.close()
        };

        let cli = move |c: Chan<(), WrongClient>| {
            let c = c.send("Hello".into())?;
            let (c, _n) = c.recv(t)?;
            c.close()
        };

        let (server_chan, client_chan) = session_channel();
        let wrong_client_chan = client_chan.cast::<(), WrongClient>();

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
        struct AddRequest(u32);
        struct AddResponse(u32);
        struct Quit;

        type TimeProtocol = Recv<TimeRequest, Send<TimeResponse, Var<Z>>>;
        type AddProtocol = Recv<AddRequest, Recv<AddRequest, Send<AddResponse, Var<Z>>>>;
        type QuitProtocol = Recv<Quit, Eps>;

        type ProtocolChoices = Offer<TimeProtocol, Offer<AddProtocol, QuitProtocol>>;

        type Server = Recv<Hail, Send<Greetings, Rec<ProtocolChoices>>>;
        type Client = <Server as HasDual>::Dual;

        // It is at this point that an invalid protocol would fail to compile.
        let (server_chan, client_chan) = session_channel::<Server>();

        let srv = |c: Chan<(), Server>| {
            let t = Duration::from_millis(100);
            let (c, Hail(cid)) = c.recv(t)?;
            let c = c.send(Greetings(format!("Hello {}!", cid)))?;
            let mut c = c.enter();
            loop {
                c = offer! { c, t,
                    Time => {
                        let (c, TimeRequest) = c.recv(t)?;
                        let c = c.send(TimeResponse(Instant::now()))?;
                        c.zero()
                    },
                    Add => {
                            let (c, AddRequest(a)) = c.recv(t)?;
                            let (c, AddRequest(b)) = c.recv(t)?;
                            let c = c.send(AddResponse(a + b))?;
                            c.zero()
                    },
                    Quit => {
                        let (c, Quit) = c.recv(t)?;
                        c.close()?;
                        break;
                    }
                };
            }

            ok(())
        };

        let cli = |c: Chan<(), Client>| {
            let t = Duration::from_millis(100);
            let c = c.send(Hail("Rusty".into()))?;
            let (c, Greetings(_)) = c.recv(t)?;
            let c = c.enter();
            let (c, AddResponse(r)) = c
                .sel2()
                .sel1()
                .send(AddRequest(1))?
                .send(AddRequest(2))?
                .recv(t)?;

            c.zero().sel2().sel2().send(Quit)?.close()?;

            ok(r)
        };

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(client_chan));

        let sr = srv_t.join().unwrap();
        let cr = cli_t.join().unwrap();

        assert!(sr.is_ok());
        assert_eq!(cr.unwrap(), 3);
    }
}

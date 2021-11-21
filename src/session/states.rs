/// Inspired by:
/// * https://github.com/input-output-hk/ouroboros-network/#ouroboros-network-documentation
/// * https://github.com/input-output-hk/ouroboros-network/blob/master/typed-protocols/src/Network/TypedProtocol/Core.hs
///
/// Unfortunately this experiment failed. So far I haven't found a way to ensure statically
/// that all possible transitions in a state are handled.
use std::{
    marker::PhantomData,
    mem::ManuallyDrop,
    sync::mpsc::{self, Receiver, Sender},
    time::Duration,
};

use super::{downcast, DynMessage, SessionError, SessionResult};

pub trait Agency {
    type Flip: Agency;
}

pub trait Role: Agency {}

/// Client role.
pub struct AsClient;

/// Server role.
pub struct AsServer;

/// Nobody has agency, indicating that the session is over.
pub struct Nobody;

impl Agency for AsClient {
    type Flip = AsServer;
}

impl Agency for AsServer {
    type Flip = AsClient;
}

impl Agency for Nobody {
    type Flip = Nobody;
}

impl Role for AsClient {}
impl Role for AsServer {}

/// Indicate which party can send the next message.
pub trait State {
    type Agency: Agency;
}

/// NOTE: In Cardano a message can appear between multiple states,
/// i.e. be part of multiple (from, message, to) transition triplets.
/// The Rust compiler doesn't seem able to infer a transition just
/// on the message type, so for now make each message unique between
/// two states.
pub trait Message: Send + 'static {
    type From: State;
    type To: State;
}

/// A session typed channel. `S` is the state, which determines the agency.
/// `R` is the role of the channel owner.
pub struct Chan<R, S> {
    tx: ManuallyDrop<Sender<DynMessage>>,
    rx: ManuallyDrop<Receiver<DynMessage>>,
    stash: ManuallyDrop<Option<DynMessage>>,
    _phantom: PhantomData<(R, S)>,
}

impl<R, S> Chan<R, S> {
    fn new(tx: Sender<DynMessage>, rx: Receiver<DynMessage>) -> Chan<R, S> {
        Chan {
            tx: ManuallyDrop::new(tx),
            rx: ManuallyDrop::new(rx),
            stash: ManuallyDrop::new(None),
            _phantom: PhantomData,
        }
    }
}

fn write_chan<R, S, T: Send + 'static>(chan: &Chan<R, S>, v: T) -> SessionResult<()> {
    chan.tx
        .send(Box::new(v))
        .map_err(|_| SessionError::Disconnected)
}

fn read_chan<R, S, T: Send + 'static>(
    chan: &mut Chan<R, S>,
    timeout: Duration,
) -> SessionResult<T> {
    let msg = read_chan_dyn(chan, timeout)?;
    downcast(msg)
}

fn read_chan_dyn<R, S>(chan: &mut Chan<R, S>, timeout: Duration) -> SessionResult<DynMessage> {
    match chan.stash.take() {
        Some(msg) => Ok(msg),
        None => Ok(chan.rx.recv_timeout(timeout)?),
    }
}

fn close_chan<R, S>(chan: Chan<R, S>) {
    // This method cleans up the channel without running the panicky destructor
    // In essence, it calls the drop glue bypassing the `Drop::drop` method.
    let mut this = ManuallyDrop::new(chan);
    unsafe {
        ManuallyDrop::drop(&mut this.tx);
        ManuallyDrop::drop(&mut this.rx);
        ManuallyDrop::drop(&mut this.stash);
    }
}

/// A sanity check destructor that kicks in if we abandon the channel by
/// returning `Ok(_)` without closing it first.
impl<R, S> Drop for Chan<R, S> {
    fn drop(&mut self) {
        panic!("Session channel prematurely dropped. Must call `.close()`.");
    }
}

impl<R, S> Chan<R, S>
where
    S: State<Agency = Nobody>,
{
    /// Close a channel. Should always be used at the end of your program.
    pub fn close(self) -> SessionResult<()> {
        close_chan(self);
        Ok(())
    }
}

impl<R, S> Chan<R, S> {
    fn cast<S2>(self) -> Chan<R, S2> {
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

impl<R, S> Chan<R, S>
where
    R: Role,
    S: State<Agency = R>,
{
    /// Send a message over the channel, which transitions the system into a new state.
    pub fn send<T>(self, v: T) -> SessionResult<Chan<R, T::To>>
    where
        T: Message<From = S>,
    {
        match write_chan(&self, v) {
            Ok(()) => Ok(self.cast()),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

impl<R, S> Chan<R, S>
where
    R: Role,
    S: State<Agency = R::Flip>,
{
    /// Receives a value of type from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    pub fn recv<T>(mut self, timeout: Duration) -> SessionResult<(Chan<R, T::To>, T)>
    where
        T: Message<From = S>,
    {
        match read_chan(&mut self, timeout) {
            Ok(v) => Ok((self.cast(), v)),
            Err(e) => {
                close_chan(self);
                Err(e)
            }
        }
    }
}

pub fn session_channel<S>() -> (Chan<AsServer, S>, Chan<AsClient, S>) {
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
        pub struct Done;

        pub struct StIdle;
        pub struct StBusy;
        pub struct StDone;

        impl State for StIdle {
            type Agency = AsClient;
        }

        impl State for StBusy {
            type Agency = AsServer;
        }

        impl State for StDone {
            type Agency = Nobody;
        }

        impl Message for Ping {
            type From = StIdle;
            type To = StBusy;
        }

        impl Message for Pong {
            type From = StBusy;
            type To = StIdle;
        }

        impl Message for Done {
            type From = StIdle;
            type To = StDone;
        }
    }

    #[test]
    fn ping_pong_basics() {
        use ping_pong::*;
        let t = Duration::from_millis(100);

        let srv = move |c: Chan<AsServer, StIdle>| {
            let (c, Ping) = c.recv::<Ping>(t)?;
            let c = c.send(Pong)?;
            // TODO: Loop and Offer.
            // XXX: The compiler doesn't say that receiving a Ping is an option.
            let (c, Done) = c.recv::<Done>(t)?;
            c.close()
        };

        let cli = move |c: Chan<AsClient, StIdle>| {
            let c = c.send(Ping)?;
            let (c, Pong) = c.recv::<Pong>(t)?;
            c.send(Done)?.close()
        };

        let (server_chan, client_chan) = session_channel();

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(client_chan));

        srv_t.join().unwrap().unwrap();
        cli_t.join().unwrap().unwrap();
    }

    #[test]
    fn ping_pong_error() {
        use ping_pong::*;
        let t = Duration::from_millis(100);

        let srv = move |c: Chan<AsServer, StIdle>| {
            let (c, Ping) = c.recv::<Ping>(t)?;
            let c = c.send(Pong)?;
            // XXX: Going to receive a Ping instead.
            let (c, Done) = c.recv::<Done>(t)?;
            c.close()
        };

        let cli = move |c: Chan<AsClient, StIdle>| {
            let c = c.send(Ping)?;
            let (c, Pong) = c.recv::<Pong>(t)?;
            let c = c.send(Ping)?;
            let (c, Pong) = c.recv::<Pong>(t)?;
            c.send(Done)?.close()
        };

        let (server_chan, client_chan) = session_channel();

        let srv_t = thread::spawn(move || srv(server_chan));
        let cli_t = thread::spawn(move || cli(client_chan));

        assert!(srv_t.join().unwrap().is_err());
        assert!(cli_t.join().unwrap().is_err());
    }
}

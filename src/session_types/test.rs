
use super::*;
use std::{thread, time::Instant};

mod ping_pong {
    use super::*;
    pub struct Ping;
    pub struct Pong;

    // NOTE: This verison doesn't include looping.
    pub type Server = Recv<Ping, Send<Pong, Eps>>;
    pub type Client = <Server as HasDual>::Dual;
}

#[test]
fn ping_pong_basics() {
    use ping_pong::*;
    let t = Duration::from_millis(100);

    let srv = move |c: Chan<Server, ()>| {
        let (c, _ping) = c.recv(t)?;
        c.send(Pong)?.close()
    };

    let cli = move |c: Chan<Client, ()>| {
        let c = c.send(Ping)?;
        let (c, _pong) = c.recv(t)?;
        c.close()
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
    let t = Duration::from_secs(10);

    type WrongClient = Send<String, Recv<u64, Eps>>;

    let srv = move |c: Chan<Server, ()>| {
        let (c, _ping) = c.recv(t)?;
        c.send(Pong)?.close()
    };

    let cli = move |c: Chan<WrongClient, ()>| {
        let c = c.send("Hello".into())?;
        let (c, _n) = c.recv(t)?;
        c.close()
    };

    let (server_chan, client_chan) = session_channel();
    let wrong_client_chan = client_chan.cast::<WrongClient, ()>();

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

    let srv = |c: Chan<Server, ()>| {
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

    let cli = |c: Chan<Client, ()>| {
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

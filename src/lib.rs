#![allow(dead_code)]
#![feature(associated_type_defaults)]
#![feature(option_zip)]

use std::cmp::Ordering;
use std::rc::Rc;
use std::{collections::HashSet, hash::Hash};

mod first;
mod stm;

trait Paxos {
    type Pid: Copy + PartialEq + Eq + Hash + PartialOrd + std::fmt::Debug;
    type Value: Clone + std::fmt::Debug;
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
struct BallotOrdinal<P> {
    pid: P,
    round: u32,
}
impl<P> BallotOrdinal<P> {
    fn incr(&self, pid: P) -> BallotOrdinal<P> {
        BallotOrdinal {
            pid,
            round: self.round + 1,
        }
    }
}
impl<P: PartialOrd> PartialOrd for BallotOrdinal<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.round.partial_cmp(&other.round) {
            Some(Ordering::Equal) => self.pid.partial_cmp(&other.pid),
            ord => ord,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
struct InstanceId(u64);

#[derive(Clone, Debug)]
struct Vote<P: Paxos> {
    value: Rc<P::Value>,
    ord: BallotOrdinal<P::Pid>,
}

#[derive(Clone)]
struct PaxosInstance<P: Paxos> {
    id: InstanceId,
    my_pid: P::Pid,
    members: HashSet<P::Pid>,
    max_ballot_ordinal: BallotOrdinal<P::Pid>,
    vote: Option<Vote<P>>,
    requested_value: Option<Rc<P::Value>>,
    safe_value_vote: Option<Vote<P>>,
    promises: HashSet<P::Pid>,
    accepting_vote: Option<Vote<P>>,
    accepts: HashSet<P::Pid>,
    decided: bool,
}

impl<P: Paxos> PaxosInstance<P>
where
    P::Pid: Default,
{
    fn new(id: InstanceId, my_pid: P::Pid, members: HashSet<P::Pid>) -> PaxosInstance<P> {
        PaxosInstance {
            id,
            my_pid,
            members,
            max_ballot_ordinal: BallotOrdinal {
                pid: P::Pid::default(),
                round: 0,
            },
            vote: None,
            requested_value: None,
            safe_value_vote: None,
            promises: HashSet::new(),
            accepting_vote: None,
            accepts: HashSet::new(),
            decided: false,
        }
    }
}

#[derive(Clone, Debug)]
struct PaxosMessage<P: Paxos> {
    src: P::Pid,
    instance_id: InstanceId,
    members: HashSet<P::Pid>,
    ballot_ordinal: BallotOrdinal<P::Pid>,
    detail: PaxosMessageDetail<P>,
}

#[derive(Clone, Debug)]
enum PaxosMessageDetail<P: Paxos> {
    Prepare,
    Promise(Option<Vote<P>>),
    Propose(Rc<P::Value>),
    Accept(Rc<P::Value>),
}

enum Event<P: Paxos> {
    /// A client requested a value to be proposed.
    RequestReceived(Rc<P::Value>),
    /// Message from a participant.
    MessageReceived(PaxosMessage<P>),
}

enum Effect<P: Paxos> {
    Broadcast { msg: PaxosMessage<P> },
    Unicast { to: P::Pid, msg: PaxosMessage<P> },
}

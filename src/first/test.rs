use std::collections::VecDeque;
use std::rc::Rc;

use super::fsm::*;
use super::PFSM;
use crate::{Effect, Event, InstanceId, Paxos, PaxosInstance};

#[derive(Clone, Debug)]
struct TestPaxos;

type Pid = u32;

impl Paxos for TestPaxos {
    type Pid = Pid;
    type Value = String;
}

#[test]
fn runner() {
    let pids = vec![1, 2, 3];

    let mut runner = FSMRunner::<Pid, PFSM<TestPaxos>, _>::new(
        PFSM::new(),
        pids.iter()
            .map(|pid| {
                (
                    *pid,
                    PaxosInstance::new(InstanceId(1), *pid, pids.iter().map(|pid| *pid).collect()),
                )
            })
            .collect(),
        VecDeque::from([
            (
                1,
                Event::RequestReceived(Rc::new("Agree on this!".to_owned())),
            ),
            (
                2,
                Event::RequestReceived(Rc::new("Or maybe this?".to_owned())),
            ),
        ]),
        |effect: Effect<TestPaxos>| match effect {
            Effect::Broadcast { msg } => {
                // Prints only visible if the test fails.
                println!("Broadcasting {:#?}", msg);
                pids.iter()
                    .map(|pid| (*pid, Event::MessageReceived(msg.clone())))
                    .collect()
            }

            Effect::Unicast { to, msg } => {
                println!("Broadcasting {:#?}", msg);
                vec![(to, Event::MessageReceived(msg))]
            }
        },
    );

    let mut i = 0;

    // Not expecting errors.
    while runner.tick_one().unwrap() && i < 100 {
        i = i + 1;
    }

    // Check that there are no more tasks.
    assert!(!runner.tick_one().unwrap());

    // Check that everyone is on the same value.
    for state in runner.get_states() {
        let accepted_value = state.accepting_vote.as_ref().map(|v| v.value.as_ref());
        assert_eq!(accepted_value, Some(&"Agree on this!".into()));
    }
}

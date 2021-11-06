use std::collections::VecDeque;
use std::sync::Arc;

use super::fsm::*;
use super::PFSM;
use crate::PaxosMessageDetail;
use crate::{Effect, Event, InstanceId, Paxos, PaxosInstance, PaxosMessage};

#[derive(Clone, Debug)]
struct TestPaxos;

type Pid = u32;

impl Paxos for TestPaxos {
    type Pid = Pid;
    type Value = String;
}

fn msg_desc(msg: &PaxosMessage<TestPaxos>) -> String {
    match &msg.detail {
        PaxosMessageDetail::Prepare => format!(
            "1a Prepare({}/{})",
            msg.ballot_ordinal.pid, msg.ballot_ordinal.round
        ),

        PaxosMessageDetail::Promise(vote) => format!(
            "1b Promise({}/{}, {:?}, {:?})",
            msg.ballot_ordinal.pid,
            msg.ballot_ordinal.round,
            vote.as_ref().map(|v| v.ord.round),
            vote.as_ref().map(|v| v.value.as_ref())
        ),

        PaxosMessageDetail::Propose(value) => format!(
            "2a Propose({}/{}, \"{}\")",
            msg.ballot_ordinal.pid,
            msg.ballot_ordinal.round,
            value.as_ref()
        ),

        PaxosMessageDetail::Accept(value) => format!(
            "2b Accept({}/{}, \"{}\")",
            msg.ballot_ordinal.pid,
            msg.ballot_ordinal.round,
            value.as_ref()
        ),
    }
}

#[test]
fn runner() {
    let pids = vec![1, 2, 3];

    let print_uml_on_success = false;
    let print_uml_on_failure = true;

    // Print out a PlantUML markup
    let uml = |s: &str| {
        if print_uml_on_success || print_uml_on_failure {
            println!("{}", s);
        }
    };

    uml("@startuml");
    for pid in &pids {
        println!("participant \"Process {0}\" as {0}", pid);
    }

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
                Event::RequestReceived(Arc::new("Agree on this!".to_owned())),
            ),
            (
                2,
                Event::RequestReceived(Arc::new("Or maybe this?".to_owned())),
            ),
        ]),
        |effect: Effect<TestPaxos>| match effect {
            Effect::Broadcast { msg } => {
                // Prints only visible if the test fails.
                //println!("Broadcast {:#?}", msg);
                pids.iter()
                    .map(|pid| {
                        uml(&format!("{} -> {}: {}", msg.src, pid, msg_desc(&msg)));
                        (*pid, Event::MessageReceived(msg.clone()))
                    })
                    .collect()
            }

            Effect::Unicast { to, msg } => {
                //println!("Unicast {:#?} to {}", msg, to);
                uml(&format!("{} -> {}: {}", msg.src, to, msg_desc(&msg)));
                vec![(to, Event::MessageReceived(msg))]
            }
        },
    );

    let mut i = 0;

    // Not expecting errors.
    while runner.tick_one().unwrap() && i < 100 {
        i = i + 1;
    }

    uml("@enduml");

    // Check that there are no more tasks.
    assert!(!runner.tick_one().unwrap());

    // Check that everyone is on the same value.
    // The 2nd process has a higher PID, so it should win, because of the ordering of messages.
    for state in runner.get_states() {
        let accepted_value = state.accepting_vote.as_ref().map(|v| v.value.as_ref());
        assert_eq!(accepted_value, Some(&"Or maybe this?".into()));
    }

    // Trigger a failure to show the UML sequence diagram if we want.
    assert!(!print_uml_on_success);
}

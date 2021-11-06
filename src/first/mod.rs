mod fsm;

#[cfg(test)]
mod test;

use std::{collections::HashSet, marker::PhantomData, sync::Arc};

use crate::{Effect, Paxos, PaxosInstance, PaxosMessage, PaxosMessageDetail, Vote};
use fsm::{FSMResult, FSM};

type Result<P> = FSMResult<PaxosInstance<P>, Effect<P>>;

struct PFSM<P: Paxos> {
    phantom: PhantomData<P>,
}

impl<P: Paxos + Clone> FSM for PFSM<P> {
    type State = PaxosInstance<P>;
    type Event = crate::Event<P>;
    type Effect = crate::Effect<P>;

    fn update(&self, state: &PaxosInstance<P>, event: Self::Event) -> Result<P> {
        match event {
            crate::Event::RequestReceived(value) => PFSM::handle_request(state, value),
            crate::Event::MessageReceived(msg) => PFSM::handle_message(state, msg),
        }
    }
}

impl<P: Paxos + Clone> PFSM<P> {
    fn new() -> PFSM<P> {
        PFSM {
            phantom: PhantomData,
        }
    }

    fn ignore() -> Result<P> {
        Ok((None, Vec::new()))
    }

    fn ok(inst: PaxosInstance<P>, effects: Vec<Effect<P>>) -> Result<P> {
        Ok((Some(inst), effects))
    }

    fn handle_request(state: &PaxosInstance<P>, value: Arc<P::Value>) -> Result<P> {
        // Making a clone because the ballot ordinal will be incremented in all cases.
        let mut inst: PaxosInstance<P> = state.clone();
        inst.max_ballot_ordinal = inst.max_ballot_ordinal.incr(inst.my_pid);
        if inst.requested_value.is_none() {
            inst.requested_value = Some(value);
        }
        let effects = vec![PFSM::send_prepare(&inst)];
        PFSM::ok(inst, effects)
    }

    fn handle_message(state: &PaxosInstance<P>, msg: PaxosMessage<P>) -> Result<P> {
        if msg.instance_id != state.id {
            Err("Wrong instance ID.".to_owned())
        } else if msg.members != state.members {
            Err("Wrong members.".to_owned())
        } else {
            use PaxosMessageDetail::*;
            match msg.detail {
                Prepare if msg.ballot_ordinal <= state.max_ballot_ordinal => PFSM::ignore(),

                Prepare => {
                    let mut inst = state.clone();
                    inst.max_ballot_ordinal = msg.ballot_ordinal;

                    let effects = vec![PFSM::send_promise(&inst, msg.src)];

                    PFSM::ok(inst, effects)
                }

                Promise(vote) => {
                    let mut inst = state.clone();
                    if inst.max_ballot_ordinal == msg.ballot_ordinal {
                        inst.safe_value_vote = match (inst.safe_value_vote, vote) {
                            (None, new) => new,
                            (Some(old), Some(new)) if old.ord < new.ord => Some(new),
                            (old, _) => old,
                        }
                    }

                    let effects = if PFSM::do_add_promise(&mut inst, msg.src) {
                        inst.safe_value_vote
                            .as_ref()
                            .map(|vote| &vote.value)
                            .or(inst.requested_value.as_ref())
                            .map(|value| PFSM::send_propose(&inst, value.clone()))
                            .into_iter()
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };

                    PFSM::ok(inst, effects)
                }

                Propose(_) if msg.ballot_ordinal < state.max_ballot_ordinal => PFSM::ignore(),

                Propose(value) => {
                    let mut inst = state.clone();
                    inst.max_ballot_ordinal = msg.ballot_ordinal;
                    inst.vote = Some(Vote {
                        value: value.clone(),
                        ord: msg.ballot_ordinal,
                    });

                    let effects = vec![PFSM::send_accept(&inst, value)];

                    PFSM::ok(inst, effects)
                }

                // Process new accepts only if we are not yet decided and the
                // accept's ord is >= accepting-ord
                Accept(_)
                    if state.decided
                        || state
                            .accepting_vote
                            .as_ref()
                            .map_or(false, |vote| vote.ord > msg.ballot_ordinal) =>
                {
                    PFSM::ignore()
                }

                Accept(value) => {
                    let mut inst = state.clone();
                    if !inst.decided
                        && inst
                            .accepting_vote
                            .as_ref()
                            .map_or(true, |vote| vote.ord < msg.ballot_ordinal)
                    {
                        inst.accepts.clear();
                        inst.accepting_vote = Some(Vote {
                            value,
                            ord: msg.ballot_ordinal,
                        })
                    }
                    if PFSM::do_add_accept(&mut inst, msg.src) {
                        inst.decided = true;
                    }
                    PFSM::ok(inst, vec![])
                }
            }
        }
    }

    fn do_add_promise(inst: &mut PaxosInstance<P>, pid: P::Pid) -> bool {
        PFSM::do_add_until_quorum(
            inst,
            pid,
            |inst| &inst.promises,
            |inst, pid| {
                inst.promises.insert(pid);
            },
        )
    }

    fn do_add_accept(inst: &mut PaxosInstance<P>, pid: P::Pid) -> bool {
        PFSM::do_add_until_quorum(
            inst,
            pid,
            |inst| &inst.accepts,
            |inst, pid| {
                inst.accepts.insert(pid);
            },
        )
    }

    fn do_add_until_quorum<F, G>(
        inst: &mut PaxosInstance<P>,
        pid: P::Pid,
        get_pids: F,
        add_pid: G,
    ) -> bool
    where
        F: Fn(&PaxosInstance<P>) -> &HashSet<P::Pid>,
        G: Fn(&mut PaxosInstance<P>, P::Pid) -> (),
    {
        let pids = get_pids(inst);
        if PFSM::check_quorum(inst, pids) {
            false
        } else {
            add_pid(inst, pid);
            PFSM::check_quorum(inst, get_pids(inst))
        }
    }

    fn check_quorum(inst: &PaxosInstance<P>, pids: &HashSet<P::Pid>) -> bool {
        let allowed = inst.members.intersection(pids);
        2 * allowed.count() > inst.members.len()
    }

    /// Create a 1a message.
    fn send_prepare(inst: &PaxosInstance<P>) -> Effect<P> {
        PFSM::send_all(inst, PaxosMessageDetail::Prepare)
    }

    /// Create a 1b message.
    fn send_promise(inst: &PaxosInstance<P>, dest: P::Pid) -> Effect<P> {
        Effect::Unicast {
            to: dest,
            msg: PFSM::make_msg(inst, PaxosMessageDetail::Promise(inst.vote.clone())),
        }
    }

    fn send_propose(inst: &PaxosInstance<P>, value: Arc<P::Value>) -> Effect<P> {
        PFSM::send_all(inst, PaxosMessageDetail::Propose(value))
    }

    fn send_accept(inst: &PaxosInstance<P>, value: Arc<P::Value>) -> Effect<P> {
        PFSM::send_all(inst, PaxosMessageDetail::Accept(value))
    }

    fn send_all(inst: &PaxosInstance<P>, detail: PaxosMessageDetail<P>) -> Effect<P> {
        Effect::Broadcast {
            msg: PFSM::make_msg(inst, detail),
        }
    }

    fn make_msg(inst: &PaxosInstance<P>, detail: PaxosMessageDetail<P>) -> PaxosMessage<P> {
        PaxosMessage {
            src: inst.my_pid,
            instance_id: inst.id,
            members: inst.members.clone(),
            ballot_ordinal: inst.max_ballot_ordinal,
            detail,
        }
    }
}

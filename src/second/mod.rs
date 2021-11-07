use crate::stm::queues::{tqueue::TQueue, TQueueLike};
use crate::stm::{atomically, STMResult, TVar};
use crate::{
    BallotOrdinal, ClientRequest, Effect, Event, InstanceId, Paxos, PaxosMessage,
    PaxosMessageDetail, Vote,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;

/// Every connected process with a queue to send messages to it.
/// A thread will listen to that queue and relay the messages over the network.
type ProcessRegistry<P: Paxos> = TVar<HashMap<P::Pid, TQueue<PaxosMessage<P>>>>;

/// Every running instance, with a queue to send events to it.
/// A thread will listen to that queue and handle the events by sending messages
/// via the process registry.
type InstanceRegistry<P: Paxos> =
    TVar<HashMap<InstanceId, (Arc<PaxosInstance<P>>, TQueue<Event<P>>)>>;

struct PaxosService<P: Paxos + Clone> {
    my_pid: P::Pid,
    instances: TVar<HashMap<InstanceId, (Arc<PaxosInstance<P>>, TQueue<Event<P>>)>>,
    processes: ProcessRegistry<P>,
}

impl<P: Paxos + Clone + 'static> PaxosService<P>
where
    P::Pid: Default,
{
    pub fn new(my_pid: P::Pid) -> PaxosService<P> {
        PaxosService {
            my_pid,
            instances: TVar::new(HashMap::new()),
            processes: TVar::new(HashMap::new()),
        }
    }

    /// Register a client process that we can send messages to by writing to a TQueue.
    /// A thread associated with that process will read that queue and send over the network.
    pub fn register_process(&self, pid: P::Pid, queue: TQueue<PaxosMessage<P>>) {
        atomically(|| {
            self.processes.update(|ps| {
                let mut ps = ps.clone();
                ps.insert(pid, queue.clone());
                ps
            })
        });
    }

    /// Maybe a JSON-RPC interface received a message from a client.
    pub fn handle_request(&self, request: ClientRequest<P>) {
        let queue = self.get_or_create_inst_event_queue(request.instance_id, &request.members);
        let value = Arc::new(request.value);
        atomically(|| queue.write(Event::RequestReceived(value.clone())))
    }

    /// Maybe a thread reading packet from a network socket received a message from a peer.
    pub fn handle_message(&self, message: PaxosMessage<P>) {
        let queue = self.get_or_create_inst_event_queue(message.instance_id, &message.members);
        atomically(|| queue.write(Event::MessageReceived(message.clone())))
    }

    fn get_or_create_inst_event_queue(
        &self,
        id: InstanceId,
        members: &HashSet<P::Pid>,
    ) -> TQueue<Event<P>> {
        let (queue, inst) = atomically(|| {
            let insts = self.instances.read()?;
            match insts.get(&id) {
                None => {
                    let inst = PaxosInstance::new(id, self.my_pid, members.clone());
                    let queue = TQueue::new();

                    let mut insts = insts.as_ref().clone();
                    insts.insert(id, (inst.clone(), queue.clone()));
                    self.instances.write(insts)?;

                    Ok((queue, Some(inst)))
                }
                Some((_, queue)) => Ok((queue.clone(), None)),
            }
        });

        if let Some(inst) = inst {
            let queuec = queue.clone();
            let processesc = self.processes.clone();
            thread::spawn(move || PaxosService::handle_events(processesc, queuec, inst));
        }

        queue
    }

    /// Forever take one event from the queue and process it.
    fn handle_events(
        processes: ProcessRegistry<P>,
        queue: TQueue<Event<P>>,
        inst: Arc<PaxosInstance<P>>,
    ) {
        loop {
            atomically(|| {
                let result = match queue.read()? {
                    Event::RequestReceived(value) => inst.handle_request(value)?,
                    Event::MessageReceived(_msg) => todo!(),
                };

                match result {
                    Err(msg) => {
                        // TODO: Trace in STM? Or return an trace after the atomically?
                        println!("Error handling message: {}", msg);
                        Ok(())
                    }
                    Ok(effects) => PaxosService::handle_effects(processes.clone(), effects),
                }
            });
        }
    }

    fn handle_effects(processes: ProcessRegistry<P>, effects: Vec<Effect<P>>) -> STMResult<()> {
        let ps = processes.read()?;
        for effect in effects {
            match effect {
                Effect::Broadcast { msg } => {
                    for (_, q) in ps.as_ref() {
                        q.write(msg.clone())?;
                    }
                    // TODO: Message to self.
                }

                Effect::Unicast { to, msg } => {
                    if let Some(q) = ps.get(&to) {
                        q.write(msg)?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Allow returning error messages from the STM functions.
type EventResult<P: Paxos> = STMResult<Result<Vec<Effect<P>>, String>>;

#[derive(Clone)]
struct PaxosInstance<P: Paxos + Clone> {
    id: InstanceId,
    my_pid: P::Pid,
    members: HashSet<P::Pid>,
    max_ballot_ordinal: TVar<BallotOrdinal<P::Pid>>,
    vote: TVar<Option<Vote<P>>>,
    requested_value: TVar<Option<Arc<P::Value>>>,
    safe_value_vote: TVar<Option<Vote<P>>>,
    promises: TVar<HashSet<P::Pid>>,
    accepting_vote: TVar<Option<Vote<P>>>,
    accepts: TVar<HashSet<P::Pid>>,
    decided: TVar<bool>,
}

impl<P: Paxos + Clone + 'static> PaxosInstance<P>
where
    P::Pid: Default + 'static,
{
    fn new(id: InstanceId, my_pid: P::Pid, members: HashSet<P::Pid>) -> Arc<PaxosInstance<P>> {
        Arc::new(PaxosInstance {
            id,
            my_pid,
            members,
            max_ballot_ordinal: TVar::new(BallotOrdinal {
                pid: P::Pid::default(),
                round: 0,
            }),
            vote: TVar::new(None),
            requested_value: TVar::new(None),
            safe_value_vote: TVar::new(None),
            promises: TVar::new(HashSet::new()),
            accepting_vote: TVar::new(None),
            accepts: TVar::new(HashSet::new()),
            decided: TVar::new(false),
        })
    }

    fn handle_request(&self, value: Arc<P::Value>) -> EventResult<P> {
        self.max_ballot_ordinal
            .update(|ord| ord.incr(self.my_pid))?;
        if self.requested_value.read()?.is_none() {
            self.requested_value.write(Some(value))?;
        }
        let effects = vec![self.send_prepare()?];
        Ok(Ok(effects))
    }

    fn send_prepare(&self) -> STMResult<Effect<P>> {
        self.send_all(PaxosMessageDetail::Prepare)
    }

    fn send_all(&self, detail: PaxosMessageDetail<P>) -> STMResult<Effect<P>> {
        Ok(Effect::Broadcast {
            msg: self.make_msg(detail)?,
        })
    }

    fn make_msg(&self, detail: PaxosMessageDetail<P>) -> STMResult<PaxosMessage<P>> {
        let msg = PaxosMessage {
            src: self.my_pid,
            instance_id: self.id,
            members: self.members.clone(),
            ballot_ordinal: self.max_ballot_ordinal.read()?.as_ref().clone(),
            detail,
        };
        Ok(msg)
    }
}

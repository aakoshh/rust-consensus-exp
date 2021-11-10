use crate::first::fsm::FSM;
use crate::first::PFSM;
use crate::stm::queues::{tqueue::TQueue, TQueueLike};
use crate::stm::{atomically, or, StmResult, TVar};
use crate::{ClientRequest, Effect, Event, InstanceId, Paxos, PaxosInstance, PaxosMessage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;

struct PaxosService<P: Paxos, O> {
    my_pid: P::Pid,
    client_request_queue: TQueue<ClientRequest<P>>,
    incoming_message_queue: TQueue<PaxosMessage<P>>,
    outgoing_message_queue: O,
    // NOTE: Alternatively it could be a pair of state and queue.
    instances: TVar<HashMap<InstanceId, TVar<PaxosInstance<P>>>>,
    machine: PFSM<P>,
}

impl<P: Paxos + Clone + Send + 'static, O: Send + 'static> PaxosService<P, O>
where
    O: Fn(P::Pid) -> StmResult<Option<TQueue<PaxosMessage<P>>>>,
    P::Pid: Default,
{
    pub fn new(
        my_pid: P::Pid,
        client_request_queue: TQueue<ClientRequest<P>>,
        incoming_message_queue: TQueue<PaxosMessage<P>>,
        outgoing_message_queue: O,
    ) -> PaxosService<P, O> {
        PaxosService {
            my_pid,
            client_request_queue,
            incoming_message_queue,
            outgoing_message_queue,
            instances: TVar::new(HashMap::new()),
            machine: PFSM::new(),
        }
    }

    pub fn run(self) {
        thread::spawn(move || loop {
            // NOTE: We can handle each message as one transaction, or we could break it up:
            // 1) create an instance and enqueue the message, then spawn a thread to handle it if it's new
            // 2) handle the event on a per-instance thread
            atomically(|| {
                or(
                    || {
                        let request = self.client_request_queue.read()?;
                        self.handle_request(request)
                    },
                    || {
                        let message = self.incoming_message_queue.read()?;
                        self.handle_message(message)
                    },
                )
            })
        });
    }

    fn get_or_create_inst(
        &self,
        id: InstanceId,
        members: &HashSet<P::Pid>,
    ) -> StmResult<TVar<PaxosInstance<P>>> {
        let insts = self.instances.read()?;
        match insts.get(&id) {
            None => {
                let inst = TVar::new(PaxosInstance::new(id, self.my_pid, members.clone()));

                let mut insts = insts.as_ref().clone();
                insts.insert(id, inst.clone());
                self.instances.write(insts)?;

                Ok(inst)
            }
            Some(inst) => Ok(inst.clone()),
        }
    }

    fn handle_request(&self, request: ClientRequest<P>) -> StmResult<()> {
        let inst = self.get_or_create_inst(request.instance_id, &request.members)?;
        self.handle_event(inst, Event::RequestReceived(Arc::new(request.value)))
    }

    fn handle_message(&self, message: PaxosMessage<P>) -> StmResult<()> {
        let inst = self.get_or_create_inst(message.instance_id, &message.members)?;
        self.handle_event(inst, Event::MessageReceived(message))
    }

    fn handle_event(&self, inst: TVar<PaxosInstance<P>>, event: Event<P>) -> StmResult<()> {
        let state = inst.read()?;
        match self.machine.update(state.as_ref(), event) {
            Ok((maybe_new_inst, effects)) => {
                if let Some(new_inst) = maybe_new_inst {
                    inst.write(new_inst)?;
                }
                self.handle_effects(effects)
            }
            Err(msg) => {
                // TODO: Trace inside the transaction?
                println!("Error handling client request: {}", msg);
                Ok(())
            }
        }
    }

    fn handle_effects(&self, effects: Vec<Effect<P>>) -> StmResult<()> {
        for effect in effects {
            match effect {
                Effect::Broadcast { msg } => {
                    for pid in &msg.members {
                        if let Some(q) = (self.outgoing_message_queue)(*pid)? {
                            q.write(msg.clone())?;
                        }
                    }
                    self.incoming_message_queue.write(msg)?;
                }

                Effect::Unicast { to, msg } => {
                    if let Some(q) = (self.outgoing_message_queue)(to)? {
                        q.write(msg)?;
                    }
                }
            }
        }
        Ok(())
    }
}

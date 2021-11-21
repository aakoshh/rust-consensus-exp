use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::hash::Hash;

/// A finite state machine interface that takes a reference to the state
/// and returns a modified clone of it, along with a list of effects.
/// It may return `None` to show that the state didn't have to be updated,
/// to save needless cloning.
///
/// Instead of `State` it could take `&self` and return `Self`, however
/// a trait implementation can't have helper functions, so some kind of
/// wrapper may be used anyway, and by separating the two types the
/// implementation can have access to other things as well from the
/// environment, that aren't strictly part of the state.
pub trait FSM {
    type State: Clone;
    type Event;
    type Effect;

    fn update(
        &self,
        state: &Self::State,
        event: Self::Event,
    ) -> FSMResult<Self::State, Self::Effect>;
}

pub type FSMResult<S, E> = std::result::Result<(Option<S>, Vec<E>), String>;

pub struct FSMRunner<P, M: FSM, E> {
    machine: M,
    states: HashMap<P, M::State>,
    events: VecDeque<(P, M::Event)>,
    execute: E,
}

impl<P, M: FSM, E> FSMRunner<P, M, E>
where
    P: Eq + Hash + Display,
    E: Fn(M::Effect) -> Vec<(P, M::Event)>,
{
    pub fn new(
        machine: M,
        states: HashMap<P, M::State>,
        events: VecDeque<(P, M::Event)>,
        execute: E,
    ) -> FSMRunner<P, M, E> {
        FSMRunner {
            machine,
            states,
            events,
            execute,
        }
    }

    /// Take one event, run it, execute its effects.
    /// Return a `false` if the queue was empty.
    pub fn tick_one(&mut self) -> Result<bool, String> {
        match self.events.pop_front() {
            None => Ok(false),

            Some((pid, event)) => {
                let state = self
                    .states
                    .get(&pid)
                    .ok_or(format!("Cannot find state for process '{}'.", pid))?;

                match self.machine.update(state, event) {
                    Err(msg) => Err(format!("Error updating process '{}': '{}'", pid, msg)),
                    Ok((maybe_new_state, effects)) => {
                        for effect in effects {
                            for event in (self.execute)(effect) {
                                self.events.push_back(event);
                            }
                        }
                        maybe_new_state.map(|new_state| {
                            self.states.insert(pid, new_state);
                        });
                        Ok(true)
                    }
                }
            }
        }
    }

    pub fn get_states(&self) -> Vec<&M::State> {
        self.states.values().collect()
    }
}

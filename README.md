# Rust Consensus Experiments

Exploring modeling techniques we can use for flexible generic consensus in blockchains.

## Software Transactional Memory

The [stm](src/stm) module was inspired by [rust-stm](https://github.com/Marthog/rust-stm) but uses MVCC instead and allows aborting transactions with errors. The module also contains various transactional queue implementations based on [Simon Marlow's book](https://simonmar.github.io/pages/pcph.html) about concurrent programming in Haskell.

The idea to explore STM for consensus is based how it's [used in Cardano](https://forum.cardano.org/t/duncan-coutts-on-cardano-s-concurrency-with-haskell-s-stm/39370).

## Session Types

The [session_types](src/session_types) module adopted from [Munksgaard/session-types](https://github.com/Munksgaard/session-types), with a few modifications:
* Operations return a `Result` so various errors can be handled, such as disconnects.
* Expects that messagages arrive from an untrusted party over the network, so it attempts to downcast into expected types, aborting the session if the wrong type of data is received.
* All receives have to use a timeouts.
* Offers/Choices can only be all incoming/outgoing, respectively, so it is always unambiguous which party has to send the next message. This was inspired by the [Cardano Shelley Networking Protocol](https://github.com/input-output-hk/ouroboros-network#ouroboros-network-documentation), in particular the "agency" model.

## Era Combinator

The [blockchain](src/blockchain) defines three different [eras](src/blockchain/eras), each with different block types and blockchain structure,
but all of them implementing generic [properties](src/blockchain/property.rs). The overall consensus has to be defined in terms of those properties, and instantiated over an era which is the [coproduct](src/blockchain/eras/mod.rs) of all the other eras, treating the whole blockchain look as one contiguous chain with heterogenous content.

The blockchain model starts with a traditional blockchain (i.e. a tree) in `era1` and moves to a hybrid design with a DAG of input blocks referenced by a tree of ranking blocks by `era3`.

The coproduct design was inspired by the [Cardano Hard Fork Combinator](https://docs.cardano.org/core-concepts/about-hard-forks), and the "property" term came from the [chain-libs](https://github.com/input-output-hk/chain-libs/blob/master/chain-core/src/property.rs) used by [Jormungandr](https://github.com/input-output-hk/jormungandr), but expanded to bundle related types into eras.

## Blockchain Synchronisation

The [sync](src/blockchain/protocols/sync) module is a Proof-of-Concept to see how we can adopt the chain-sync [mini-protocol](https://coot.me/posts/typed-protocol-pipelining.html) from the Cardano Shelley network spec to the hybrid input + ranking block design, using the STM and Session Types modules introduced earlier. The difference is that instead of explicit states and agency, we are using [russian doll-like](src/blockchain/protocols/sync/mod.rs) typed channels.

The [consumer](src/blockchain/protocols/sync/consumer.rs) and the [producer](src/blockchain/protocols/sync/producer.rs) that implement this typed protocol are using STM enabled [storage](src/blockchain/store) components to maintain their state. They are generic in the type of content they handle, as long as they conform to the properties defined in the era model. Only the era-specific block store implementations know about the specific types of blocks they can handle.

## Paxos

The [paxos](src/paxos) module has partial experiments to model the [Paxos consensus](https://vadosware.io/post/paxosmon-gotta-concensus-them-all/) algorithm:
* `first`, with a pure Finite State Machine
* `second`, with STM
* `third`, with a hybrid approach using STM to wrap the coarse grained FSM from the `first` attempt, and STM queues to communicate with peers, inspired by the [chat server example](https://github.com/simonmar/parconc-examples/blob/master/distrib-chat/chat.hs) in Simon Marlow's book.

#![forbid(unsafe_code)]

//! Opaque capability credentials and operational digest-key custody.

/// Isolated offline bootstrap credential generation and validation.
pub mod bootstrap_secret;

mod authenticator;
mod current;
mod digest_keys;
mod entropy;
mod principal_facts;
mod protected_file;
mod token;

pub use authenticator::*;
pub use current::*;
pub use digest_keys::*;
pub use entropy::*;
pub use principal_facts::*;
pub use riffdb_storage_api::{
    ChangelogTransactionSequence, FollowerHoldBudget, ReplicationAdministrationRequestV1,
};
pub use token::*;

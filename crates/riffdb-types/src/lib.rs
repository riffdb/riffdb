#![forbid(unsafe_code)]

//! Canonical domain types shared across RiffDB semantic boundaries.

mod actor;
mod aggregate;
/// Canonical application-installation value model.
pub mod application_plan;
/// Canonical application-portability value model.
pub mod application_portability;
mod canonical_clone_capacity;
mod capability;
mod capability_grant;
mod codec;
mod columnar;
mod consumer;
mod database;
mod decimal;
mod dual_frontier;
/// Trusted compiled exact-provider accounting; applications have no budget selector.
#[doc(hidden)]
#[path = "exact_provider_budget.rs"]
pub mod exact_provider_budget_v1;
/// Trusted fixed source-worker accounting; no application budget selector.
#[doc(hidden)]
pub use exact_provider_budget_v1::{
    PROJECTION_SOURCE_WORKING_MEMORY_BYTES, ProjectionSourceWorkAllocationV1,
    ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};
mod exact_text;
mod execution;
mod export;
mod freshness;
mod hash;
mod ids;
mod key;
mod limits;
mod long_pattern;
mod maintenance;
mod owned_capacity;
mod owned_capacity_collections;
mod owned_capacity_lifecycle;
mod owned_capacity_shared;
#[cfg(test)]
mod owned_capacity_tests;
mod principal_fact;
mod projection;
mod projection_provider;
mod projection_source;
mod query;
mod reimport;
mod replication;
mod secret;
mod service;
mod text_analyzer;
mod time;
mod unicode_fold;
mod value;
mod vector;

pub use actor::*;
pub use aggregate::*;
pub use application_plan::*;
pub use application_portability::*;
pub use canonical_clone_capacity::*;
pub use capability::*;
pub use capability_grant::*;
pub use codec::*;
pub use columnar::*;
pub use consumer::*;
pub use database::*;
pub use decimal::*;
pub use dual_frontier::*;
pub use exact_text::*;
pub use execution::*;
pub use export::*;
pub use freshness::*;
pub use hash::*;
pub use ids::*;
pub use key::*;
pub use limits::*;
pub use long_pattern::*;
pub use maintenance::*;
pub use owned_capacity::*;
pub use owned_capacity_lifecycle::{
    OwnedCapacityLifecycleObserverV1, OwnedCapacityLifecycleReferenceV1, OwnedCapacityLifecycleV1,
};
pub use owned_capacity_shared::{
    IntoOwnedCapacityErasedV1, OwnedCapacityErasedV1, OwnedCapacitySharedV1, OwnedCapacitySliceV1,
    OwnedCapacityTextV1, OwnedCapacityWeakV1,
};
pub use principal_fact::*;
pub use projection::*;
pub use projection_provider::*;
pub use projection_source::*;
pub use query::*;
pub use reimport::*;
pub use replication::*;
pub use secret::*;
pub use service::*;
pub use text_analyzer::*;
pub use time::*;
pub use unicode_fold::*;
pub use value::*;
pub use vector::*;

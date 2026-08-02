//! Commit apply protocol with supersession deferral and frontier holdback (D4).

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_storage_api::{
    AuthoritativePointReader, AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest,
    CommittedEntityReferenceV2, EntityTarget, StorageScanLimit, StoredEntityRecordV1,
};
use riffdb_types::{CommitSequence, EntityVersion, FrontierPosition};

use crate::definition::RegisteredDefinition;
use crate::error::ColumnarError;
use crate::store::{
    ColumnarSnapshot, LiveRow, OrgKey, PrimaryKeyBytes, WorkingState, project_cells,
};

/// Progress reported after one `apply_available` pull.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyProgress {
    /// Last commit sequence fully processed (may lead the published frontier).
    pub processed: FrontierPosition,
    /// Visible frontier of the published snapshot.
    pub published_frontier: FrontierPosition,
    /// Number of entities waiting for their superseding commit.
    pub deferred_set_size: usize,
    /// Whether the scan reached the frozen ExactEnd fence.
    pub caught_up: bool,
}

/// Unit-test callback observing every snapshot at the moment it is published.
#[cfg(test)]
pub(crate) type PublishObserver = Box<dyn FnMut(&ColumnarSnapshot)>;

/// Mutable apply machinery shared with the engine.
pub(crate) struct ApplyState {
    pub definition: RegisteredDefinition,
    pub working: WorkingState,
    pub published: Arc<ColumnarSnapshot>,
    pub deferred: BTreeMap<EntityTargetKey, EntityVersion>,
    /// Unit-test observer invoked with every snapshot at the moment it is
    /// published. Never compiled into non-test builds.
    #[cfg(test)]
    pub(crate) publish_observer: Option<PublishObserver>,
}

/// Orderable wrapper for EntityTarget (keyed by entity key bytes).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct EntityTargetKey {
    entity_type: u32,
    key: Vec<u8>,
}

impl EntityTargetKey {
    fn from_target(target: &EntityTarget) -> Self {
        Self {
            entity_type: target.entity_type_id().get(),
            key: target.key().as_bytes().to_vec(),
        }
    }
}

impl ApplyState {
    pub(crate) fn new(definition: RegisteredDefinition) -> Self {
        Self {
            definition,
            working: WorkingState::default(),
            published: Arc::new(ColumnarSnapshot::empty()),
            deferred: BTreeMap::new(),
            #[cfg(test)]
            publish_observer: None,
        }
    }

    /// Pulls available commits and applies them under the D4 protocol.
    pub(crate) fn apply_available(
        &mut self,
        reader: &(impl AuthoritativeScanReader + AuthoritativePointReader),
    ) -> Result<ApplyProgress, ColumnarError> {
        let limit = StorageScanLimit::new(64).ok_or(ColumnarError::Integrity("scan limit"))?;
        let mut scan = match self.working.processed {
            FrontierPosition::BeforeFirst => CommitScanRequest::initial(limit),
            FrontierPosition::AppliedThrough(sequence) => {
                CommitScanRequest::initial_after(sequence, limit)
            }
        };
        let caught_up;
        loop {
            let page = reader
                .scan_commits(scan)
                .map_err(|error| ColumnarError::Storage(error.to_string()))?;
            let inclusive_upper = page.inclusive_upper();
            if inclusive_upper < self.working.processed {
                return Err(ColumnarError::Integrity(
                    "scan upper before processed frontier",
                ));
            }
            for charged in page.records() {
                let commit = charged.value();
                let sequence = commit.commit_sequence();
                if FrontierPosition::AppliedThrough(sequence) <= self.working.processed {
                    continue;
                }
                if !is_exact_successor(self.working.processed, sequence) {
                    return Err(ColumnarError::Integrity("commit sequence gap"));
                }
                self.apply_commit(reader, commit.entity_references(), sequence)?;
            }
            match page {
                CommitScanPageV1::Page {
                    next_after,
                    inclusive_upper,
                    ..
                } => {
                    let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                        return Err(ColumnarError::Integrity("page upper missing sequence"));
                    };
                    scan = CommitScanRequest::continuing(next_after, upper, limit)
                        .map_err(|_| ColumnarError::Integrity("continue request"))?;
                }
                CommitScanPageV1::ExactEnd { .. } => {
                    // Fence check: after ExactEnd, processed must equal the fence when
                    // the fence is AppliedThrough and we consumed all records, or both
                    // BeforeFirst when the log is empty.
                    match (self.working.processed, inclusive_upper) {
                        (p, u) if p == u => caught_up = true,
                        (FrontierPosition::BeforeFirst, FrontierPosition::BeforeFirst) => {
                            caught_up = true;
                        }
                        _ => {
                            if self.working.processed < inclusive_upper {
                                return Err(ColumnarError::Integrity(
                                    "exact-end fence not reached",
                                ));
                            }
                            caught_up = true;
                        }
                    }
                    break;
                }
            }
        }
        Ok(ApplyProgress {
            processed: self.working.processed,
            published_frontier: self.published.visible_frontier,
            deferred_set_size: self.deferred.len(),
            caught_up,
        })
    }

    fn apply_commit(
        &mut self,
        reader: &impl AuthoritativePointReader,
        references: &[CommittedEntityReferenceV2],
        sequence: CommitSequence,
    ) -> Result<(), ColumnarError> {
        let projected_type = self.definition.entity_type_id();
        for reference in references {
            if reference.target().entity_type_id() != projected_type {
                continue;
            }
            self.apply_entity_reference(reader, reference)?;
        }
        self.working.processed = FrontierPosition::AppliedThrough(sequence);
        // Publication only at the commit boundary, after ALL of the commit's
        // effects are in the working state (D4 all-or-none visibility).
        self.maybe_publish(FrontierPosition::AppliedThrough(sequence));
        Ok(())
    }

    fn apply_entity_reference(
        &mut self,
        reader: &impl AuthoritativePointReader,
        reference: &CommittedEntityReferenceV2,
    ) -> Result<(), ColumnarError> {
        let record = reader
            .read_entity(reference.target())
            .map_err(|error| ColumnarError::Storage(error.to_string()))?
            .ok_or(ColumnarError::Integrity(
                "entity absent for commit reference",
            ))?;

        if reference.matches(&record) {
            self.apply_matched_record(&record)?;
            let key = EntityTargetKey::from_target(reference.target());
            if let Some(deferred_version) = self.deferred.get(&key).copied()
                && record.entity_version().get() >= deferred_version.get()
            {
                self.deferred.remove(&key);
            }
            return Ok(());
        }

        let live_version = record.entity_version();
        if live_version.get() > reference.entity_version().get() {
            // Raced ahead: never apply the newer record at this commit. The
            // commit that produced `live_version` will apply it when reached.
            self.deferred.insert(
                EntityTargetKey::from_target(reference.target()),
                live_version,
            );
            return Ok(());
        }

        // Same version but hash mismatch, or live version behind the reference.
        Err(ColumnarError::Integrity(
            "entity reference does not match live record and is not a forward race",
        ))
    }

    fn apply_matched_record(&mut self, record: &StoredEntityRecordV1) -> Result<(), ColumnarError> {
        let fields = record.fields().fields();
        let (org_value, cells) = project_cells(
            fields,
            self.definition.projected_fields(),
            self.definition.org_scope_field(),
        )?;
        let org = OrgKey::from_value(&org_value)?;
        let key = PrimaryKeyBytes::from_entity_key_bytes(record.target().key().as_bytes().to_vec());
        let row = LiveRow {
            entity_version: record.entity_version(),
            cells,
        };

        // Idempotence: skip when a strictly newer version is already present in
        // the delta (equal-version replay is an identical overwrite).
        if let Some(org_delta) = self.working.delta.get(&org)
            && let Some(existing) = org_delta.get(&key)
            && !crate::store::supersession_should_replace(
                existing.entity_version.get(),
                row.entity_version.get(),
            )
        {
            // Existing is newer than incoming — idempotent skip.
            return Ok(());
        }

        self.working.upsert_live(org, key, row);
        Ok(())
    }

    fn maybe_publish(&mut self, candidate: FrontierPosition) {
        // Frontier holdback: publication lands only on race-free points.
        if self.deferred.is_empty() {
            self.publish(candidate);
        }
        // Else hold back: keep previous published snapshot.
    }

    fn publish(&mut self, frontier: FrontierPosition) {
        let snapshot = self.working.to_snapshot(frontier);
        #[cfg(test)]
        if let Some(observer) = self.publish_observer.as_mut() {
            observer(&snapshot);
        }
        self.published = Arc::new(snapshot);
    }
}

const fn is_exact_successor(frontier: FrontierPosition, sequence: CommitSequence) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => sequence.get() == 1,
        FrontierPosition::AppliedThrough(previous) => match previous.checked_next() {
            Some(expected) => expected.get() == sequence.get(),
            None => false,
        },
    }
}

#[cfg(test)]
mod successor_tests {
    use super::*;
    use riffdb_types::CommitSequence;

    #[test]
    fn exact_successor_from_before_first() {
        assert!(is_exact_successor(
            FrontierPosition::BeforeFirst,
            CommitSequence::new(1).expect("1")
        ));
        assert!(!is_exact_successor(
            FrontierPosition::BeforeFirst,
            CommitSequence::new(2).expect("2")
        ));
    }
}

// Publish-observer property test (same cfg(test)-hook shape as the clone probe
// in riffdb-query-executor's pipeline_clone_tests): the observer sees every
// snapshot at the instant it is published, so moving the publish call inside
// the per-entity loop of `apply_commit` makes this test fail.
#[cfg(test)]
mod publish_observer_tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::ContractBundle;
    use riffdb_storage_api::{
        DurableKeySchemaBindingV1, ExecutablePlanRef, IdempotencyIdentity, StorageError,
        StoredCommitRecordV1, StoredDurableEventV1, StoredOutcomeV1, StoredProvenanceRecordV1,
    };
    use riffdb_types::{
        CanonicalRecord, CanonicalValue, CommandId, CommitSequence, ContractBundleHash,
        ContractLineage, ContractVersion, EntityKeyBuilder, EventId, PlanHash, ProvenanceId,
    };

    use super::*;
    use crate::definition::ColumnarProjectionDefinition;
    use crate::store::OrgKey;

    const CONTRACT: &str = r#"
contract ColumnarPublish version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: u64)
    field status: u64
  }

  event TicketCreated {
    organization_id: uuid
    ticket_id: u64
  }

  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }

  command CreateTicket {
    input idempotency_key: string<128>
    input organization_id: uuid
    input ticket_id: u64
    input status: u64

    idempotency_key idempotency_key
    create Ticket(organization_id, ticket_id) as ticket
      else AlreadyExists { ticket_id: ticket_id }

    set ticket.status = status

    emit TicketCreated { organization_id: organization_id, ticket_id: ticket_id }
    return Created { ticket: ticket }
  }
}
"#;

    struct PointState {
        entities: BTreeMap<Vec<u8>, StoredEntityRecordV1>,
    }

    impl AuthoritativePointReader for PointState {
        fn read_entity(
            &self,
            target: &EntityTarget,
        ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
            Ok(self.entities.get(target.key().as_bytes()).cloned())
        }

        fn read_stored_outcome(
            &self,
            _identity: &IdempotencyIdentity,
        ) -> Result<Option<StoredOutcomeV1>, StorageError> {
            Ok(None)
        }

        fn read_commit(
            &self,
            _sequence: CommitSequence,
        ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
            Ok(None)
        }

        fn read_provenance(
            &self,
            _provenance_id: ProvenanceId,
        ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
            Ok(None)
        }

        fn read_durable_event(
            &self,
            _event_id: EventId,
        ) -> Result<Option<StoredDurableEventV1>, StorageError> {
            Ok(None)
        }
    }

    fn plan_ref() -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("columnar-publish").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::new(1).expect("command"),
            PlanHash::from_bytes([0x22; 32]),
        )
    }

    fn ticket_entity(
        bundle: &ContractBundle,
        org: [u8; 16],
        ticket_id: u64,
        status: u64,
    ) -> StoredEntityRecordV1 {
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Ticket")
            .expect("entity");
        let field = |name: &str| {
            entity
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .map(riffdb_contract_ir::FieldSchema::id)
                .expect("field")
        };
        let mut key = EntityKeyBuilder::new(entity.id());
        key.push_uuid(&org).expect("uuid");
        key.push_u64(ticket_id).expect("u64");
        let target = EntityTarget::new(entity.id(), key.finish().expect("key")).expect("target");
        let fields = CanonicalRecord::new(vec![
            (field("organization_id"), CanonicalValue::Uuid(org)),
            (field("ticket_id"), CanonicalValue::U64(ticket_id)),
            (field("status"), CanonicalValue::U64(status)),
        ])
        .expect("fields");
        let plan = plan_ref();
        StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            plan.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            fields,
        )
        .expect("entity record")
    }

    #[test]
    fn publish_is_all_or_nothing_for_multi_entity_commits() {
        let bundle = compile_contract_source(CONTRACT).expect("compile");
        let entity = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Ticket")
            .expect("entity");
        let org_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "organization_id")
            .map(riffdb_contract_ir::FieldSchema::id)
            .expect("org");
        let status_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "status")
            .map(riffdb_contract_ir::FieldSchema::id)
            .expect("status");
        let definition = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "publish-observer".into(),
                entity_name: "Ticket".into(),
                projected_fields: vec![status_field],
                org_scope_field: org_field,
            },
            &bundle,
        )
        .expect("register");

        let org = [0x77u8; 16];
        let first = ticket_entity(&bundle, org, 1, 10);
        let second = ticket_entity(&bundle, org, 2, 20);
        let first_reference =
            riffdb_storage_api::CommittedEntityReferenceV2::from_post_image(&first).expect("ref");
        let second_reference =
            riffdb_storage_api::CommittedEntityReferenceV2::from_post_image(&second).expect("ref");
        let first_key =
            PrimaryKeyBytes::from_entity_key_bytes(first.target().key().as_bytes().to_vec());
        let second_key =
            PrimaryKeyBytes::from_entity_key_bytes(second.target().key().as_bytes().to_vec());
        let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org key");

        let reader = PointState {
            entities: BTreeMap::from([
                (first.target().key().as_bytes().to_vec(), first.clone()),
                (second.target().key().as_bytes().to_vec(), second.clone()),
            ]),
        };

        let mut state = ApplyState::new(definition);
        let observed: Rc<RefCell<Vec<(bool, bool)>>> = Rc::new(RefCell::new(Vec::new()));
        state.publish_observer = Some(Box::new({
            let observed = Rc::clone(&observed);
            let org_key = org_key.clone();
            let first_key = first_key.clone();
            let second_key = second_key.clone();
            move |snapshot: &ColumnarSnapshot| {
                let merged = snapshot.merged_org(&org_key);
                observed.borrow_mut().push((
                    merged.contains_key(&first_key),
                    merged.contains_key(&second_key),
                ));
            }
        }));

        state
            .apply_commit(
                &reader,
                &[first_reference, second_reference],
                CommitSequence::new(1).expect("1"),
            )
            .expect("apply");

        let observed = observed.borrow();
        assert!(!observed.is_empty(), "commit publication must be observed");
        for (has_first, has_second) in observed.iter() {
            assert_eq!(
                has_first, has_second,
                "published snapshot must contain all-or-nothing of the \
                 multi-entity commit (saw first={has_first}, second={has_second})"
            );
        }
        assert_eq!(
            observed.last(),
            Some(&(true, true)),
            "final publication must contain the complete commit"
        );
    }
}

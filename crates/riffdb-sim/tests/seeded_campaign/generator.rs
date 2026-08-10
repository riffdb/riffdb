//! D1 (SIM-C2): the seeded workload generator.
//!
//! [`WorkloadPlan::generate`] draws a deterministic command sequence from one
//! [`SplitMix64`] stream: command mix (creates versus expected-version
//! replaces, i.e. ADR-0083 supersessions), admission-shape mix (fused
//! vacant-terminal versus two-phase existing-pending), contention profile
//! (fresh targets versus reuse of live ones), value-size distribution (the
//! `note` payload length), and batch grouping (multiple commands staged into
//! one atomic durable commit). The plan is a pure value — a `Vec` of typed
//! steps over typed commands — so campaigns can log, replay, and minimize it
//! without re-running the store.
//!
//! GENERATOR DETERMINISM ≠ EXECUTION-TRACE DETERMINISM: the plan for a seed is
//! byte-stable and pinned here (same-seed equality, cross-seed inequality, and
//! a golden digest), while store-level execution traces are deliberately NOT
//! pinned — see the campaign module comment for the two store-level blockers.

use riffdb_sim::SplitMix64;

/// Version of the plan encoding AND of the generator's draw order. Bump it
/// whenever the step encoding, the set of drawn fields, or the PRNG
/// consumption order changes: plan digests are only comparable within one
/// version, and the version is the first value folded into every digest, so
/// cross-version digests never collide silently. Corpus entries record the
/// version they were pinned under and the replay test refuses stale entries.
pub(crate) const WORKLOAD_GENERATOR_VERSION: u32 = 1;

/// Which durable admission state the committing writer expects to find (the
/// `storage_recovery_matrix` distinction carried through the SIM-C1 oracle:
/// the fused shape never writes a `Pending` row; the two-phase shape leaves
/// one behind for the commit to consume).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionShape {
    /// Fused admission and terminal commit in one durable transition.
    VacantTerminal,
    /// Phase one durably admits a `Pending` row; a later commit consumes it.
    ExistingPending,
}

/// Bounds and mix ratios for one generated plan. All fields feed the plan
/// digest, so two configs that could generate different plans never share an
/// identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GeneratorConfig {
    /// Exact number of commands to plan (every command is one commit
    /// sequence; the recovered final frontier must equal this).
    pub commands: u32,
    /// Bound on distinct entity targets; once reached, every further command
    /// reuses (supersedes) a live target. Must stay well under the durable
    /// inspection limit of 256 targets.
    pub max_targets: u32,
    /// Percent (0..=100) of commands drawn as two-phase admissions.
    pub two_phase_percent: u64,
    /// Percent (0..=100) chance a command supersedes an existing target
    /// instead of creating a fresh one (the contention profile).
    pub reuse_percent: u64,
    /// Percent (0..=100) chance the next command joins the current batch
    /// instead of closing it.
    pub batch_percent: u64,
    /// Upper bound on commands staged into one atomic batch.
    pub max_batch_len: u32,
    /// Upper bound (inclusive) of the drawn `note` payload length in bytes —
    /// the value-size distribution. Must stay within the fixture contract's
    /// `string<512>` bound.
    pub note_len_max: u64,
}

/// One planned command: everything the fixture builder needs, fully resolved
/// at generation time so retries after simulated crashes rebuild the exact
/// same durable records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PlannedCommand {
    /// One-based commit sequence this command must be assigned.
    pub ordinal: u64,
    /// Stable target ordinal (entity key component `6 + target`).
    pub target: u64,
    /// Index of the superseded command in the plan's command vector, when
    /// this command replaces (with expected version) that command's entity.
    pub supersedes: Option<u32>,
    /// How many commits precede this one on the same target (0 for creates);
    /// fixes the expected index-epoch position.
    pub chain_depth: u64,
    /// Drawn `value` payload field, unique per command by construction.
    pub value: u64,
    /// Drawn `note` payload length (the value-size distribution).
    pub note_len: u64,
    /// Admission shape.
    pub shape: AdmissionShape,
}

/// One planned step. Commands are referenced by index into
/// [`WorkloadPlan::commands`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PlannedStep {
    /// Phase one of a two-phase admission: durably admit the command's
    /// `Pending` row (with its `Started` audit) ahead of the commit.
    AdmitPending {
        /// Index of the admitted command.
        command: u32,
    },
    /// Stage every listed command into one batch and commit it atomically.
    /// Members have pairwise-distinct targets, so no command observes another
    /// member's staged transaction-local state.
    CommitBatch {
        /// Member command indices in staging (and sequence) order.
        commands: Vec<u32>,
    },
}

/// A generated plan: a pure value replayable from `(seed, config)` under one
/// generator version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkloadPlan {
    /// Seed the plan was drawn from.
    pub seed: u64,
    /// Config the plan was drawn under.
    pub config: GeneratorConfig,
    /// Every planned command, indexed by the step vector.
    pub commands: Vec<PlannedCommand>,
    /// The ordered step sequence the campaign executes.
    pub steps: Vec<PlannedStep>,
}

fn fold_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

impl WorkloadPlan {
    /// Draws the complete plan for `(seed, config)`. Deterministic: the only
    /// entropy source is one [`SplitMix64`] stream seeded with `seed`, and
    /// every drawn field is stored in the returned value.
    pub(crate) fn generate(seed: u64, config: GeneratorConfig) -> Self {
        assert!(config.commands > 0, "a plan needs at least one command");
        assert!(config.max_targets > 0, "a plan needs at least one target");
        assert!(
            config.max_targets <= 200,
            "target bound must stay within the durable inspection limits"
        );
        assert!(
            config.max_batch_len > 0,
            "batches contain at least one command"
        );
        assert!(
            config.note_len_max <= 512,
            "note length bound exceeds the fixture contract's string<512>"
        );
        let mut rng = SplitMix64::new(seed);
        let mut commands: Vec<PlannedCommand> = Vec::new();
        let mut steps: Vec<PlannedStep> = Vec::new();
        // Per-target chain state: index of the last command on the target.
        let mut chain_tail: Vec<u32> = Vec::new();
        let mut batch: Vec<u32> = Vec::new();
        let mut batch_targets: Vec<u64> = Vec::new();

        for index in 0..config.commands {
            let ordinal = u64::from(index) + 1;
            let budget_exhausted = chain_tail.len() >= config.max_targets as usize;
            if budget_exhausted && batch_targets.len() >= chain_tail.len() {
                // Every live target is already staged in the open batch and no
                // fresh target may be created: close the batch first so a
                // reusable target exists. Not a drawn decision — forced by the
                // batch-disjointness rule — so it consumes no randomness.
                steps.push(PlannedStep::CommitBatch {
                    commands: std::mem::take(&mut batch),
                });
                batch_targets.clear();
            }
            // Contention profile: reuse a live target when the draw says so
            // (or when the target budget is exhausted), excluding targets
            // already staged in the open batch — a batch member must not
            // observe another member's transaction-local state.
            let reusable: Vec<u32> = (0..chain_tail.len())
                .map(|target| u32::try_from(target).expect("bounded target ordinal"))
                .filter(|target| !batch_targets.contains(&u64::from(*target)))
                .collect();
            let reuse = if budget_exhausted {
                true
            } else {
                // The chance draw is consumed even when no target is reusable
                // yet, keeping the stream's consumption independent of prior
                // outcomes' side effects beyond the stored plan itself.
                rng.chance(config.reuse_percent, 100) && !reusable.is_empty()
            };
            let (target, supersedes, chain_depth) = if reuse {
                assert!(
                    !reusable.is_empty(),
                    "the forced flush above guarantees a reusable target"
                );
                let pick = reusable
                    [usize::try_from(rng.next_below(reusable.len() as u64)).expect("bounded pick")];
                let tail = chain_tail[pick as usize];
                let depth = commands[tail as usize].chain_depth + 1;
                (u64::from(pick), Some(tail), depth)
            } else {
                let fresh = u64::try_from(chain_tail.len()).expect("bounded target count");
                (fresh, None, 0)
            };
            let shape = if rng.chance(config.two_phase_percent, 100) {
                AdmissionShape::ExistingPending
            } else {
                AdmissionShape::VacantTerminal
            };
            // Unique per command: the ordinal occupies the high half, a drawn
            // low half varies the bytes (a cross-swapped row can never
            // compare equal).
            let value = (ordinal << 32) | (rng.next_u64() & 0xFFFF_FFFF);
            let note_len = rng.next_below(config.note_len_max + 1);

            if shape == AdmissionShape::ExistingPending {
                steps.push(PlannedStep::AdmitPending { command: index });
            }
            commands.push(PlannedCommand {
                ordinal,
                target,
                supersedes,
                chain_depth,
                value,
                note_len,
                shape,
            });
            if reuse {
                chain_tail[usize::try_from(target).expect("bounded target")] = index;
            } else {
                chain_tail.push(index);
            }
            batch.push(index);
            batch_targets.push(target);

            let close = batch.len() >= config.max_batch_len as usize
                || !rng.chance(config.batch_percent, 100);
            if close {
                steps.push(PlannedStep::CommitBatch {
                    commands: std::mem::take(&mut batch),
                });
                batch_targets.clear();
            }
        }
        if !batch.is_empty() {
            steps.push(PlannedStep::CommitBatch { commands: batch });
        }
        Self {
            seed,
            config,
            commands,
            steps,
        }
    }

    /// The versioned plan identity: [`WORKLOAD_GENERATOR_VERSION`] first, then
    /// seed, config, and every drawn field of every command and step in order.
    /// Any change to the draw order or the encoding changes this digest, which
    /// is what the golden-digest pin falsifies.
    #[must_use]
    pub(crate) fn identity_digest(&self) -> u64 {
        self.digest_with_version(u64::from(WORKLOAD_GENERATOR_VERSION))
    }

    /// Digest under an explicit version value — the version-sensitivity pin
    /// uses this to prove the version constant genuinely feeds the identity.
    #[must_use]
    pub(crate) fn digest_with_version(&self, version: u64) -> u64 {
        let mut bytes = Vec::new();
        fold_u64(&mut bytes, version);
        fold_u64(&mut bytes, self.seed);
        fold_u64(&mut bytes, u64::from(self.config.commands));
        fold_u64(&mut bytes, u64::from(self.config.max_targets));
        fold_u64(&mut bytes, self.config.two_phase_percent);
        fold_u64(&mut bytes, self.config.reuse_percent);
        fold_u64(&mut bytes, self.config.batch_percent);
        fold_u64(&mut bytes, u64::from(self.config.max_batch_len));
        fold_u64(&mut bytes, self.config.note_len_max);
        for command in &self.commands {
            fold_u64(&mut bytes, command.ordinal);
            fold_u64(&mut bytes, command.target);
            fold_u64(
                &mut bytes,
                command.supersedes.map_or(0, |index| u64::from(index) + 1),
            );
            fold_u64(&mut bytes, command.chain_depth);
            fold_u64(&mut bytes, command.value);
            fold_u64(&mut bytes, command.note_len);
            fold_u64(
                &mut bytes,
                match command.shape {
                    AdmissionShape::VacantTerminal => 1,
                    AdmissionShape::ExistingPending => 2,
                },
            );
        }
        for step in &self.steps {
            match step {
                PlannedStep::AdmitPending { command } => {
                    fold_u64(&mut bytes, 1);
                    fold_u64(&mut bytes, u64::from(*command));
                }
                PlannedStep::CommitBatch { commands } => {
                    fold_u64(&mut bytes, 2);
                    fold_u64(&mut bytes, commands.len() as u64);
                    for command in commands {
                        fold_u64(&mut bytes, u64::from(*command));
                    }
                }
            }
        }
        riffdb_sim::fnv1a64(&bytes)
    }

    /// Number of distinct targets the plan touches.
    #[must_use]
    pub(crate) fn distinct_targets(&self) -> usize {
        let mut targets: Vec<u64> = self.commands.iter().map(|command| command.target).collect();
        targets.sort_unstable();
        targets.dedup();
        targets.len()
    }
}

// ---------------------------------------------------------------------------
// Generator determinism pins (SIM-C2 D1).
// ---------------------------------------------------------------------------

/// The reference config the golden-digest pin freezes. Deliberately exercises
/// every generator dimension: two-phase admissions, supersession, batching,
/// and a nonzero note-length bound.
#[cfg(test)]
const PIN_CONFIG: GeneratorConfig = GeneratorConfig {
    commands: 40,
    max_targets: 12,
    two_phase_percent: 35,
    reuse_percent: 45,
    batch_percent: 50,
    max_batch_len: 4,
    note_len_max: 300,
};

/// GOLDEN DIGEST for `(seed 0x51C2_0001, PIN_CONFIG)` under
/// [`WORKLOAD_GENERATOR_VERSION`] 1. This is the falsifier the same-seed and
/// cross-seed pins cannot provide: a change to the draw order or the plan
/// encoding changes THIS value even though both runs of the changed generator
/// still agree with each other. If this test reds after a deliberate
/// generator change, bump [`WORKLOAD_GENERATOR_VERSION`] and re-pin; never
/// update the constant without the version bump.
#[cfg(test)]
const PIN_GOLDEN_DIGEST: u64 = 0x5DDA_B964_6535_90FB;

#[test]
fn same_seed_twice_generates_identical_plans_and_digests() {
    let first = WorkloadPlan::generate(0x51C2_0001, PIN_CONFIG);
    let second = WorkloadPlan::generate(0x51C2_0001, PIN_CONFIG);
    assert_eq!(first, second, "same seed, same version: identical plans");
    assert_eq!(first.identity_digest(), second.identity_digest());
}

#[test]
fn different_seeds_generate_diverging_plans_and_digests() {
    let first = WorkloadPlan::generate(0x51C2_0001, PIN_CONFIG);
    let second = WorkloadPlan::generate(0x51C2_0002, PIN_CONFIG);
    assert_ne!(
        first.commands, second.commands,
        "different seeds must draw different command sequences"
    );
    assert_ne!(first.identity_digest(), second.identity_digest());
}

#[test]
fn the_golden_digest_pins_the_draw_order_and_encoding() {
    let plan = WorkloadPlan::generate(0x51C2_0001, PIN_CONFIG);
    assert_eq!(
        plan.identity_digest(),
        PIN_GOLDEN_DIGEST,
        "the plan identity for the pinned (seed, config) changed: the draw \
         order or encoding changed — bump WORKLOAD_GENERATOR_VERSION and \
         re-pin (never repin without the bump)"
    );
}

#[test]
fn the_generator_version_feeds_the_plan_identity() {
    let plan = WorkloadPlan::generate(0x51C2_0001, PIN_CONFIG);
    assert_ne!(
        plan.digest_with_version(u64::from(WORKLOAD_GENERATOR_VERSION)),
        plan.digest_with_version(u64::from(WORKLOAD_GENERATOR_VERSION) + 1),
        "a version bump must change every plan identity"
    );
}

#[test]
fn the_config_feeds_the_plan_identity() {
    let base = WorkloadPlan::generate(0x51C2_0001, PIN_CONFIG);
    let mut widened = PIN_CONFIG;
    widened.note_len_max += 1;
    let other = WorkloadPlan::generate(0x51C2_0001, widened);
    assert_ne!(
        base.identity_digest(),
        other.identity_digest(),
        "two configs that can generate different plans must never share an identity"
    );
}

/// Structural invariants every plan must satisfy, checked over many seeds:
/// sequential ordinals, every command committed exactly once, admits precede
/// their commits, batch members target-disjoint, chain depths consistent.
#[test]
fn generated_plans_satisfy_the_structural_invariants() {
    for seed in 0..64_u64 {
        let plan = WorkloadPlan::generate(0x51C2_1000 + seed, PIN_CONFIG);
        assert_eq!(plan.commands.len(), PIN_CONFIG.commands as usize);
        let mut committed = vec![false; plan.commands.len()];
        let mut admitted = vec![false; plan.commands.len()];
        for (position, command) in plan.commands.iter().enumerate() {
            assert_eq!(command.ordinal, position as u64 + 1, "sequential ordinals");
            assert!(command.note_len <= PIN_CONFIG.note_len_max);
            match command.supersedes {
                None => assert_eq!(command.chain_depth, 0),
                Some(prior) => {
                    let prior = &plan.commands[prior as usize];
                    assert_eq!(prior.target, command.target, "supersession stays on target");
                    assert_eq!(prior.chain_depth + 1, command.chain_depth);
                    assert!(prior.ordinal < command.ordinal);
                }
            }
        }
        let mut next_expected = 0_u32;
        for step in &plan.steps {
            match step {
                PlannedStep::AdmitPending { command } => {
                    assert!(!admitted[*command as usize], "one admit per command");
                    assert!(
                        !committed[*command as usize],
                        "admits precede their commits"
                    );
                    admitted[*command as usize] = true;
                    assert_eq!(
                        plan.commands[*command as usize].shape,
                        AdmissionShape::ExistingPending
                    );
                }
                PlannedStep::CommitBatch { commands } => {
                    assert!(!commands.is_empty());
                    assert!(commands.len() <= PIN_CONFIG.max_batch_len as usize);
                    let mut targets: Vec<u64> = commands
                        .iter()
                        .map(|index| plan.commands[*index as usize].target)
                        .collect();
                    targets.sort_unstable();
                    let before = targets.len();
                    targets.dedup();
                    assert_eq!(before, targets.len(), "batch members are target-disjoint");
                    for member in commands {
                        assert_eq!(*member, next_expected, "commands commit in ordinal order");
                        next_expected += 1;
                        assert!(!committed[*member as usize], "one commit per command");
                        committed[*member as usize] = true;
                        if plan.commands[*member as usize].shape == AdmissionShape::ExistingPending
                        {
                            assert!(
                                admitted[*member as usize],
                                "two-phase commits follow admits"
                            );
                        }
                    }
                }
            }
        }
        assert!(committed.iter().all(|done| *done), "every command commits");
    }
}

/// The pinned config's plan actually exercises every generator dimension —
/// the corpus-guard-style non-vacuity proof that the mix knobs draw real
/// variety rather than degenerate constants.
#[test]
fn the_pinned_config_exercises_every_generator_dimension() {
    let plan = WorkloadPlan::generate(0x51C2_0001, PIN_CONFIG);
    assert!(
        plan.commands
            .iter()
            .any(|command| command.shape == AdmissionShape::ExistingPending),
        "no two-phase admission drawn"
    );
    assert!(
        plan.commands
            .iter()
            .any(|command| command.shape == AdmissionShape::VacantTerminal),
        "no fused admission drawn"
    );
    assert!(
        plan.commands
            .iter()
            .any(|command| command.supersedes.is_some()),
        "no supersession drawn"
    );
    assert!(
        plan.commands.iter().any(|command| command.chain_depth >= 2),
        "no supersession chain deeper than one drawn"
    );
    assert!(
        plan.steps.iter().any(|step| matches!(
            step,
            PlannedStep::CommitBatch { commands } if commands.len() >= 2
        )),
        "no multi-command batch drawn"
    );
    let mut lengths: Vec<u64> = plan
        .commands
        .iter()
        .map(|command| command.note_len)
        .collect();
    lengths.sort_unstable();
    lengths.dedup();
    assert!(
        lengths.len() >= 8,
        "the value-size distribution drew too few distinct note lengths"
    );
    assert!(
        plan.distinct_targets() >= 2,
        "the contention profile drew no target variety"
    );
}

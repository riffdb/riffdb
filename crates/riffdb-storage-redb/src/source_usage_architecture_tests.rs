//! Closed raw durable-writer architecture inventory. No mutation authority.
// req: STO-012, PRJ-004, REC-001
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

const MAX_FILES: usize = 2048;
const MAX_FILE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 32 * 1024 * 1024;

fn rust_files(root: &Path, files: &mut Vec<PathBuf>, depth: u16, entries: &mut usize) {
    assert!(depth <= 32);
    for entry in fs::read_dir(root).unwrap() {
        *entries += 1;
        assert!(*entries <= 4096);
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        assert!(
            !kind.is_symlink(),
            "native source tree cannot hide caller aliases"
        );
        if kind.is_dir() {
            rust_files(&entry.path(), files, depth + 1, entries);
        } else if entry.path().extension().is_some_and(|value| value == "rs") {
            assert!(kind.is_file());
            files.push(entry.path());
            assert!(files.len() <= MAX_FILES);
        }
    }
}
fn quoted(source: &[u8], mut position: usize, quote: u8) -> usize {
    while position < source.len() {
        match source[position] {
            b'\\' => position += 2,
            value if value == quote => return position + 1,
            _ => position += 1,
        }
    }
    panic!("unterminated Rust quoted literal")
}
fn raw_end(source: &[u8], position: usize) -> Option<usize> {
    let mut cursor = position;
    if matches!(source.get(cursor), Some(b'b' | b'c')) {
        cursor += 1;
    }
    if source.get(cursor) != Some(&b'r') {
        return None;
    }
    cursor += 1;
    let start = cursor;
    while source.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    let hashes = cursor - start;
    if source.get(cursor) != Some(&b'"') {
        return None;
    }
    assert!(hashes <= 255);
    cursor += 1;
    while cursor < source.len() {
        if source[cursor] == b'"'
            && source
                .get(cursor + 1..cursor + 1 + hashes)
                .is_some_and(|tail| tail.iter().all(|value| *value == b'#'))
        {
            return Some(cursor + 1 + hashes);
        }
        cursor += 1;
    }
    panic!("unterminated Rust raw literal")
}
fn character_end(source: &str, position: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(position) != Some(&b'\'') {
        return None;
    }
    let mut end = position + 1;
    if bytes.get(end) == Some(&b'\\') {
        end += 1;
        match bytes.get(end)? {
            b'x' => end += 3,
            b'u' => {
                end += 1;
                if bytes.get(end) != Some(&b'{') {
                    return None;
                }
                end += 1;
                while bytes.get(end).is_some_and(|value| *value != b'}') {
                    end += 1;
                }
                if bytes.get(end) != Some(&b'}') {
                    return None;
                }
                end += 1;
            }
            _ => end += 1,
        }
    } else {
        let value = source.get(end..)?.chars().next()?;
        if matches!(value, '\'' | '\n') {
            return None;
        }
        end += value.len_utf8();
    }
    (bytes.get(end) == Some(&b'\'')).then_some(end + 1)
}
fn lexical_with_literals(source: &str, include_literals: bool) -> Vec<&str> {
    let bytes = source.as_bytes();
    let mut position = 0;
    let mut tokens = Vec::new();
    while position < bytes.len() {
        if bytes[position..].starts_with(b"//") {
            while position < bytes.len() && bytes[position] != b'\n' {
                position += 1;
            }
        } else if bytes[position..].starts_with(b"/*") {
            position += 2;
            let mut depth = 1_u32;
            while depth > 0 {
                assert!(position < bytes.len(), "unterminated Rust comment");
                if bytes[position..].starts_with(b"/*") {
                    depth = depth.checked_add(1).unwrap();
                    position += 2;
                } else if bytes[position..].starts_with(b"*/") {
                    depth -= 1;
                    position += 2;
                } else {
                    position += 1;
                }
            }
        } else if let Some(end) = raw_end(bytes, position) {
            if include_literals {
                tokens.push(&source[position..end]);
            }
            position = end;
        } else if bytes[position] == b'"' {
            let end = quoted(bytes, position + 1, b'"');
            if include_literals {
                tokens.push(&source[position..end]);
            }
            position = end;
        } else if let Some(end) = character_end(source, position) {
            if include_literals {
                tokens.push(&source[position..end]);
            }
            position = end;
        } else if bytes[position].is_ascii_whitespace() {
            position += 1;
        } else {
            let start = position;
            if bytes[position].is_ascii_alphabetic() || bytes[position] == b'_' {
                position += 1;
                while bytes
                    .get(position)
                    .is_some_and(|value| value.is_ascii_alphanumeric() || *value == b'_')
                {
                    position += 1;
                }
            } else {
                // Unicode punctuation/comments cannot split a UTF-8 slice.
                position += source[position..].chars().next().unwrap().len_utf8();
            }
            tokens.push(&source[start..position]);
        }
    }
    tokens
}
fn lexical(source: &str) -> Vec<&str> {
    lexical_with_literals(source, false)
}
fn body_digest(source: &str) -> String {
    let mut hash = Sha256::new();
    for token in lexical_with_literals(source, true) {
        // Rustfmt's optional trailing commas do not enlarge a physical writer
        // recipe. Literals, identities, every call and all other syntax remain.
        if token == "," {
            continue;
        }
        hash.update((token.len() as u64).to_be_bytes());
        hash.update(token.as_bytes());
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn callers(source: &str) -> BTreeMap<String, usize> {
    let tokens = lexical(source);
    let mut current = None;
    let mut pending = None;
    let mut stack = Vec::new();
    let mut rows = BTreeMap::new();
    for (index, token) in tokens.iter().enumerate() {
        match *token {
            "fn" if tokens.get(index + 1).is_some_and(|name| {
                name.as_bytes()
                    .first()
                    .is_some_and(|value| value.is_ascii_alphabetic() || *value == b'_')
            }) =>
            {
                pending = tokens.get(index + 1).copied()
            }
            "{" => {
                stack.push(current);
                if let Some(function) = pending.take() {
                    current = Some(function);
                }
            }
            "}" => current = stack.pop().expect("balanced Rust body"),
            ";" => pending = None,
            "commit_durable" if index == 0 || tokens[index - 1] != "fn" => {
                let function = current.expect("raw writer reference outside function inventory");
                *rows.entry(function.to_owned()).or_default() += 1;
            }
            _ => {}
        }
    }
    rows
}
fn function_bodies(source: &str) -> BTreeMap<String, Vec<String>> {
    let tokens = lexical(source);
    let mut pending = None;
    let mut stack = Vec::new();
    let mut bodies: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (index, token) in tokens.iter().enumerate() {
        match *token {
            "fn" if tokens.get(index + 1).is_some_and(|name| {
                name.as_bytes()
                    .first()
                    .is_some_and(|value| value.is_ascii_alphabetic() || *value == b'_')
            }) =>
            {
                let offset = token.as_ptr() as usize - source.as_ptr() as usize;
                pending = Some((tokens[index + 1], offset));
            }
            "{" => stack.push(pending.take()),
            "}" => {
                if let Some((name, start)) = stack.pop().expect("balanced Rust body") {
                    let end = token.as_ptr() as usize - source.as_ptr() as usize + 1;
                    let digest = body_digest(&source[start..end]);
                    bodies.entry(name.to_owned()).or_default().push(digest);
                }
            }
            ";" => pending = None,
            _ => {}
        }
    }
    for values in bodies.values_mut() {
        values.sort();
    }
    bodies
}

#[test]
fn source_raw_writer_lexer_detects_methods_ufcs_references_and_ignores_literals() {
    let text = r###"fn owner<'a>() { value.commit_durable(x); SharedRedb::commit_durable(y);
        let method=SharedRedb::commit_durable; let c='x'; let escaped='\'';
        let unicode='é'; let lifetime:&'a str="commit_durable(x)";
        /* outer /* commit_durable() */ nested */ // commit_durable()
        let raw=r##"commit_durable(x)"##;
        fn nested(){other.commit_durable(z);} value.commit_durable(q);
    }"###;
    assert_eq!(
        callers(text),
        BTreeMap::from([("nested".to_owned(), 1), ("owner".to_owned(), 4)])
    );
    let original = "fn owner(){let table=\"derived\"; shared.commit_durable(tx);}";
    let enlarged =
        "fn owner(){let table=\"projection_source_rows/v1\"; shared.commit_durable(tx);}";
    assert_eq!(callers(original), callers(enlarged));
    assert_ne!(
        function_bodies(original),
        function_bodies(enlarged),
        "an existing caller cannot silently expand its Source absence recipe"
    );
}

#[test]
fn raw_durable_writers_match_reviewed_source_absence_and_fresh_guard_inventory() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    let mut entries = 0;
    rust_files(&root, &mut files, 0, &mut entries);
    files.sort();
    let mut total = 0_usize;
    let mut actual = Vec::new();
    for path in files {
        let length = usize::try_from(fs::metadata(&path).unwrap().len()).unwrap();
        total = total.checked_add(length).unwrap();
        assert!(length <= MAX_FILE_BYTES && total <= MAX_TOTAL_BYTES);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len(), length);
        let source = std::str::from_utf8(&bytes).unwrap();
        let relative = path
            .strip_prefix(&root)
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\\', "/");
        let bodies = function_bodies(source);
        for (function, count) in callers(source) {
            let digest = bodies
                .get(&function)
                .expect("physical raw writer body")
                .clone();
            actual.push((relative.clone(), function, count, digest));
        }
    }
    // PathBuf sorts path components, while this public-relative inventory uses
    // String tuple ordering. Compare the same canonical row order exactly.
    actual.sort();
    let expected = reviewed_inventory();
    assert_eq!(
        actual, expected,
        "raw durable caller family changed; review its exact Source absence/guard before admitting it"
    );
}

fn reviewed_inventory() -> Vec<(String, String, usize, Vec<String>)> {
    // Filled only from the reviewed actual source composition. Test callers are
    // included, so cfg parsing cannot hide a newly added production bypass.
    REVIEWED
        .iter()
        .map(|(path, function, count, digests)| {
            (
                (*path).to_owned(),
                (*function).to_owned(),
                *count,
                digests.iter().map(|value| (*value).to_owned()).collect(),
            )
        })
        .collect()
}
const REVIEWED: &[(&str, &str, usize, &[&str])] = &[
    (
        "administration.rs",
        "submitted_service_audit_refuses_partial_malformed_and_unknown_v3_metadata",
        1,
        &["65c845bbb24f869b9df8ae598cad59bf02ac546b787d9e373b70b88b13645aa5"],
    ),
    (
        "administration_v3_retention_tests.rs",
        "changelog_history_reclamation_respects_checkpoint_and_fences",
        1,
        &["73733badbd34113f194417c96ad11bdc9b1696e7f3432062f985e3602b79ad6f"],
    ),
    (
        "administration_v3_retention_tests.rs",
        "control_only_publication_pins_exact_history_without_advancing_application_frontier",
        1,
        &["e9b5e344c221821170337da7099617b1edbdba1d77bc1c925010d515d21ce73f"],
    ),
    (
        "application_export_ledger/tests.rs",
        "open",
        1,
        &["b4c49d12b157379fa266d58588dbb4369662453cef7d5ce1bae3f42981a11c02"],
    ),
    (
        "changelog_bootstrap_attachment_tests.rs",
        "bootstrap_attachment_at_the_hold_population_ceiling_does_not_need_an_extra_slot",
        1,
        &["a1eca32728f6e2cf90134ef8eb7d2f0f7af0e7bfa3abe0f918282d39bac6d7cb"],
    ),
    (
        "changelog_bootstrap_attachment_tests.rs",
        "follower_acknowledgement_cannot_replace_a_foreign_or_miskeyed_prior_hold",
        1,
        &["8863ca4a272e1cf1be700cdcb0530b184b7e090af22530f6c778a8ffa8b18d2b"],
    ),
    (
        "changelog_bootstrap_custody_tests.rs",
        "bootstrap_custody_cancellation_preserves_shared_candidate_capacity_and_pin_bound",
        1,
        &["9f43c0f74acdd3a79fee6b56149eff167fdb10cfbee8130aa46e02b921921f5f"],
    ),
    (
        "changelog_bootstrap_custody_tests.rs",
        "bootstrap_custody_refuses_retired_changed_corrupt_and_unclassified_evidence",
        1,
        &["5a863fdd333dc67bcf340fd8341dfcad7f1642203b703d9454b1a3004b5a3c27"],
    ),
    (
        "changelog_follower_acknowledgement.rs",
        "follower_acknowledgement_refuses_incomplete_stale_and_unclassified_preparation",
        1,
        &["2b28b11a957f63d9adb5385daee4df611fbb0d0db71e802d47b7725827cbd25e"],
    ),
    (
        "changelog_source_accounting_holds_tests.rs",
        "accounting_hold_validation_retired_policy_requires_its_genuine_release_evidence",
        1,
        &["d9f2bf9b13a0d592ab4ae9c671dbe44353f87b06cf5071fb0116d925410f4ec4"],
    ),
    (
        "changelog_source_accounting_holds_tests.rs",
        "source_accounting_refuses_changed_or_incomplete_floor_evidence",
        1,
        &["b669a102eb7603c33a29c69cba3b084368fbcfa37a2e8823f2b4a4d5fbf75d83"],
    ),
    (
        "changelog_source_checkpoint_license.rs",
        "checkpoint_license_still_refuses_a_missing_receipt_inside_claimed_checkpoint_coverage",
        1,
        &["dc9eca5e3ef961ad7a8b40e8d66222a4cf7897d7fb5b043c0ed143c956074960"],
    ),
    (
        "changelog_source_continuation_tests.rs",
        "changed_receipt_preimages_and_gaps_refuse_continuation_and_release_failed_pins",
        1,
        &["c69fa68c0ab729d52363a0d0c60b279827ccb9da34cfa26809d0070ce98815b2"],
    ),
    (
        "changelog_source_continuation_tests.rs",
        "unclassified_publication_invalidates_a_proof_even_when_source_roots_are_equal",
        1,
        &["599455fcea772698699be4b3d04d0e16ecd74ab7235948e2453bb62f825d124c"],
    ),
    (
        "changelog_source_control_refusal_tests.rs",
        "exhaust_allocator",
        1,
        &["f96cdec58bc347af5c4f1180504df08e716f4d5c260de701b4542aaa04224742"],
    ),
    (
        "changelog_source_control_refusal_tests.rs",
        "exhausted_control_allocator_refuses_hold_and_prune_before_any_mutation",
        1,
        &["308d7d417f097f7d97ee501e3c700b2a578553d3059be5f1c102f0c832c7e8b9"],
    ),
    (
        "changelog_source_control_refusal_tests.rs",
        "retained_control_handles_refuse_after_clean_or_writer_fencing",
        1,
        &["ee1493d1b9d3ba7bdabcd40eb206049649c6da8c66b8bc2b378277f585c929f0"],
    ),
    (
        "changelog_source_control_refusal_tests.rs",
        "source_hold_population_bound_includes_all_kinds_and_refuses_before_allocation",
        2,
        &["362073777145a9ad4c4fb7fb02b19e3ec9efd2897b6f13c2bcae961344458635"],
    ),
    (
        "changelog_source_control_refusal_tests.rs",
        "source_hold_refuses_substitution_foreign_epoch_regression_and_unknown_rows_without_writes",
        1,
        &["415e6b9779847c4129160dbf0d4f8de86a1e4195bb20b2570f9fc63ad0210494"],
    ),
    (
        "changelog_source_prefix_refresh.rs",
        "prefix_refresh_publication_during_session_setup_discards_the_candidate",
        1,
        &["1c704a4753670482e5fab00006044aae9fb82262ca1433f299a929a7a554e0e2"],
    ),
    (
        "changelog_v3_control_tests.rs",
        "initialize_store",
        1,
        &["f1343a97cd56975e8897e3ce139139dfb1a2b26b5ec666c1232b705571aaae80"],
    ),
    (
        "derived/tests/follower_projection.rs",
        "setup",
        1,
        &["15384a26374186310c6c20da589d57093cb581d13bffaaac06976776c7f72718"],
    ),
    (
        "extent_reclamation.rs",
        "extent_reader_tracking_is_bounded_and_releases_dropped_roots",
        1,
        &["6ed9dcf7afff801b90344a6f227f6ffcc29a5341538a19a96d0179bf7b83ddee"],
    ),
    (
        "fixtures.rs",
        "contract_migration_stage_ports_fixture",
        1,
        &["25ef3ea1933222c7324e55d86e29f4686c3d6d2d1ebf7be80f82d2588560929c"],
    ),
    (
        "primary_fence_proof_tests.rs",
        "primary_fence_source_evidence_keeps_pruned_registration_evidence_but_requires_retained_candidate",
        1,
        &["e3750e7c120e32a0e6e4c1b6801372d32bdd1513ef5a93f0fd2e961125aff05c"],
    ),
    (
        "primary_fence_selection_tests.rs",
        "primary_fence_selection_never_masks_corrupt_source_as_request_refusal",
        1,
        &["70140a247af4834449312fbd2a45bb685f204305a792ef012b0bcdc6ebc9e977"],
    ),
    (
        "query.rs",
        "missing_entities_table_on_point_query_is_backend_integrity",
        1,
        &["8a9988ce913fdc3c7b3bdcf4ba136e1421ae5d1b70fbba56736b59300319c64c"],
    ),
    (
        "replication_administration_write_tests.rs",
        "fixture",
        1,
        &["13d67933249f5f12fed3c46b13281b43568ce7b5ac911cebb365b8981d019533"],
    ),
    (
        "replication_administration_write_tests.rs",
        "registration_capacity_refuses_new_identity_but_preserves_legacy_adoption",
        1,
        &["6e151261081cfb44d90bdc5742eec1583b9358feef318da846310c9f3246ff14"],
    ),
    (
        "replication_policy_ack_tests.rs",
        "attached_fixture",
        1,
        &["c74ddfb9f96dd6e3e2f3b6a8f9276dfff2a4ae0152acd141dccbb1e21914d736"],
    ),
    (
        "replication_policy_bootstrap_tests.rs",
        "retired_artifact_custody_requires_exact_release_evidence",
        1,
        &["30e2235adccc7e43780bb432e8d7a7c2304c5df81a6d68e9946ffba9bdf4de28"],
    ),
    (
        "replication_registration_links_tests.rs",
        "advisory_progress_refuses_a_retired_policy_stored_under_a_foreign_key",
        1,
        &["93c71ad083caccec97e9413f0fb912823f81eebbcfd922f1e2711e739c380fc2"],
    ),
    (
        "replication_registration_links_tests.rs",
        "complete_source_validation_uses_one_pin_and_refuses_erased_registration_evidence",
        1,
        &["36bbf0299b4cc7e9cad0929d219559be48ba404c5f7495ce7df0066607fd4782"],
    ),
    (
        "replication_registration_links_tests.rs",
        "retired_tombstone_retains_audit_links_without_pinning_old_physical_receipts",
        1,
        &["f95ff10c9c4e8e6668ceb614062580b26f75a3f176c19ed31e762e532887a318"],
    ),
    (
        "replication_registration_links_tests.rs",
        "store_record",
        1,
        &["9d34f34cb7e80a4166fab73ac88e8d104dbedd95da0e1524d3c284258af301e5"],
    ),
    (
        "retention/eligible_bytes_tests.rs",
        "logical_bytes_corrupt_locator_refuses_instead_of_crediting_partial_retained_work",
        1,
        &["334979754c1c6f47f0ff9b9f73ed23cc9142af9dc779aebfe2ce4ac8a8971e88"],
    ),
    (
        "retention/logical_observer_tests.rs",
        "logical_bytes_and_online_retention_accept_pre_restore_segments_and_tombstones",
        2,
        &["7998a67567c9f190afc42d162bb7b87fca05058218a7da3e9c303cf8cc61aeab"],
    ),
    (
        "retention/logical_observer_tests.rs",
        "logical_bytes_gap_refuses_and_owner_retirement_releases_metric_work",
        1,
        &["85bfcd0003beb6d76c67c718aa0093122d3ee6549932d17e943f4912f8704fae"],
    ),
    (
        "retention/logical_observer_tests.rs",
        "logical_bytes_preserve_original_operator_hold_and_refuse_unknown_prefix_as_zero",
        2,
        &["056777d5c7e3951eaf9b78626341b3639fcc170285036b338fd1e84944c0182c"],
    ),
    (
        "retention/logical_observer_tests.rs",
        "populate",
        1,
        &["11aeecb1309b53d5b60d9f8d1801605eda2e9a7febee1858b844d5c05d9f34b8"],
    ),
    (
        "retention/logical_observer_tests.rs",
        "segment_fixture",
        1,
        &["f0916e5465d1bf568ddc9cb962acebf51d2b26563cee9dbdf275f66f08512da9"],
    ),
    (
        "startup/background_history.rs",
        "completed_prefix_requires_the_exact_original_publication_even_with_equal_history",
        1,
        &["cb489dc2ba85501600b2fc48acd10f6050bd5c91a4bdf910c3a2894516826a17"],
    ),
    (
        "startup/background_history.rs",
        "prefix_refresh_discards_stale_structural_and_catalog_pages",
        1,
        &["420401714cfa924231e6cf7a00b0a450d99c21fe288351630668c4c9988df1b9"],
    ),
    (
        "startup/index_migration_backend.rs",
        "apply_index_migration_substitution_fixture",
        1,
        &["23ca2b97dd0f54ab7433201fd4b3fe94464053a874505d549f44f7f4e6034d0b"],
    ),
    (
        "startup/offline_logical_proof_tests.rs",
        "complete_logical_proof_refuses_a_later_unclassified_native_commit",
        1,
        &["4d6c36dbef92d8dfd1ac1c309f884f1f0a8e8d9c03c054f7eae970afbddd4688"],
    ),
    (
        "startup/offline_logical_proof_tests.rs",
        "complete_logical_reader_handoff_refuses_a_changed_epoch_catalog_or_cancellation",
        1,
        &["e6a6553a1435172570ef1c2c833043e0d2374c4c2e4baf650a870f068d882a1e"],
    ),
    (
        "startup/offline_logical_proof_tests.rs",
        "complete_logical_reader_keeps_original_bytes_and_refuses_later_mutation_or_cancellation",
        1,
        &["5a81360cd1941c5834a75d35f28243f240581cc539d1949baf69a823bc272408"],
    ),
    (
        "startup/query_module_retention_tests.rs",
        "cold_query_module_read_still_refuses_substituted_activation_audit",
        1,
        &["f43524f00edd2a8938a0f1c4b7411fcb80a5e2cacfab00656b943e60b0455421"],
    ),
    (
        "startup/tests.rs",
        "same_count_value_drift_is_rejected_by_the_session_commit_epoch",
        2,
        &["c9bb35f5f5d42b3530d82b6a29971666450a23feaec24ff412b719590927f4b9"],
    ),
    (
        "startup_v3_activation.rs",
        "install_compatible_history",
        1,
        &["56fb997d164f77af69708d80b577017413baf26c37f8027b2db97ee4ba9e064f"],
    ),
    (
        "startup_v3_tests.rs",
        "follower_uses_existing_structural_and_catalog_validators_without_local_receipts",
        1,
        &["09992a4f7ac675a3eb6c90be7ede81977653d4064cff52040e988abac117d1d0"],
    ),
    (
        "store.rs",
        "advance_dirty_lifecycle_before_activation",
        1,
        &["7237beabf1a040311850893897f5bf7b2626b70d23689aca09cad3104111722a"],
    ),
    (
        "store.rs",
        "backfill_vector_evidence_index",
        1,
        &["c0a4170b18c0b08d5a52cf3d21c175ce9cb7999048f45c35204a93d0961e9b9b"],
    ),
    (
        "store.rs",
        "backfill_vector_observations_and_health",
        1,
        &["3663d6582b23aaaf8e3ac8487165f81dbbd4ce1dfa67b94fa654989a6745be8d"],
    ),
    (
        "store.rs",
        "ensure_audit_by_request_table",
        1,
        &["5d240fc4af0c07e95d8b56688762f3329fa49293a084a7cbad782e50b807938c"],
    ),
    (
        "store.rs",
        "ensure_current_storage_format",
        1,
        &["7269fcac72110239f4a121b1ae1cc048103e2334779799fb8ffce381f4982dc0"],
    ),
    (
        "store.rs",
        "ensure_event_routes_table",
        1,
        &["e19447bff056e813943a4407cc96db2143f333bf0af350a0b397f06fc8fdc698"],
    ),
    (
        "store.rs",
        "install_application_export_operation_table",
        1,
        &["6bd0891eb205c2dd9d3717898008b61a38d0867bbbb67713998751fc3a0d1854"],
    ),
    (
        "store.rs",
        "install_application_installation_campaign_table",
        1,
        &["6347a2a5fc6322dd381fe922cf2bf005b6f0df69d6af240355ce514dc2053c00"],
    ),
    (
        "store.rs",
        "install_columnar_projection_controls_table",
        1,
        &["11bbd139c14ac19f3ff1bda4e9cae9689503a4e6dd82457bd859a7b4d6963888"],
    ),
    (
        "store.rs",
        "install_contract_migration_tables",
        1,
        &["cd744fb886b95029be7874742c38b8b16559fe126069495829c5c21c104b8f57"],
    ),
    (
        "store.rs",
        "install_history_tombstones_table",
        1,
        &["adc7bad7f0f172738fb41e53a2fb6999a1c9bb1e1429e402f68fcb44e28b47b1"],
    ),
    (
        "store.rs",
        "install_reactive_consumer_tables",
        1,
        &["166b0e1de6b77f007f6e70e87989cbf19cebc0c3b770b73290f93d6eb3ef921b"],
    ),
    (
        "store.rs",
        "install_validated_prefix_entity_heads_table",
        1,
        &["3872bc10942dad0d3b6ccf2f38484adb52f05a647033b36e13a1b5e42fdfc504"],
    ),
    (
        "store.rs",
        "install_vector_evidence_table",
        1,
        &["db85eb90020dbcba8fae26513923ae0433b9b69f68e872f028be60a24157fb36"],
    ),
    (
        "store.rs",
        "install_vector_observations_table",
        1,
        &["ea2e1e5f3b24ea1381bf37635222965a9e3448b1b083c10d77e10093c5d77c61"],
    ),
    (
        "store.rs",
        "install_vector_projection_controls_table",
        1,
        &["72ed9d2eb9063d1193ec1b06bcda99f64608aeb8591e6cd914bd6cc17bc34a11"],
    ),
    (
        "store.rs",
        "migrate_audit_request_index",
        1,
        &["2ddf3b1b763e4896bbbb76548464061a5cff18d4ca752fcd8c10fa4f2283d5cd"],
    ),
    (
        "store.rs",
        "migrate_byte_table",
        1,
        &["a5ef03443257fe2209ba428174fa96919b058c55436c023a98364fcac75db338"],
    ),
    (
        "store.rs",
        "migrate_commit_entity_references",
        1,
        &["838a313702929d299cca3eaa63d2013800e6ca0a82924198ce20fb75f847e6ce"],
    ),
    (
        "store.rs",
        "migrate_commit_event_references",
        1,
        &["97eb8bba2ff7aa08ad8ca430291fc757e8f74b89d851c7d48ea33e62cc579c24"],
    ),
    (
        "store.rs",
        "migrate_entity_transition_heads",
        1,
        &["354ab631fe06d5df4d6eb984b398229acbdd654bfac814a2071380398028fd57"],
    ),
    (
        "store.rs",
        "migrate_event_routes",
        1,
        &["65d1d87b0daa8293288617a10397952c2496395126307acc7e4a8bd64f063644"],
    ),
    (
        "store.rs",
        "migrate_history_incarnation",
        1,
        &["a59fe7475fdc05217b771eed0a1edd8fc822e3461b26e29a851e432b2fd1190b"],
    ),
    (
        "store.rs",
        "migrate_outbox_event_references",
        1,
        &["28bcc083a0df6f7cd8dfdf37b94f9373bb4699b066bef6e399d6aa9d062649bf"],
    ),
    (
        "store.rs",
        "migrate_string_table",
        1,
        &["6c69b7888d2cd75e86e91dd239bf097729c8eeb1eed58759003c9f7584b33788"],
    ),
    (
        "store.rs",
        "publish_record_registry",
        1,
        &["63bbdab64af49c31e39da2b728a6425d430dfef831306e080b7922470134a614"],
    ),
    (
        "store/initialization.rs",
        "initialize_source",
        1,
        &["7bf52816cac7c0f9913300a6b4507d58783fcdadda09f84e6ca4bc4f2b0447a6"],
    ),
    (
        "store/locator_publication_tests.rs",
        "locator_builder_current_ownership_requires_exact_capability_and_stable_epoch",
        1,
        &["6a78cc9e48d35cda7e288353755b162ac82e57f7de67e25dd2e31c68a5ef7d9e"],
    ),
    (
        "store/source_reclamation_drain_tests.rs",
        "off_gate_validation_corruption_releases_proof_and_reader_without_a_writer_turn",
        1,
        &["320543144ef391e8d120c2faf9b6825db0f9434ef8cb26c834cf8deb58f663b3"],
    ),
    (
        "store/tests.rs",
        "durable_root_capture_retries_the_engine_visible_before_identity_window",
        1,
        &["b6e74a1bd5d960ac6fbd4b22c5abbd8fffe94042980fdfa9e5c5cc783bb68a45"],
    ),
    (
        "store/transient_access.rs",
        "assert_payload_keeps_captured_authority",
        1,
        &["32db120662d5cd3a21bb4d1c8086ec399ab316615f41e35169c4b12205dc2c95"],
    ),
    (
        "store/transient_access.rs",
        "install_payload_fixture",
        1,
        &["4000449dd2f9f1253e080c49d857bbf78399c5f9196e65127cfe55f9c961c395"],
    ),
    (
        "store_changelog_lifecycle_tests.rs",
        "follower_restart_lifecycle_must_preserve_the_exact_upstream_successor",
        2,
        &["57f75c8c656b4ebaad07f5786900c2b0000b71243fb587251a2e0a50c9686bad"],
    ),
    (
        "store_export_ledger.rs",
        "install_empty_legacy_table",
        1,
        &["0b1e87e637eb388c9bbce5cc435456141057598fb7ead8f1750a131361dd7a40"],
    ),
    (
        "store_follower_tests.rs",
        "fixture",
        1,
        &["4f8c8f0e6a870005388528c5f0124e43bfc49bd456e4dcc4c1196bcf18c55590"],
    ),
    (
        "store_graceful_close.rs",
        "write_final_clean_close_lifecycle_after_barrier",
        1,
        &["ada3b4294937af695702fa4f5f671a0900918eaf945255f669bd5122b48080cd"],
    ),
    (
        "validated_prefix.rs",
        "write_checkpoint_with_completion",
        1,
        &["b373f0d69e07b7c329d191013c75ebd518fe70bfa7650d97e299e41d340c8236"],
    ),
    (
        "vector_projection_control.rs",
        "columnar_control_fresh_rebuild_ignores_every_legacy_selector",
        1,
        &["ff0d4dd4c174e73430943baede7e42e928b99760e78be6e8bb6ec039247a74c6"],
    ),
];

// Any raw WriteTransaction/open_table/open_system_table helper that escapes the
// direct commit_durable caller body digest could otherwise acquire a second
// table recipe without touching commit_durable. A helper file change therefore
// requires renewing its reviewed Source-absence/Fresh-guard argument.
#[test]
fn raw_source_absence_recipe_files_match_reviewed_native_helpers() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut total = 0_usize;
    for (relative, expected) in REVIEWED_RECIPE_FILES {
        let path = root.join(relative);
        let metadata = fs::symlink_metadata(&path).unwrap();
        assert!(metadata.is_file() && !metadata.file_type().is_symlink());
        let length = usize::try_from(metadata.len()).unwrap();
        total = total.checked_add(length).unwrap();
        assert!(length <= MAX_FILE_BYTES && total <= MAX_TOTAL_BYTES);
        let bytes = fs::read(path).unwrap();
        assert_eq!(bytes.len(), length);
        let source = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(
            &body_digest(source),
            expected,
            "reviewed native raw recipe helper changed: {relative}"
        );
    }
}

const REVIEWED_RECIPE_FILES: &[(&str, &str)] = &[
    (
        "changelog_v3_activation.rs",
        "7dbccb0bc0fb216789120e3fb781a3963d7480115fcc257a7eb55bf0ef1abf91",
    ),
    (
        "changelog_v3_write.rs",
        "3249dd2d51b4981ab52af9fe1dc45c3cd863d833b8ecbbf550c73d5c286a386b",
    ),
    (
        "layout.rs",
        "d373207fbe789e11690027536cd1a013c37dfba5ed82bd4f8367f7f7f2f2aeac",
    ),
    (
        "native_source_fresh.rs",
        "574874aa0673975b198eeb0a7aae33061bf18d2e0d3f1624bada2b46753ac49d",
    ),
    (
        "source_usage_fresh.rs",
        "e1b18a34c5c0475ddbcfbc05127abca37b639929adfdc3570b7a8cc20ecc637a",
    ),
    (
        "startup_v3_activation.rs",
        "80649cfff6eb2e7e2d76a0147f5504187b15c3d85b57dcf5a32fae3c957f7d1d",
    ),
    (
        "store.rs",
        "076d622a53adbf917967db3679b7b7860c143f1dd00bcb1755811c4bdb882670",
    ),
    (
        "store/initialization.rs",
        "752c055e2e14f13e96cb3b5ef5c99fa468c1e22f5be9ad30a309aa3a176e43f0",
    ),
    (
        "store_export_ledger.rs",
        "6d0aaf18756a09d063c8de80aad8f94d8d7438906d462c3acd5a7cbea6c07760",
    ),
    (
        "store_graceful_close.rs",
        "10165e9f63711b8528d5ad4475e7b6433bb4a4f3edde1950023748de99aec3a3",
    ),
    (
        "validated_prefix.rs",
        "80a85103eb58dbd57955e35394a0b8c24e4a9caf0074060cf118c64316e67ef9",
    ),
];

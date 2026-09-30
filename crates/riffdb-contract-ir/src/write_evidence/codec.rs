use super::*;
use crate::codec::{Reader, Writer};
use crate::format_registry::{write_evidence as tags, write_location as locations};

pub(crate) fn encode(writer: &mut Writer, proofs: &[WriteProof]) -> Result<(), IrValidationError> {
    writer.u32(proofs.len() as u32)?;
    for proof in proofs {
        match proof.location {
            WriteLocation::Instruction(index) => {
                writer.u8(locations::INSTRUCTION)?;
                writer.u32(index)?;
            }
            WriteLocation::Decision {
                decision,
                arm,
                instruction,
            } => {
                writer.u8(locations::DECISION)?;
                writer.u32(decision)?;
                writer.u32(arm)?;
                writer.u32(instruction)?;
            }
        }
        writer.u32(proof.evidence.len() as u32)?;
        for evidence in &proof.evidence {
            match evidence {
                WriteEvidence::Initialization => writer.u8(tags::INITIALIZATION)?,
                WriteEvidence::StateDerived => writer.u8(tags::STATE_DERIVED)?,
                WriteEvidence::BusinessCheck { requirement } => {
                    writer.u8(tags::BUSINESS_CHECK)?;
                    writer.u32(*requirement)?;
                }
                WriteEvidence::CallerVersionFence {
                    requirement,
                    advancement,
                } => {
                    writer.u8(tags::CALLER_VERSION_FENCE)?;
                    writer.u32(*requirement)?;
                    writer.u32(*advancement)?;
                }
                WriteEvidence::Overwrite => writer.u8(tags::OVERWRITE)?,
            }
        }
    }
    Ok(())
}

pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Vec<WriteProof>, IrValidationError> {
    let count = reader.u32()? as usize;
    crate::error::checked_len("write proofs", count, MAX_COMMAND_ITEMS)?;
    let mut proofs = Vec::with_capacity(count);
    let mut total = 0usize;
    for _ in 0..count {
        let location = match reader.u8()? {
            locations::INSTRUCTION => WriteLocation::Instruction(reader.u32()?),
            locations::DECISION => WriteLocation::Decision {
                decision: reader.u32()?,
                arm: reader.u32()?,
                instruction: reader.u32()?,
            },
            _ => return Err(invalid()),
        };
        let count = reader.u32()? as usize;
        total = total.checked_add(count).ok_or_else(invalid)?;
        crate::error::checked_len("write evidence", total, MAX_COMMAND_ITEMS)?;
        let mut evidence = Vec::with_capacity(count);
        for _ in 0..count {
            evidence.push(match reader.u8()? {
                tags::INITIALIZATION => WriteEvidence::Initialization,
                tags::STATE_DERIVED => WriteEvidence::StateDerived,
                tags::BUSINESS_CHECK => WriteEvidence::BusinessCheck {
                    requirement: reader.u32()?,
                },
                tags::CALLER_VERSION_FENCE => WriteEvidence::CallerVersionFence {
                    requirement: reader.u32()?,
                    advancement: reader.u32()?,
                },
                tags::OVERWRITE => WriteEvidence::Overwrite,
                _ => return Err(invalid()),
            });
        }
        proofs.push(WriteProof { location, evidence });
    }
    Ok(proofs)
}

pub(crate) fn validate(
    plan: CommandPlan,
    schema: &SchemaIr,
    proofs: &[WriteProof],
) -> Result<CommandPlan, IrValidationError> {
    let mut associations = BTreeMap::new();
    for proof in proofs {
        for evidence in &proof.evidence {
            if let WriteEvidence::BusinessCheck { requirement } = evidence
                && associations.insert(proof.location, *requirement).is_some()
            {
                return Err(invalid());
            }
        }
    }
    let plan = plan.with_write_proofs(&associations, schema)?;
    if plan.write_proofs().unwrap_or_default() != proofs {
        return Err(invalid());
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: CMP-015
    #[test]
    fn proof_decoder_refuses_unknown_tags_and_unbounded_counts() {
        for bytes in [
            vec![0, 0, 16, 1],
            vec![0, 0, 0, 1, 255],
            vec![0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 16, 1],
        ] {
            let mut reader = Reader::new(&bytes);
            assert!(decode(&mut reader).is_err());
        }
    }
    // req: CMP-015
    #[test]
    fn legacy_guard_and_fabricated_proofs_cannot_bypass_new_validation() {
        let (old, schema) = crate::plan::tests::minimal_mutation();
        let plan = crate::plan::tests::mutation_with_added_outcome(&old, &schema, true);
        assert!(plan.write_proofs().is_none());
        assert!(validate(plan.clone(), &schema, &[]).is_err());
        for evidence in [
            WriteEvidence::Initialization,
            WriteEvidence::StateDerived,
            WriteEvidence::BusinessCheck { requirement: 0 },
            WriteEvidence::CallerVersionFence {
                requirement: 0,
                advancement: 1,
            },
        ] {
            let fabricated = [WriteProof {
                location: WriteLocation::Instruction(1),
                evidence: vec![evidence],
            }];
            assert!(validate(plan.clone(), &schema, &fabricated).is_err());
        }
    }
    // req: CMP-015
    #[test]
    fn canonical_proof_validation_refuses_omissions_duplicates_and_wrong_locations() {
        let (old, schema) = crate::plan::tests::minimal_mutation();
        let old = crate::plan::tests::mutation_with_added_outcome(&old, &schema, true);
        let instructions = old
            .instructions()
            .iter()
            .map(|instruction| match instruction {
                Instruction::SetField {
                    binding,
                    field,
                    value,
                    ..
                } => Instruction::SetField {
                    binding: *binding,
                    field: *field,
                    value: *value,
                    guard: WriteGuard::Unguarded,
                },
                other => other.clone(),
            })
            .collect();
        let plan = CommandPlan::new(
            old.command_id(),
            old.required_capability().lineage().clone(),
            old.name(),
            old.contract_version(),
            old.input().clone(),
            old.outcomes().to_vec(),
            old.success_outcome(),
            old.idempotency_input(),
            old.expressions().clone(),
            old.bindings().to_vec(),
            old.root_validation_reads().to_vec(),
            old.locality().clone(),
            old.commit_checks().to_vec(),
            instructions,
            old.execution_class(),
            &schema,
        )
        .expect("explicit overwrite")
        .with_write_proofs(&BTreeMap::new(), &schema)
        .expect("sealed proof");
        let proofs = plan.write_proofs().expect("new proofs");
        assert_eq!(proofs[0].evidence(), &[WriteEvidence::Overwrite]);
        assert_eq!(
            validate(plan.clone(), &schema, proofs).expect("canonical proofs"),
            plan
        );
        let mut writer = Writer::new(4096);
        encode(&mut writer, proofs).expect("encode");
        let bytes = writer.finish();
        assert_eq!(decode(&mut Reader::new(&bytes)).expect("decode"), proofs);
        for malformed in [
            vec![],
            vec![proofs[0].clone(), proofs[0].clone()],
            vec![WriteProof {
                location: proofs[0].location,
                evidence: vec![WriteEvidence::Overwrite, WriteEvidence::Overwrite],
            }],
            vec![WriteProof {
                location: WriteLocation::Instruction(0),
                evidence: vec![WriteEvidence::Overwrite],
            }],
            vec![WriteProof {
                location: WriteLocation::Decision {
                    decision: 0,
                    arm: 0,
                    instruction: 0,
                },
                evidence: vec![WriteEvidence::Overwrite],
            }],
        ] {
            assert!(validate(plan.clone(), &schema, &malformed).is_err());
        }
    }
}

//! Exact-parent compatibility evidence for the campaign's additive deployment.

#[cfg(test)]
mod tests {
    // req: END-003, END-007, END-008
    #[test]
    fn additive_successor_preserves_parent_entity_and_command_identities() {
        let parent = riffdb_contract_compiler::compile_contract_source(include_str!(
            "../../lifecycle/riffdb/contract.riff"
        ))
        .expect("parent compiles");
        assert_eq!(
            parent.bundle_hash().as_bytes(),
            &crate::application::CONTRACT_BUNDLE_HASH
        );
        let successor = riffdb_contract_compiler::compile_contract_successor(
            include_str!("../../../../fixtures/ticketdesk_lifecycle/additive-v2.riff"),
            &parent,
        )
        .expect("exact-parent additive successor compiles");
        assert_eq!(
            successor.bundle_hash().as_bytes(),
            &crate::application_v2::CONTRACT_BUNDLE_HASH
        );
        assert_eq!(
            successor.schema().entities().len(),
            parent.schema().entities().len()
        );
        for original in parent.schema().entities() {
            let retained = successor
                .schema()
                .entities()
                .iter()
                .find(|entity| entity.name() == original.name())
                .expect("entity retained");
            assert_eq!(retained.id(), original.id());
            assert_eq!(retained, original);
        }
        assert_eq!(successor.commands().len(), parent.commands().len() + 1);
        for original in parent.commands() {
            let retained = successor
                .commands()
                .iter()
                .find(|command| command.name() == original.name())
                .expect("command retained");
            assert_eq!(retained.command_id(), original.command_id());
        }
    }
}

//! Reproducibility check for the checked-in complete language reference.

const GRAMMAR: &str = include_str!("../src/grammar.lalrpop");
const REFERENCE: &str = include_str!("../LANGUAGE.md");
const GRAMMAR_MARKER: &str = "## Complete Grammar\n\n```lalrpop\n";

#[test]
fn checked_reference_embeds_the_complete_grammar_exactly() {
    let (_, generated_grammar) = REFERENCE
        .split_once(GRAMMAR_MARKER)
        .expect("language reference must contain its generated grammar section");
    let generated_grammar = generated_grammar
        .strip_suffix("```\n")
        .expect("language reference must close the grammar fence");
    assert_eq!(generated_grammar, GRAMMAR);
}

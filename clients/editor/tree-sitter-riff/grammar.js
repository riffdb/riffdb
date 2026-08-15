// Highlighting grammar only. Compiler acceptance lives in
// crates/riffdb-contract-syntax and crates/riffdb-contract-compiler.
const keywords = require('./keywords.json');

module.exports = grammar({
  name: 'riff',
  extras: $ => [/\s/, $.line_comment],
  word: $ => $.identifier,
  rules: {
    source_file: $ => repeat(choice(
      $.keyword,
      $.string,
      $.number,
      $.identifier,
      $.operator,
      $.punctuation,
      $.block_comment,
    )),
    keyword: _ => choice(...keywords),
    identifier: _ => /[A-Za-z_][A-Za-z0-9_]*/,
    number: _ => /[0-9]+(?:\.[0-9]+|\.\.[0-9]+)?/,
    string: _ => /"(?:[^"\\\n]|\\.)*"/,
    operator: _ => choice('->', '<=', '>=', '==', '!=', '&&', '||', '<', '>', '=', '!', '-', '*', '/', '+'),
    punctuation: _ => choice('{', '}', '(', ')', ',', ':', '.'),
    line_comment: _ => token(seq('//', /[^\n]*/)),
    block_comment: _ => token(seq('/*', /([^*]|\*+[^*/])*/, '*', '/')),
  },
});

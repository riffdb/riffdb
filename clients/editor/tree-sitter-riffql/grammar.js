// Highlighting grammar only. RiffQL acceptance lives in
// crates/riffdb-riffql-syntax and crates/riffdb-query-compiler.
const keywords = require('./keywords.json');

module.exports = grammar({
  name: 'riffql',
  extras: $ => [/\s/, $.line_comment],
  word: $ => $.identifier,
  rules: {
    source_file: $ => repeat(choice(
      $.keyword,
      $.parameter,
      $.string,
      $.number,
      $.identifier,
      $.operator,
      $.punctuation,
    )),
    keyword: _ => choice(...keywords),
    parameter: _ => /\$[A-Za-z_][A-Za-z0-9_]*/,
    identifier: _ => /[A-Za-z_][A-Za-z0-9_]*/,
    number: _ => /[0-9]+/,
    string: _ => /"(?:[^"\\\n]|\\.)*"/,
    operator: _ => choice('<=', '>=', '==', '!=', '&&', '||', '<', '>', '=', '|'),
    punctuation: _ => choice('{', '}', '(', ')', ',', ':', '.', '?'),
    line_comment: _ => token(seq('//', /[^\n]*/)),
  },
});

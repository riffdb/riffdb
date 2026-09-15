#![forbid(unsafe_code)]

//! Prints the public error registry fixture from the semantic owner.

use std::io::{self, Write};

fn main() -> io::Result<()> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    writeln!(output, "code\tcategory\trecovery_action\tfixes\tmessage")?;
    for code in riffdb_errors::APPLICATION_ERROR_CODES {
        let fixes = code
            .fixes()
            .iter()
            .map(|fix| fix.as_str())
            .collect::<Vec<_>>()
            .join(",");
        writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}",
            code.as_str(),
            code.category().as_str(),
            code.recovery_action().as_str(),
            fixes,
            code.safe_message()
        )?;
    }
    output.flush()
}

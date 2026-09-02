//! Infallible completion for formatting operations whose destination is a `String`.

/// Consumes the `fmt::Result` returned by `write!` and `writeln!` when their
/// destination is a `String`.
///
/// `String`'s `fmt::Write` implementation appends directly and cannot return
/// `fmt::Error`. Keeping that invariant in one helper avoids hundreds of
/// panic-capable `expect` calls in the language generators.
pub(crate) trait InfallibleStringWrite {
    /// Completes a formatting operation whose destination is a `String`.
    fn infallible(self);
}

impl InfallibleStringWrite for std::fmt::Result {
    #[inline]
    fn infallible(self) {
        // `std::fmt::Write for String` always returns `Ok(())`. The result is
        // consumed here instead of converting the unrepresentable error into
        // a panic at every generator call site.
        let _ = self;
    }
}

#[cfg(test)]
mod tests {
    const GENERATORS: [(&str, &str); 3] = [
        ("generation.rs", include_str!("generation.rs")),
        ("go_generation.rs", include_str!("go_generation.rs")),
        ("python_generation.rs", include_str!("python_generation.rs")),
    ];

    #[test]
    fn generators_use_the_infallible_writer_and_carry_no_fmt_expect() {
        for (name, source) in GENERATORS {
            let formatting_calls =
                source.matches("write!(").count() + source.matches("writeln!(").count();
            let infallible_completions = source.matches(".infallible()").count();
            assert!(
                source.contains("InfallibleStringWrite as _"),
                "{name} must import the shared infallible String writer"
            );
            assert_eq!(
                infallible_completions, formatting_calls,
                "every formatting call in {name} must route through the shared helper"
            );
            assert!(
                !source.contains("String writes cannot fail")
                    && !source.contains(".expect(\"string\")")
                    && !source.contains(".unwrap()"),
                "{name} must not retain panic-capable fmt::Write completion"
            );
        }
    }
}

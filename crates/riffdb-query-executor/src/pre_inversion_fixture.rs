//! Canonical, test-only query observation encoding for WP-754.

use riffdb_types::encode_canonical_value;

use crate::{
    QueryAggregateCell, QueryAggregateRow, QueryExecutionError, QueryOwnedSnapshot,
    QueryResultValue, QueryRow,
};

const MAGIC: &[u8] = b"riffdb.query-pre-inversion/v1\0";
const MAX_FIXTURE_BYTES: usize = 16 * 1024 * 1024;

/// Encodes one successful pre-inversion observation without debug formatting.
pub fn encode_query_snapshot_fixture_v1(case: &str, snapshot: &QueryOwnedSnapshot) -> Vec<u8> {
    let mut encoder = Encoder::new(case, 1);
    encoder.u64(snapshot.application_head());
    encoder.count(snapshot.index_epochs().len());
    for (name, epoch) in snapshot.index_epochs() {
        encoder.bytes(name.as_bytes());
        encoder.u64(*epoch);
    }
    encoder.bytes(snapshot.outcome().as_bytes());
    encoder.count(snapshot.fields().len());
    for (name, value) in snapshot.fields() {
        encoder.bytes(name.as_bytes());
        encoder.result(value);
    }
    match snapshot.covered_result() {
        Some(covered) => {
            encoder.byte(1);
            encoder.bytes(covered.result_name().as_bytes());
            encoder.bytes(covered.entity().as_bytes());
            let fields = covered.fields().collect::<Vec<_>>();
            encoder.count(fields.len());
            for field in fields {
                encoder.bytes(field.as_bytes());
            }
            encoder.count(covered.rows().len());
            for row in covered.rows() {
                encoder.count(row.len());
                for value in row {
                    encoder.canonical(value);
                }
            }
        }
        None => encoder.byte(0),
    }
    encoder.optional_bytes(snapshot.continuation_binding().map(str::as_bytes));
    encoder.optional_bytes(snapshot.continuation());
    encoder.finish()
}

/// Encodes one closed typed pre-inversion refusal.
pub fn encode_query_error_fixture_v1(case: &str, error: &QueryExecutionError) -> Vec<u8> {
    let mut encoder = Encoder::new(case, 2);
    match error {
        QueryExecutionError::MissingParameter { parameter } => {
            encoder.byte(1);
            encoder.bytes(parameter.as_bytes());
        }
        QueryExecutionError::InvalidParameter { parameter } => {
            encoder.byte(2);
            encoder.bytes(parameter.as_bytes());
        }
        QueryExecutionError::MissingField { entity, field } => {
            encoder.byte(3);
            encoder.bytes(entity.as_bytes());
            encoder.bytes(field.as_bytes());
        }
        QueryExecutionError::InvalidProgram => encoder.byte(4),
        QueryExecutionError::BackendUnavailable => encoder.byte(5),
        QueryExecutionError::BackendIntegrity => encoder.byte(6),
        QueryExecutionError::BackendLimitExceeded => encoder.byte(7),
        QueryExecutionError::BoundExceeded => encoder.byte(8),
        QueryExecutionError::AggregateOverflow => encoder.byte(9),
        QueryExecutionError::FuelExhausted => encoder.byte(10),
        QueryExecutionError::UnexpectedCardinality { binding } => {
            encoder.byte(11);
            encoder.bytes(binding.as_bytes());
        }
        QueryExecutionError::UnsupportedPredicate => encoder.byte(12),
        QueryExecutionError::InvalidDependentKey { binding, field } => {
            encoder.byte(13);
            encoder.bytes(binding.as_bytes());
            encoder.bytes(field.as_bytes());
        }
        QueryExecutionError::StaleCursor => encoder.byte(14),
        QueryExecutionError::InvalidContinuation => encoder.byte(15),
    }
    encoder.finish()
}

struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn new(case: &str, kind: u8) -> Self {
        let mut encoder = Self {
            bytes: Vec::with_capacity(1024),
        };
        encoder.bytes.extend_from_slice(MAGIC);
        encoder.bytes(case.as_bytes());
        encoder.byte(kind);
        encoder
    }

    fn finish(self) -> Vec<u8> {
        assert!(
            self.bytes.len() <= MAX_FIXTURE_BYTES,
            "query fixture exceeds the independent 16 MiB capture ceiling"
        );
        self.bytes
    }

    fn byte(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn count(&mut self, value: usize) {
        let value = u32::try_from(value).expect("query fixture count fits u32");
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.count(value.len());
        self.bytes.extend_from_slice(value);
    }

    fn optional_bytes(&mut self, value: Option<&[u8]>) {
        match value {
            Some(value) => {
                self.byte(1);
                self.bytes(value);
            }
            None => self.byte(0),
        }
    }

    fn canonical(&mut self, value: &riffdb_types::CanonicalValue) {
        self.bytes(
            &encode_canonical_value(value).expect("checked query value canonically encodes"),
        );
    }

    fn row(&mut self, row: &QueryRow) {
        self.bytes(row.entity().as_bytes());
        let fields = row.fields().collect::<Vec<_>>();
        self.count(fields.len());
        for (name, value) in fields {
            self.bytes(name.as_bytes());
            self.canonical(value);
        }
        let nested = row.nested_fields().collect::<Vec<_>>();
        self.count(nested.len());
        for (name, rows) in nested {
            self.bytes(name.as_bytes());
            self.count(rows.len());
            for row in rows {
                self.row(row);
            }
        }
    }

    fn aggregate(&mut self, row: &QueryAggregateRow) {
        self.bytes(row.entity().as_bytes());
        self.count(row.fields().len());
        for (name, value) in row.fields() {
            self.bytes(name.as_bytes());
            match value {
                QueryAggregateCell::Canonical(value) => {
                    self.byte(1);
                    self.canonical(value);
                }
                QueryAggregateCell::ExactDecimal { coefficient, scale } => {
                    self.byte(2);
                    self.bytes.extend_from_slice(&coefficient.to_be_bytes());
                    self.byte(*scale);
                }
                QueryAggregateCell::ExactMean {
                    coefficient,
                    scale,
                    count,
                } => {
                    self.byte(3);
                    self.bytes.extend_from_slice(&coefficient.to_be_bytes());
                    self.byte(*scale);
                    self.u64(*count);
                }
            }
        }
    }

    fn result(&mut self, value: &QueryResultValue) {
        match value {
            QueryResultValue::One(row) => {
                self.byte(1);
                self.row(row);
            }
            QueryResultValue::Maybe(row) => {
                self.byte(2);
                match row {
                    Some(row) => {
                        self.byte(1);
                        self.row(row);
                    }
                    None => self.byte(0),
                }
            }
            QueryResultValue::Many(rows) => {
                self.byte(3);
                self.count(rows.len());
                for row in rows {
                    self.row(row);
                }
            }
            QueryResultValue::AggregateOne(row) => {
                self.byte(4);
                self.aggregate(row);
            }
            QueryResultValue::AggregateMany(rows) => {
                self.byte(5);
                self.count(rows.len());
                for row in rows {
                    self.aggregate(row);
                }
            }
        }
    }
}

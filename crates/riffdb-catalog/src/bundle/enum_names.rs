//! The shared enum table keeps its charge through actual final table destruction.
use super::*;

/// First-party immutable enum display-name storage.
/// Ownership bookkeeping is absent from semantic equality and durable bytes.
#[doc(hidden)]
#[derive(Clone, Eq, PartialEq)]
pub struct ContractEnumVariantNameTable {
    names: BTreeMap<(u32, u32), String>,
    lifecycle: riffdb_types::OwnedCapacityLifecycleV1,
}
impl From<BTreeMap<(u32, u32), String>> for ContractEnumVariantNameTable {
    fn from(names: BTreeMap<(u32, u32), String>) -> Self {
        Self {
            names,
            lifecycle: riffdb_types::OwnedCapacityLifecycleV1::default(),
        }
    }
}
impl std::ops::Deref for ContractEnumVariantNameTable {
    type Target = BTreeMap<(u32, u32), String>;
    fn deref(&self) -> &Self::Target {
        &self.names
    }
}
impl fmt::Debug for ContractEnumVariantNameTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContractEnumVariantNameTable([REDACTED])")
    }
}
riffdb_types::inspect_owned_capacity_struct_v1!(ContractEnumVariantNameTable {
    owned: [names],
    inline: [],
    lifecycle: lifecycle
});

//! Native immutable AVL directory. Keys and values are bounded ownership handles;
//! payload/manifest population belongs to admitted, incrementally retired owners.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use riffdb_storage_api::{StorageError, StorageErrorKind};

use crate::locator_memory::{
    ChargedLocatorAllocation, LocatorAllocationClass, LocatorCharge, LocatorRetireContents,
    NativeLocatorPool,
};

/// Closed source-ownership proof for keys and values admitted into one family.
/// Heap-owning members must retain their original charged physical allocation.
pub(crate) trait LocatorFamilyMember: Clone + Send + Sync + 'static {
    fn validate_family(&self, pool: &NativeLocatorPool) -> Result<(), StorageError>;
}
impl LocatorFamilyMember for u64 {
    fn validate_family(&self, _: &NativeLocatorPool) -> Result<(), StorageError> {
        Ok(())
    }
}
impl LocatorFamilyMember for riffdb_types::CommitSequence {
    fn validate_family(&self, _: &NativeLocatorPool) -> Result<(), StorageError> {
        Ok(())
    }
}
impl LocatorFamilyMember for crate::locator_memory::ChargedLocatorKey {
    fn validate_family(&self, pool: &NativeLocatorPool) -> Result<(), StorageError> {
        self.validate_family_pool(pool)
    }
}

const MAX_HEIGHT: usize = 64;
type Link<K, V> = Option<ChargedLocatorAllocation<Node<K, V>>>;
type RemovedFirst<K, V> = ((K, V), Link<K, V>);

struct Node<K: LocatorFamilyMember, V: LocatorFamilyMember> {
    key: K,
    value: V,
    height: u8,
    left: Link<K, V>,
    right: Link<K, V>,
}
impl<K: LocatorFamilyMember, V: LocatorFamilyMember> LocatorRetireContents for Node<K, V> {
    fn retire_one(&mut self) -> bool {
        // Each handle's final destruction only enqueues a preadmitted descriptor.
        self.left.take();
        self.right.take();
        true
    }
}

pub(crate) struct LocatorTree<K: LocatorFamilyMember, V: LocatorFamilyMember> {
    root: Link<K, V>,
    pool: Arc<NativeLocatorPool>,
    class: LocatorAllocationClass,
    len: usize,
}
impl<K: LocatorFamilyMember, V: LocatorFamilyMember> Clone for LocatorTree<K, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            pool: Arc::clone(&self.pool),
            class: self.class,
            len: self.len,
        }
    }
}
impl<K: LocatorFamilyMember, V: LocatorFamilyMember> fmt::Debug for LocatorTree<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocatorTree")
            .field("len", &self.len)
            .field("height", &height(&self.root))
            .finish()
    }
}
fn height<K: LocatorFamilyMember, V: LocatorFamilyMember>(link: &Link<K, V>) -> u8 {
    link.as_ref().map_or(0, |node| node.height)
}
fn invariant() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

impl<K: Ord + LocatorFamilyMember, V: LocatorFamilyMember> LocatorTree<K, V> {
    pub(crate) fn empty(pool: Arc<NativeLocatorPool>, class: LocatorAllocationClass) -> Self {
        Self {
            root: None,
            pool,
            class,
            len: 0,
        }
    }
    pub(crate) fn with_allocation_class(&self, class: LocatorAllocationClass) -> Self {
        Self {
            root: self.root.clone(),
            pool: Arc::clone(&self.pool),
            class,
            len: self.len,
        }
    }
    /// All reachable nodes, including precreated final-release tasks and a
    /// conservative descriptor share. External key/value ownership is counted
    /// by the generation's admitted metadata total, rather than walked here.
    #[cfg(test)]
    pub(crate) fn structural_charge_bound(&self) -> Option<usize> {
        let node = ChargedLocatorAllocation::<Node<K, V>>::allocation_charge_bound(0)?
            .checked_add(ChargedLocatorAllocation::<Node<K, V>>::retirement_descriptor_bound())?;
        self.len.checked_mul(node)
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.root.is_none()
    }
    pub(crate) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        let mut current = self.root.as_ref();
        while let Some(node) = current {
            match key.cmp(node.key.borrow()) {
                Ordering::Less => current = node.left.as_ref(),
                Ordering::Equal => return Some(&node.value),
                Ordering::Greater => current = node.right.as_ref(),
            }
        }
        None
    }
    pub(crate) fn contains_key<Q: Ord + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.get(key).is_some()
    }
    pub(crate) fn predecessor<Q: Ord + ?Sized>(&self, key: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q>,
    {
        let mut current = self.root.as_ref();
        let mut best = None;
        while let Some(node) = current {
            if node.key.borrow() <= key {
                best = Some((&node.key, &node.value));
                current = node.right.as_ref();
            } else {
                current = node.left.as_ref();
            }
        }
        best
    }
    #[cfg(test)]
    pub(crate) fn first(&self) -> Option<(&K, &V)> {
        let mut node = self.root.as_ref()?;
        while let Some(left) = node.left.as_ref() {
            node = left;
        }
        Some((&node.key, &node.value))
    }
    pub(crate) fn last(&self) -> Option<(&K, &V)> {
        let mut node = self.root.as_ref()?;
        while let Some(right) = node.right.as_ref() {
            node = right;
        }
        Some((&node.key, &node.value))
    }
    pub(crate) fn iter(&self) -> LocatorTreeIter<'_, K, V> {
        let mut result = LocatorTreeIter {
            stack: [None; MAX_HEIGHT],
            depth: 0,
        };
        result.push_left(self.root.as_deref());
        result
    }
    pub(crate) fn iter_from<Q: Ord + ?Sized>(&self, key: &Q) -> LocatorTreeIter<'_, K, V>
    where
        K: Borrow<Q>,
    {
        let mut result = LocatorTreeIter {
            stack: [None; MAX_HEIGHT],
            depth: 0,
        };
        let mut current = self.root.as_deref();
        while let Some(node) = current {
            if node.key.borrow() >= key {
                result.push(node);
                current = node.left.as_deref();
            } else {
                current = node.right.as_deref();
            }
        }
        result
    }

    /// Construct a successor without changing this root. Every node allocation
    /// is admitted before construction; cancelled paths retire through the queue.
    pub(crate) fn try_insert(&self, key: K, value: V) -> Result<Self, StorageError> {
        let _scratch = self.construction_scratch()?;
        let existed = self.get(&key).is_some();
        let len = self
            .len
            .checked_add(usize::from(!existed))
            .ok_or_else(invariant)?;
        let root = Some(self.insert(&self.root, key, value, 0)?);
        Ok(Self {
            root,
            pool: Arc::clone(&self.pool),
            class: self.class,
            len,
        })
    }
    pub(crate) fn try_remove<Q: Ord + ?Sized>(&self, key: &Q) -> Result<Self, StorageError>
    where
        K: Borrow<Q>,
    {
        let _scratch = self.construction_scratch()?;
        if !self.contains_key(key) {
            return Ok(self.clone());
        }
        let root = self.remove(&self.root, key, 0)?;
        Ok(Self {
            root,
            pool: Arc::clone(&self.pool),
            class: self.class,
            len: self.len - 1,
        })
    }
    fn construction_scratch(&self) -> Result<LocatorCharge, StorageError> {
        // Path recursion is bounded by MAX_HEIGHT. Account conservative fixed
        // frame space plus every generic-sized argument/temporary before work;
        // heap nodes and their retirement tasks are independently admitted.
        let frame_bytes = std::mem::size_of::<K>()
            .checked_mul(4)
            .and_then(|n| n.checked_add(std::mem::size_of::<V>().checked_mul(4)?))
            .and_then(|n| n.checked_add(std::mem::size_of::<Link<K, V>>().checked_mul(8)?))
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(invariant)?;
        let bytes = frame_bytes.checked_mul(MAX_HEIGHT).ok_or_else(invariant)?;
        self.pool.admit(bytes, self.class)
    }
    fn node(
        &self,
        key: K,
        value: V,
        left: Link<K, V>,
        right: Link<K, V>,
    ) -> Result<ChargedLocatorAllocation<Node<K, V>>, StorageError> {
        key.validate_family(&self.pool)?;
        value.validate_family(&self.pool)?;
        for child in [&left, &right].into_iter().flatten() {
            child.validate_family_pool(&self.pool)?;
        }
        let height = 1 + height(&left).max(height(&right));
        if usize::from(height) > MAX_HEIGHT {
            return Err(invariant());
        }
        ChargedLocatorAllocation::try_new(&self.pool, self.class, 0, || {
            Ok(Node {
                key,
                value,
                height,
                left,
                right,
            })
        })
    }
    fn balanced(
        &self,
        key: K,
        value: V,
        left: Link<K, V>,
        right: Link<K, V>,
    ) -> Result<ChargedLocatorAllocation<Node<K, V>>, StorageError> {
        let balance = i16::from(height(&left)) - i16::from(height(&right));
        if balance > 1 {
            let pivot = left.as_ref().ok_or_else(invariant)?;
            if height(&pivot.left) >= height(&pivot.right) {
                let new_right = self.node(key, value, pivot.right.clone(), right)?;
                return self.node(
                    pivot.key.clone(),
                    pivot.value.clone(),
                    pivot.left.clone(),
                    Some(new_right),
                );
            }
            let middle = pivot.right.as_ref().ok_or_else(invariant)?;
            let new_left = self.node(
                pivot.key.clone(),
                pivot.value.clone(),
                pivot.left.clone(),
                middle.left.clone(),
            )?;
            let new_right = self.node(key, value, middle.right.clone(), right)?;
            return self.node(
                middle.key.clone(),
                middle.value.clone(),
                Some(new_left),
                Some(new_right),
            );
        }
        if balance < -1 {
            let pivot = right.as_ref().ok_or_else(invariant)?;
            if height(&pivot.right) >= height(&pivot.left) {
                let new_left = self.node(key, value, left, pivot.left.clone())?;
                return self.node(
                    pivot.key.clone(),
                    pivot.value.clone(),
                    Some(new_left),
                    pivot.right.clone(),
                );
            }
            let middle = pivot.left.as_ref().ok_or_else(invariant)?;
            let new_left = self.node(key, value, left, middle.left.clone())?;
            let new_right = self.node(
                pivot.key.clone(),
                pivot.value.clone(),
                middle.right.clone(),
                pivot.right.clone(),
            )?;
            return self.node(
                middle.key.clone(),
                middle.value.clone(),
                Some(new_left),
                Some(new_right),
            );
        }
        self.node(key, value, left, right)
    }
    fn insert(
        &self,
        link: &Link<K, V>,
        key: K,
        value: V,
        depth: usize,
    ) -> Result<ChargedLocatorAllocation<Node<K, V>>, StorageError> {
        if depth >= MAX_HEIGHT {
            return Err(invariant());
        }
        let Some(node) = link else {
            return self.node(key, value, None, None);
        };
        match key.cmp(&node.key) {
            Ordering::Less => self.balanced(
                node.key.clone(),
                node.value.clone(),
                Some(self.insert(&node.left, key, value, depth + 1)?),
                node.right.clone(),
            ),
            Ordering::Equal => self.node(key, value, node.left.clone(), node.right.clone()),
            Ordering::Greater => self.balanced(
                node.key.clone(),
                node.value.clone(),
                node.left.clone(),
                Some(self.insert(&node.right, key, value, depth + 1)?),
            ),
        }
    }
    fn remove<Q: Ord + ?Sized>(
        &self,
        link: &Link<K, V>,
        key: &Q,
        depth: usize,
    ) -> Result<Link<K, V>, StorageError>
    where
        K: Borrow<Q>,
    {
        if depth >= MAX_HEIGHT {
            return Err(invariant());
        }
        let Some(node) = link else {
            return Ok(None);
        };
        match key.cmp(node.key.borrow()) {
            Ordering::Less => Ok(Some(self.balanced(
                node.key.clone(),
                node.value.clone(),
                self.remove(&node.left, key, depth + 1)?,
                node.right.clone(),
            )?)),
            Ordering::Greater => Ok(Some(self.balanced(
                node.key.clone(),
                node.value.clone(),
                node.left.clone(),
                self.remove(&node.right, key, depth + 1)?,
            )?)),
            Ordering::Equal => {
                if node.left.is_none() {
                    return Ok(node.right.clone());
                }
                if node.right.is_none() {
                    return Ok(node.left.clone());
                }
                let (successor, right) = self.remove_first(&node.right, depth + 1)?;
                Ok(Some(self.balanced(
                    successor.0,
                    successor.1,
                    node.left.clone(),
                    right,
                )?))
            }
        }
    }
    fn remove_first(
        &self,
        link: &Link<K, V>,
        depth: usize,
    ) -> Result<RemovedFirst<K, V>, StorageError> {
        if depth >= MAX_HEIGHT {
            return Err(invariant());
        }
        let node = link.as_ref().ok_or_else(invariant)?;
        if node.left.is_none() {
            return Ok(((node.key.clone(), node.value.clone()), node.right.clone()));
        }
        let (first, left) = self.remove_first(&node.left, depth + 1)?;
        let replacement = self.balanced(
            node.key.clone(),
            node.value.clone(),
            left,
            node.right.clone(),
        )?;
        Ok((first, Some(replacement)))
    }
}

pub(crate) struct LocatorTreeIter<'a, K: LocatorFamilyMember, V: LocatorFamilyMember> {
    stack: [Option<&'a Node<K, V>>; MAX_HEIGHT],
    depth: usize,
}
impl<'a, K: LocatorFamilyMember, V: LocatorFamilyMember> LocatorTreeIter<'a, K, V> {
    fn push(&mut self, node: &'a Node<K, V>) {
        // Constructor admission rejects a height exceeding this fixed stack.
        if self.depth < MAX_HEIGHT {
            self.stack[self.depth] = Some(node);
            self.depth += 1;
        }
    }
    fn push_left(&mut self, mut link: Option<&'a Node<K, V>>) {
        while let Some(node) = link {
            self.push(node);
            link = node.left.as_deref();
        }
    }
}
impl<'a, K: LocatorFamilyMember, V: LocatorFamilyMember> Iterator for LocatorTreeIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        self.depth = self.depth.checked_sub(1)?;
        let node = self.stack[self.depth].take()?;
        self.push_left(node.right.as_deref());
        Some((&node.key, &node.value))
    }
}

#[cfg(test)]
#[path = "locator_tree_tests.rs"]
mod tests;

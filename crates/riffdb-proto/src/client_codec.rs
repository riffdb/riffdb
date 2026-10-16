//! Strict bounded codec for generated client RPCs.

use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic_prost::prost::Message;
use tonic_prost::prost::bytes::{Buf, BufMut, Bytes};

use crate::{PublicMessage, decode_public_message, validate_public_message_encoded_len};

const INVALID_MESSAGE: &str = "invalid protobuf message";

/// Base tag for client-internal wire cache carrier IDs (`selected_result_encoding <= -0x4000_0001`).
#[doc(hidden)]
pub const CLIENT_WIRE_CARRIER_ID_BASE: u32 = 0x4000_0001;
const CLIENT_WIRE_CARRIER_ID_MAX: u32 = 0x7FFF_FFFE;
const LOCAL_CLIENT_QUERY_WIRE_CACHE_CAP: usize = 256;
const LOCAL_CLIENT_QUERY_WIRE_HASH_SLOTS: usize = 512;
const SHARED_CLIENT_QUERY_WIRE_SHARDS: usize = 16;
const SHARED_CLIENT_QUERY_WIRE_SHARD_CAP: usize = 64;
const MAX_CACHED_CLIENT_QUERY_WIRE_BYTES: usize = 262_144;

static NEXT_CLIENT_WIRE_CACHE_ID: AtomicU64 = AtomicU64::new(CLIENT_WIRE_CARRIER_ID_BASE as u64);

std::thread_local! {
    static CLIENT_QUERY_WIRE_CARRIER_DEPTH: Cell<u32> = const { Cell::new(0) };
    static LOCAL_CLIENT_QUERY_WIRE_CACHE: RefCell<LocalClientQueryWireCache> =
        RefCell::new(LocalClientQueryWireCache::default());
}

/// Drop guard that enables lightweight carrier responses for `ExecuteQueryResponse`
/// during a synchronous `Future::poll` invocation inside `execute_named_query`.
#[doc(hidden)]
pub struct ClientQueryWireCarrierScopeGuard {
    _private: (),
}

impl Drop for ClientQueryWireCarrierScopeGuard {
    fn drop(&mut self) {
        CLIENT_QUERY_WIRE_CARRIER_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

/// Enters a synchronous poll scope where `StrictProstDecoder` may return a
/// lightweight carrier `ExecuteQueryResponse` for cached legacy-record responses.
#[doc(hidden)]
#[must_use]
pub fn enter_client_query_wire_carrier_scope() -> ClientQueryWireCarrierScopeGuard {
    CLIENT_QUERY_WIRE_CARRIER_DEPTH.with(|depth| {
        depth.set(depth.get().saturating_add(1));
    });
    ClientQueryWireCarrierScopeGuard { _private: () }
}

fn is_client_query_wire_carrier_enabled() -> bool {
    CLIENT_QUERY_WIRE_CARRIER_DEPTH.with(|depth| depth.get() > 0)
}

#[derive(Clone)]
struct CachedClientQueryWireEntry {
    wire_hash: u64,
    wire_bytes: Bytes,
    wire_cache_id: u32,
    can_use_carrier: bool,
    response: Arc<crate::app::v1::ExecuteQueryResponse>,
    carrier_response: crate::app::v1::ExecuteQueryResponse,
}

impl CachedClientQueryWireEntry {
    fn select_response(&self, allow_carrier: bool) -> crate::app::v1::ExecuteQueryResponse {
        if allow_carrier && self.can_use_carrier {
            self.carrier_response.clone()
        } else {
            (*self.response).clone()
        }
    }
}

struct LocalClientQueryWireCache {
    entries: Vec<CachedClientQueryWireEntry>,
    next_slot: usize,
    by_hash: [u16; LOCAL_CLIENT_QUERY_WIRE_HASH_SLOTS],
    by_wire_id: [u16; LOCAL_CLIENT_QUERY_WIRE_HASH_SLOTS],
}

impl Default for LocalClientQueryWireCache {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            next_slot: 0,
            by_hash: [0; LOCAL_CLIENT_QUERY_WIRE_HASH_SLOTS],
            by_wire_id: [0; LOCAL_CLIENT_QUERY_WIRE_HASH_SLOTS],
        }
    }
}

impl LocalClientQueryWireCache {
    fn hash_bucket(wire_hash: u64) -> usize {
        (wire_hash as usize) & (LOCAL_CLIENT_QUERY_WIRE_HASH_SLOTS - 1)
    }

    fn wire_id_bucket(wire_cache_id: u32) -> usize {
        (wire_cache_id as usize) & (LOCAL_CLIENT_QUERY_WIRE_HASH_SLOTS - 1)
    }

    fn lookup_by_bytes(&self, wire_hash: u64, bytes: &[u8]) -> Option<&CachedClientQueryWireEntry> {
        let bucket = Self::hash_bucket(wire_hash);
        let idx1 = self.by_hash[bucket] as usize;
        if idx1 != 0
            && let Some(entry) = self.entries.get(idx1 - 1)
            && entry.wire_hash == wire_hash
            && entry.wire_bytes.as_ref() == bytes
        {
            return Some(entry);
        }
        self.entries
            .iter()
            .find(|entry| entry.wire_hash == wire_hash && entry.wire_bytes.as_ref() == bytes)
    }

    fn lookup_by_wire_id(
        &self,
        wire_cache_id: u32,
    ) -> Option<Arc<crate::app::v1::ExecuteQueryResponse>> {
        let bucket = Self::wire_id_bucket(wire_cache_id);
        let idx1 = self.by_wire_id[bucket] as usize;
        if idx1 != 0
            && let Some(entry) = self.entries.get(idx1 - 1)
            && entry.wire_cache_id == wire_cache_id
        {
            return Some(Arc::clone(&entry.response));
        }
        self.entries
            .iter()
            .find(|entry| entry.wire_cache_id == wire_cache_id)
            .map(|entry| Arc::clone(&entry.response))
    }

    fn insert(&mut self, entry: CachedClientQueryWireEntry) {
        let hash_bucket = Self::hash_bucket(entry.wire_hash);
        let id_bucket = Self::wire_id_bucket(entry.wire_cache_id);
        let slot = if self.entries.len() < LOCAL_CLIENT_QUERY_WIRE_CACHE_CAP {
            let slot = self.entries.len();
            self.entries.push(entry);
            slot
        } else {
            let slot = self.next_slot;
            self.entries[slot] = entry;
            self.next_slot = (slot + 1) % LOCAL_CLIENT_QUERY_WIRE_CACHE_CAP;
            slot
        };
        self.by_hash[hash_bucket] = (slot + 1) as u16;
        self.by_wire_id[id_bucket] = (slot + 1) as u16;
    }
}

#[derive(Default)]
struct SharedClientQueryWireShard {
    entries: Vec<CachedClientQueryWireEntry>,
    next_slot: usize,
}

static SHARED_CLIENT_QUERY_WIRE_CACHE: OnceLock<
    [RwLock<SharedClientQueryWireShard>; SHARED_CLIENT_QUERY_WIRE_SHARDS],
> = OnceLock::new();

fn shared_client_query_wire_cache()
-> &'static [RwLock<SharedClientQueryWireShard>; SHARED_CLIENT_QUERY_WIRE_SHARDS] {
    SHARED_CLIENT_QUERY_WIRE_CACHE
        .get_or_init(|| std::array::from_fn(|_| RwLock::new(SharedClientQueryWireShard::default())))
}

fn hash_wire_bytes(bytes: &[u8]) -> u64 {
    let mut h = (bytes.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xCBF2_9CE4_8422_2325;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let mut word = [0u8; 8];
        word.copy_from_slice(chunk);
        h = (h ^ u64::from_le_bytes(word)).wrapping_mul(0x1000_0000_01B3);
    }
    for &b in chunks.remainder() {
        h = (h ^ u64::from(b)).wrapping_mul(0x1000_0000_01B3);
    }
    h ^ (h >> 32)
}

fn lookup_shared_client_query_wire_by_bytes(
    wire_hash: u64,
    bytes: &[u8],
) -> Option<CachedClientQueryWireEntry> {
    let shard_idx = ((wire_hash >> 16) as usize) & (SHARED_CLIENT_QUERY_WIRE_SHARDS - 1);
    let guard = shared_client_query_wire_cache()[shard_idx].read().ok()?;
    guard
        .entries
        .iter()
        .find(|entry| entry.wire_hash == wire_hash && entry.wire_bytes.as_ref() == bytes)
        .cloned()
}

fn insert_shared_client_query_wire(entry: CachedClientQueryWireEntry) {
    let shard_idx = ((entry.wire_hash >> 16) as usize) & (SHARED_CLIENT_QUERY_WIRE_SHARDS - 1);
    let Ok(mut guard) = shared_client_query_wire_cache()[shard_idx].write() else {
        return;
    };
    if guard.entries.iter().any(|existing| {
        existing.wire_hash == entry.wire_hash && existing.wire_bytes == entry.wire_bytes
    }) {
        return;
    }
    if guard.entries.len() < SHARED_CLIENT_QUERY_WIRE_SHARD_CAP {
        guard.entries.push(entry);
    } else {
        let slot = guard.next_slot;
        guard.entries[slot] = entry;
        guard.next_slot = (slot + 1) % SHARED_CLIENT_QUERY_WIRE_SHARD_CAP;
    }
}

/// Looks up the full validated `ExecuteQueryResponse` for a client carrier ID.
#[doc(hidden)]
pub fn lookup_client_query_wire_response(
    wire_cache_id: u32,
) -> Option<Arc<crate::app::v1::ExecuteQueryResponse>> {
    if let Some(found) = LOCAL_CLIENT_QUERY_WIRE_CACHE.with(|cell| {
        let guard = cell.try_borrow().ok()?;
        guard.lookup_by_wire_id(wire_cache_id)
    }) {
        return Some(found);
    }
    for shard in shared_client_query_wire_cache() {
        let Ok(guard) = shard.read() else {
            continue;
        };
        if let Some(entry) = guard
            .entries
            .iter()
            .find(|entry| entry.wire_cache_id == wire_cache_id)
        {
            let cloned = entry.clone();
            let response = Arc::clone(&cloned.response);
            LOCAL_CLIENT_QUERY_WIRE_CACHE.with(|cell| {
                if let Ok(mut local) = cell.try_borrow_mut() {
                    local.insert(cloned);
                }
            });
            return Some(response);
        }
    }
    None
}

fn decode_cached_execute_query_response(
    bytes: Bytes,
) -> Result<crate::app::v1::ExecuteQueryResponse, Status> {
    let allow_carrier = is_client_query_wire_carrier_enabled();
    if bytes.len() <= MAX_CACHED_CLIENT_QUERY_WIRE_BYTES {
        let wire_hash = hash_wire_bytes(bytes.as_ref());
        if let Some(cached) = LOCAL_CLIENT_QUERY_WIRE_CACHE.with(|cell| {
            let guard = cell.try_borrow().ok()?;
            guard
                .lookup_by_bytes(wire_hash, bytes.as_ref())
                .map(|entry| entry.select_response(allow_carrier))
        }) {
            return Ok(cached);
        }
        if let Some(shared_entry) =
            lookup_shared_client_query_wire_by_bytes(wire_hash, bytes.as_ref())
        {
            let out = shared_entry.select_response(allow_carrier);
            LOCAL_CLIENT_QUERY_WIRE_CACHE.with(|cell| {
                if let Ok(mut guard) = cell.try_borrow_mut() {
                    guard.insert(shared_entry);
                }
            });
            return Ok(out);
        }
        let response: crate::app::v1::ExecuteQueryResponse = decode_public_message(bytes.as_ref())
            .map_err(|_| Status::invalid_argument(INVALID_MESSAGE))?;
        let raw_id = NEXT_CLIENT_WIRE_CACHE_ID.fetch_add(1, Ordering::Relaxed);
        let wire_cache_id = if raw_id <= u64::from(CLIENT_WIRE_CARRIER_ID_MAX) {
            raw_id as u32
        } else {
            0
        };
        let can_use_carrier = wire_cache_id >= CLIENT_WIRE_CARRIER_ID_BASE
            && response.next_cursor.is_none()
            && response.compact_result.is_none()
            && response.packed_result.is_none()
            && matches!(
                crate::app::v1::NamedResultEncoding::try_from(response.selected_result_encoding),
                Ok(crate::app::v1::NamedResultEncoding::Unspecified
                    | crate::app::v1::NamedResultEncoding::LegacyRecords)
            );
        let carrier_response = crate::app::v1::ExecuteQueryResponse {
            identity: response.identity.clone(),
            outcome: response.outcome.clone(),
            application_head: response.application_head,
            fields: Vec::new(),
            next_cursor: response.next_cursor.clone(),
            selected_result_encoding: if can_use_carrier {
                -(wire_cache_id as i32)
            } else {
                response.selected_result_encoding
            },
            compact_result: None,
            packed_result: None,
        };
        let response_arc = Arc::new(response);
        let entry = CachedClientQueryWireEntry {
            wire_hash,
            wire_bytes: bytes,
            wire_cache_id,
            can_use_carrier,
            response: Arc::clone(&response_arc),
            carrier_response,
        };
        let out = entry.select_response(allow_carrier);
        insert_shared_client_query_wire(entry.clone());
        LOCAL_CLIENT_QUERY_WIRE_CACHE.with(|cell| {
            if let Ok(mut guard) = cell.try_borrow_mut() {
                guard.insert(entry);
            }
        });
        return Ok(out);
    }
    decode_public_message(bytes.as_ref()).map_err(|_| Status::invalid_argument(INVALID_MESSAGE))
}

pub(crate) struct StrictProstCodec<Encode, Decode> {
    marker: PhantomData<(Encode, Decode)>,
}

impl<Encode, Decode> Default for StrictProstCodec<Encode, Decode> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<Encode, Decode> Codec for StrictProstCodec<Encode, Decode>
where
    Encode: PublicMessage + Send + 'static,
    Decode: PublicMessage + Send + 'static,
{
    type Encode = Encode;
    type Decode = Decode;
    type Encoder = StrictProstEncoder<Encode>;
    type Decoder = StrictProstDecoder<Decode>;

    fn encoder(&mut self) -> Self::Encoder {
        StrictProstEncoder {
            marker: PhantomData,
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        StrictProstDecoder {
            marker: PhantomData,
        }
    }
}

pub(crate) struct StrictProstEncoder<MessageType> {
    marker: PhantomData<MessageType>,
}

impl<MessageType> Encoder for StrictProstEncoder<MessageType>
where
    MessageType: PublicMessage,
{
    type Item = MessageType;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        let encoded_len = validate_public_message_encoded_len(&item)
            .map_err(|_| Status::internal("an internal error occurred"))?;
        if destination.remaining_mut() < encoded_len {
            return Err(Status::internal("an internal error occurred"));
        }
        Message::encode_raw(&item, destination);
        Ok(())
    }
}

pub(crate) struct StrictProstDecoder<MessageType> {
    marker: PhantomData<MessageType>,
}

impl<MessageType> Decoder for StrictProstDecoder<MessageType>
where
    MessageType: PublicMessage + 'static,
{
    type Item = MessageType;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let length = source.remaining();
        if length > MessageType::MAX_ENCODED_BYTES {
            source.advance(length);
            return Err(Status::invalid_argument(INVALID_MESSAGE));
        }
        let bytes = source.copy_to_bytes(length);
        if TypeId::of::<MessageType>() == TypeId::of::<crate::app::v1::ExecuteQueryResponse>() {
            let mut slot: Option<MessageType> = None;
            if let Some(target) = (&mut slot as &mut dyn Any)
                .downcast_mut::<Option<crate::app::v1::ExecuteQueryResponse>>()
            {
                *target = Some(decode_cached_execute_query_response(bytes)?);
                return Ok(slot);
            }
        }
        decode_public_message(bytes.as_ref())
            .map(Some)
            .map_err(|_| Status::invalid_argument(INVALID_MESSAGE))
    }
}

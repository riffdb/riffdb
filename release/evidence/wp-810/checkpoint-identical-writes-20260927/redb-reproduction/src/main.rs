#![forbid(unsafe_code)]
use redb::{ReadableDatabase, StorageBackend, TableDefinition};
use std::{io, sync::{Arc, Mutex}};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("rows");
const ROW_COUNT: u64 = 2048;
const VALUE_BYTES: usize = 8192;

#[derive(Debug, Default, Clone, Copy)]
struct Counts { writes: u64, bytes: u64, equal_writes: u64, equal_bytes: u64, syncs: u64 }
#[derive(Debug, Default)]
struct State { phase: usize, counts: [Counts; 3] }
#[derive(Debug)]
struct Observer { file: redb::backends::FileBackend, state: Arc<Mutex<State>> }
impl StorageBackend for Observer {
    fn len(&self) -> io::Result<u64> { self.file.len() }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> { self.file.read(offset, out) }
    fn set_len(&self, len: u64) -> io::Result<()> { self.file.set_len(len) }
    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()?;
        let mut s = self.state.lock().unwrap(); let p=s.phase;
        s.counts[p].syncs += 1; Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let mut equal = offset.checked_add(data.len() as u64).is_some_and(|end| end <= self.file.len().unwrap());
        let mut scratch = [0u8; 8192];
        if equal {
            for (i, chunk) in data.chunks(scratch.len()).enumerate() {
                self.file.read(offset + (i * scratch.len()) as u64, &mut scratch[..chunk.len()])?;
                if scratch[..chunk.len()] != *chunk { equal=false; break; }
            }
        }
        // Observation only: every original write still reaches the real backend.
        self.file.write(offset, data)?;
        let mut s=self.state.lock().unwrap(); let p=s.phase; let c=&mut s.counts[p];
        c.writes+=1; c.bytes+=data.len() as u64;
        if equal { c.equal_writes+=1; c.equal_bytes+=data.len() as u64; }
        Ok(())
    }
    fn close(&self) -> io::Result<()> { self.file.close() }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os().nth(1).expect("new database path");
    let file = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path)?;
    let state=Arc::new(Mutex::new(State::default()));
    let mut builder=redb::Builder::new(); builder.set_cache_size(4*1024*1024);
    let db=builder.create_with_backend(Observer { file:redb::backends::FileBackend::new(file)?, state:state.clone() })?;
    for round in 0..3u64 {
        state.lock().unwrap().counts=[Counts::default();3];
        let mut tx=db.begin_write()?; tx.set_two_phase_commit(false); tx.set_durability(redb::Durability::Immediate)?;
        state.lock().unwrap().phase=1;
        {
            let mut rows=tx.open_table(ROWS)?;
            for row in 0..ROW_COUNT {
                let mut value=vec![0u8; VALUE_BYTES];
                value[..8].copy_from_slice(&row.to_le_bytes()); value[8..16].copy_from_slice(&round.to_le_bytes());
                rows.insert(row,value.as_slice())?;
            }
        }
        state.lock().unwrap().phase=2; tx.commit()?;
        state.lock().unwrap().phase=0;
        let counts=state.lock().unwrap().counts;
        for (phase,c) in counts.iter().enumerate() {
            println!("{{\"round\":{round},\"phase\":{phase},\"writes\":{},\"bytes\":{},\"equal_writes\":{},\"equal_bytes\":{},\"syncs\":{}}}", c.writes,c.bytes,c.equal_writes,c.equal_bytes,c.syncs);
        }
        let read=db.begin_read()?; let rows=read.open_table(ROWS)?;
        for row in 0..ROW_COUNT {
            let value=rows.get(row)?.unwrap();
            assert_eq!(&value.value()[..8],row.to_le_bytes());
            assert_eq!(&value.value()[8..16],round.to_le_bytes());
        }
    }
    drop(db);
    let db=redb::Database::open(&path)?; let read=db.begin_read()?; let rows=read.open_table(ROWS)?;
    for row in 0..ROW_COUNT { assert_eq!(&rows.get(row)?.unwrap().value()[8..16],2u64.to_le_bytes()); }
    Ok(())
}

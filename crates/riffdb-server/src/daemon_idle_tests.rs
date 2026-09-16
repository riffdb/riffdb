use super::*;
use std::sync::atomic::AtomicUsize;
use std::task::{Wake, Waker};
use tokio_stream::StreamExt;

struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn duplex_idle_timeout_wakes_both_read_and_write_pollers() {
    let (io, _peer) = tokio::io::duplex(1);
    let mut incoming = BoundedIncoming::new(
        tokio_stream::iter([Ok::<_, io::Error>(io)]),
        NonZeroU32::new(1).unwrap(),
        Duration::from_secs(1),
    );
    let mut connection = incoming.next().await.unwrap().unwrap();
    let read_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let write_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let read_waker = Waker::from(read_wakes.clone());
    let write_waker = Waker::from(write_wakes.clone());
    let mut read_cx = Context::from_waker(&read_waker);
    let mut write_cx = Context::from_waker(&write_waker);
    let mut bytes = [0; 1];
    let mut buffer = ReadBuf::new(&mut bytes);
    assert!(
        Pin::new(&mut connection)
            .poll_read(&mut read_cx, &mut buffer)
            .is_pending()
    );
    assert!(matches!(
        Pin::new(&mut connection).poll_write(&mut write_cx, b"x"),
        Poll::Ready(Ok(1))
    ));
    assert!(
        Pin::new(&mut connection)
            .poll_write(&mut write_cx, b"y")
            .is_pending()
    );
    read_wakes.0.store(0, Ordering::SeqCst);
    write_wakes.0.store(0, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(read_wakes.0.load(Ordering::SeqCst) > 0);
    assert!(write_wakes.0.load(Ordering::SeqCst) > 0);
    assert!(
        matches!(Pin::new(&mut connection).poll_read(&mut read_cx, &mut buffer), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::TimedOut)
    );
    assert!(
        matches!(Pin::new(&mut connection).poll_write(&mut write_cx, b"y"), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::TimedOut)
    );
}

//! Explicit HTTP/2 response-frame gate for public uncertainty tests.

use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const HTTP2_FRAME_HEADER_BYTES: usize = 9;
const MAX_GATED_FRAME_PAYLOAD_BYTES: usize = 1_048_576;
const IO_POLL: Duration = Duration::from_millis(25);
const ACCEPT_POLL: Duration = Duration::from_millis(10);
const HELD_RELEASE_TIMEOUT: Duration = Duration::from_secs(60);
const DROP_FINISH_TIMEOUT: Duration = Duration::from_secs(5);

const FRAME_DATA: u8 = 0;
const FRAME_HEADERS: u8 = 1;

/// Metadata for the complete response frame retained by the gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeldHttp2Frame {
    /// HTTP/2 frame type.
    pub frame_type: u8,
    /// Nonzero HTTP/2 stream identifier.
    pub stream_id: u32,
    /// Complete payload length buffered by the gate.
    pub payload_bytes: usize,
}

enum GateCommand {
    Arm,
    Release(GateRelease),
    Stop,
}

enum GateRelease {
    IncompletePrefix(usize),
    DropCompleteFrame,
}

/// One loopback proxy that can retain the first application response frame.
///
/// Client-to-server traffic and stream-zero control frames pass through. Once
/// armed, the first `HEADERS` or `DATA` frame on a nonzero stream is read
/// completely into a bounded buffer and reported to the test. The test then
/// chooses either an incomplete byte prefix or no response bytes at all.
pub struct Http2ResponseGate {
    address: SocketAddr,
    commands: SyncSender<GateCommand>,
    held: Receiver<HeldHttp2Frame>,
    finished: Receiver<io::Result<()>>,
    worker: Option<JoinHandle<()>>,
    armed: bool,
    released: bool,
}

impl Http2ResponseGate {
    /// Binds a loopback proxy for one upstream loopback server.
    pub fn bind(upstream: SocketAddr) -> Result<Self, Http2GateError> {
        if !upstream.ip().is_loopback() || upstream.port() == 0 {
            return Err(Http2GateError::InvalidUpstream);
        }
        let listener = TcpListener::bind("127.0.0.1:0").map_err(Http2GateError::Io)?;
        listener.set_nonblocking(true).map_err(Http2GateError::Io)?;
        let address = listener.local_addr().map_err(Http2GateError::Io)?;
        let (commands, command_receiver) = mpsc::sync_channel(4);
        let (held_sender, held) = mpsc::sync_channel(1);
        let (finished_sender, finished) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let result = run_gate(listener, upstream, command_receiver, held_sender);
            let _ = finished_sender.send(result);
        });
        Ok(Self {
            address,
            commands,
            held,
            finished,
            worker: Some(worker),
            armed: false,
            released: false,
        })
    }

    /// Returns the loopback address to which the public client connects.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Arms exactly one response-frame interception.
    pub fn arm(&mut self) -> Result<(), Http2GateError> {
        if self.armed {
            return Err(Http2GateError::AlreadyArmed);
        }
        self.commands
            .send(GateCommand::Arm)
            .map_err(|_| Http2GateError::WorkerDisconnected)?;
        self.armed = true;
        Ok(())
    }

    /// Waits until a complete application response frame is buffered.
    pub fn wait_for_held_frame(
        &self,
        deadline: Duration,
    ) -> Result<HeldHttp2Frame, Http2GateError> {
        if !self.armed {
            return Err(Http2GateError::NotArmed);
        }
        match self.held.recv_timeout(deadline) {
            Ok(frame) => Ok(frame),
            Err(RecvTimeoutError::Timeout) => Err(Http2GateError::HoldTimeout),
            Err(RecvTimeoutError::Disconnected) => Err(Http2GateError::WorkerDisconnected),
        }
    }

    /// Forwards only a strict incomplete prefix of the retained frame, then closes.
    pub fn release_incomplete_prefix(&mut self, prefix_bytes: usize) -> Result<(), Http2GateError> {
        if !self.armed || self.released || prefix_bytes == 0 {
            return Err(Http2GateError::InvalidRelease);
        }
        self.commands
            .send(GateCommand::Release(GateRelease::IncompletePrefix(
                prefix_bytes,
            )))
            .map_err(|_| Http2GateError::WorkerDisconnected)?;
        self.released = true;
        Ok(())
    }

    /// Drops the retained complete frame without forwarding any of its bytes.
    pub fn drop_complete_frame(&mut self) -> Result<(), Http2GateError> {
        if !self.armed || self.released {
            return Err(Http2GateError::InvalidRelease);
        }
        self.commands
            .send(GateCommand::Release(GateRelease::DropCompleteFrame))
            .map_err(|_| Http2GateError::WorkerDisconnected)?;
        self.released = true;
        Ok(())
    }

    /// Waits for proxy completion and joins all forwarding threads.
    pub fn finish(mut self, deadline: Duration) -> Result<(), Http2GateError> {
        self.finish_inner(deadline)
    }

    fn finish_inner(&mut self, deadline: Duration) -> Result<(), Http2GateError> {
        let result = match self.finished.recv_timeout(deadline) {
            Ok(result) => result.map_err(Http2GateError::Io),
            Err(RecvTimeoutError::Timeout) => Err(Http2GateError::FinishTimeout),
            Err(RecvTimeoutError::Disconnected) => Err(Http2GateError::WorkerDisconnected),
        };
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| Http2GateError::WorkerPanicked)?;
        }
        result
    }
}

impl fmt::Debug for Http2ResponseGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Http2ResponseGate")
            .field("address", &self.address)
            .field("armed", &self.armed)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl Drop for Http2ResponseGate {
    fn drop(&mut self) {
        if self.worker.is_none() {
            return;
        }
        let _ = self.commands.send(GateCommand::Stop);
        let _ = TcpStream::connect_timeout(&self.address, IO_POLL);
        let _ = self.finish_inner(DROP_FINISH_TIMEOUT);
    }
}

fn run_gate(
    listener: TcpListener,
    upstream: SocketAddr,
    commands: Receiver<GateCommand>,
    held: SyncSender<HeldHttp2Frame>,
) -> io::Result<()> {
    let Some((mut client, mut armed)) = accept_client(&listener, &commands)? else {
        return Ok(());
    };
    client.set_nodelay(true)?;
    let mut server = TcpStream::connect_timeout(&upstream, Duration::from_secs(5))?;
    server.set_nodelay(true)?;
    server.set_read_timeout(Some(IO_POLL))?;

    let mut client_reader = client.try_clone()?;
    let mut server_writer = server.try_clone()?;
    let request_forwarder = thread::spawn(move || {
        let result = io::copy(&mut client_reader, &mut server_writer);
        let _ = server_writer.shutdown(Shutdown::Write);
        result
    });

    let response_result =
        forward_response_frames(&mut server, &mut client, &commands, &held, &mut armed);
    let _ = client.shutdown(Shutdown::Both);
    let _ = server.shutdown(Shutdown::Both);
    let request_result = request_forwarder
        .join()
        .map_err(|_| io::Error::other("HTTP/2 request forwarder panicked"))?;
    response_result?;
    match request_result {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::NotConnected
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn accept_client(
    listener: &TcpListener,
    commands: &Receiver<GateCommand>,
) -> io::Result<Option<(TcpStream, bool)>> {
    let mut armed = false;
    loop {
        drain_control_before_hold(commands, &mut armed)?;
        match listener.accept() {
            Ok((stream, peer)) if peer.ip().is_loopback() => {
                return Ok(Some((stream, armed)));
            }
            Ok((_stream, _peer)) => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "HTTP/2 gate rejected a non-loopback peer",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                match commands.recv_timeout(ACCEPT_POLL) {
                    Ok(GateCommand::Arm) => armed = true,
                    Ok(GateCommand::Stop) | Err(RecvTimeoutError::Disconnected) => {
                        return Ok(None);
                    }
                    Ok(GateCommand::Release(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "HTTP/2 gate released before holding a frame",
                        ));
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn forward_response_frames(
    server: &mut TcpStream,
    client: &mut TcpStream,
    commands: &Receiver<GateCommand>,
    held: &SyncSender<HeldHttp2Frame>,
    armed: &mut bool,
) -> io::Result<()> {
    loop {
        let mut header = [0_u8; HTTP2_FRAME_HEADER_BYTES];
        if !read_exact_interruptible(server, &mut header, commands, armed)? {
            return Ok(());
        }
        let payload_bytes = decode_payload_length(header);
        if payload_bytes > MAX_GATED_FRAME_PAYLOAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP/2 response frame exceeded the gate bound",
            ));
        }
        let mut payload = vec![0_u8; payload_bytes];
        if !read_exact_interruptible(server, &mut payload, commands, armed)? {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "HTTP/2 response ended inside a frame",
            ));
        }
        drain_control_before_hold(commands, armed)?;

        let stream_id = decode_stream_id(header);
        let frame_type = header[3];
        if *armed && stream_id != 0 && matches!(frame_type, FRAME_DATA | FRAME_HEADERS) {
            held.send(HeldHttp2Frame {
                frame_type,
                stream_id,
                payload_bytes,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "gate observer disconnected"))?;
            return release_held_frame(header, &payload, client, commands);
        }
        client.write_all(&header)?;
        client.write_all(&payload)?;
        client.flush()?;
    }
}

fn read_exact_interruptible(
    stream: &mut TcpStream,
    output: &mut [u8],
    commands: &Receiver<GateCommand>,
    armed: &mut bool,
) -> io::Result<bool> {
    let mut offset = 0usize;
    while offset < output.len() {
        drain_control_before_hold(commands, armed)?;
        match stream.read(&mut output[offset..]) {
            Ok(0) if offset == 0 => return Ok(false),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "HTTP/2 response ended inside a frame",
                ));
            }
            Ok(read) => offset = offset.saturating_add(read),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

fn drain_control_before_hold(commands: &Receiver<GateCommand>, armed: &mut bool) -> io::Result<()> {
    loop {
        match commands.try_recv() {
            Ok(GateCommand::Arm) => *armed = true,
            Ok(GateCommand::Stop) | Err(TryRecvError::Disconnected) => {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "HTTP/2 gate stopped",
                ));
            }
            Ok(GateCommand::Release(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "HTTP/2 gate released before holding a frame",
                ));
            }
            Err(TryRecvError::Empty) => return Ok(()),
        }
    }
}

fn release_held_frame(
    header: [u8; HTTP2_FRAME_HEADER_BYTES],
    payload: &[u8],
    client: &mut TcpStream,
    commands: &Receiver<GateCommand>,
) -> io::Result<()> {
    let release = loop {
        match commands.recv_timeout(HELD_RELEASE_TIMEOUT) {
            Ok(GateCommand::Release(release)) => break release,
            Ok(GateCommand::Arm) => {}
            Ok(GateCommand::Stop) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
            Err(RecvTimeoutError::Timeout) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP/2 held-frame release timed out",
                ));
            }
        }
    };
    match release {
        GateRelease::DropCompleteFrame => Ok(()),
        GateRelease::IncompletePrefix(prefix_bytes) => {
            let frame_bytes = header.len().saturating_add(payload.len());
            if prefix_bytes >= frame_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "HTTP/2 gate prefix would complete the held frame",
                ));
            }
            if prefix_bytes <= header.len() {
                client.write_all(&header[..prefix_bytes])?;
            } else {
                client.write_all(&header)?;
                client.write_all(&payload[..prefix_bytes - header.len()])?;
            }
            client.flush()
        }
    }
}

fn decode_payload_length(header: [u8; HTTP2_FRAME_HEADER_BYTES]) -> usize {
    usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2])
}

fn decode_stream_id(header: [u8; HTTP2_FRAME_HEADER_BYTES]) -> u32 {
    u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]])
}

/// Closed HTTP/2 gate failure.
#[derive(Debug)]
pub enum Http2GateError {
    /// The upstream server was not a bound loopback address.
    InvalidUpstream,
    /// The gate was armed twice.
    AlreadyArmed,
    /// A hold was requested before arming.
    NotArmed,
    /// A release was duplicated or invalid.
    InvalidRelease,
    /// No response frame arrived before the explicit deadline.
    HoldTimeout,
    /// The worker did not finish before the explicit deadline.
    FinishTimeout,
    /// The worker channel disconnected unexpectedly.
    WorkerDisconnected,
    /// The worker thread panicked.
    WorkerPanicked,
    /// Socket or proxy I/O failed.
    Io(io::Error),
}

impl fmt::Display for Http2GateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidUpstream => "HTTP/2 gate upstream is invalid",
            Self::AlreadyArmed => "HTTP/2 gate is already armed",
            Self::NotArmed => "HTTP/2 gate is not armed",
            Self::InvalidRelease => "HTTP/2 gate release is invalid",
            Self::HoldTimeout => "HTTP/2 response hold timed out",
            Self::FinishTimeout => "HTTP/2 gate finish timed out",
            Self::WorkerDisconnected => "HTTP/2 gate worker disconnected",
            Self::WorkerPanicked => "HTTP/2 gate worker panicked",
            Self::Io(_) => "HTTP/2 gate I/O failed",
        })
    }
}

impl Error for Http2GateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_header_decoding_ignores_reserved_stream_bit() {
        let header = [0x00, 0x01, 0x02, FRAME_HEADERS, 0, 0x80, 0x00, 0x00, 0x07];
        assert_eq!(decode_payload_length(header), 258);
        assert_eq!(decode_stream_id(header), 7);
    }

    #[test]
    fn only_data_and_headers_are_application_response_frames() {
        assert!(matches!(FRAME_DATA, 0));
        assert!(matches!(FRAME_HEADERS, 1));
        const {
            assert!(MAX_GATED_FRAME_PAYLOAD_BYTES < 16_777_216);
        }
    }
}

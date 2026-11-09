//! Bounded OTLP HTTP adapter over the existing Hyper and verified Rustls stack.
use crate::config::telemetry::Signal;
use crate::config::{TelemetryPushConfig, TelemetryPushEndpoint};
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::TokioIo;
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use serde::Deserialize;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;

pub(crate) const MAX_REQUEST_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_RESPONSE_BYTES: usize = 65536;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeliveryReceipt {
    pub(crate) accepted: u64,
    pub(crate) rejected: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExportFailure {
    Encoding,
    RequestLimit,
    Dns,
    Connect,
    Tls,
    Transport,
    ResponseLimit,
    ResponseInvalid,
    Http(u16),
    Timeout,
    Shutdown,
    Credential,
}
impl fmt::Display for ExportFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Encoding => "telemetry_encoding_failed",
            Self::RequestLimit => "telemetry_request_limit",
            Self::Dns => "telemetry_dns_failed",
            Self::Connect => "telemetry_connect_failed",
            Self::Tls => "telemetry_tls_failed",
            Self::Transport => "telemetry_transport_failed",
            Self::ResponseLimit => "telemetry_response_limit",
            Self::ResponseInvalid => "telemetry_response_invalid",
            Self::Http(_) => "telemetry_http_rejected",
            Self::Timeout => "telemetry_export_timeout",
            Self::Shutdown => "telemetry_shutdown_deadline",
            Self::Credential => "telemetry_credential_unavailable",
        })
    }
}
impl std::error::Error for ExportFailure {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SetupFailure {
    Credential,
    TrustRoot,
}

/// Exactly one in-flight SDK batch owns this callback state per signal.
pub(crate) struct BatchContext {
    count: AtomicU64,
    pub(crate) written: AtomicBool,
    receipt: Mutex<Option<Result<DeliveryReceipt, ExportFailure>>>,
    drain: watch::Sender<Option<Instant>>,
}
impl BatchContext {
    pub(crate) fn new() -> Self {
        let (drain, _) = watch::channel(None);
        Self {
            count: AtomicU64::new(0),
            written: AtomicBool::new(false),
            receipt: Mutex::new(None),
            drain,
        }
    }
    pub(crate) fn begin(&self, count: usize) {
        self.count.store(count as u64, Ordering::Release);
        self.written.store(false, Ordering::Release);
        *self
            .receipt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
    pub(crate) fn take_receipt(&self) -> Option<Result<DeliveryReceipt, ExportFailure>> {
        self.receipt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
    pub(crate) fn begin_drain(&self, deadline: Instant) {
        self.drain.send_modify(|current| {
            *current = Some(current.map_or(deadline, |old| old.min(deadline)))
        });
    }
}

type PendingResolution = JoinHandle<Result<Vec<SocketAddr>, ExportFailure>>;

struct Material {
    bearer: Option<zeroize::Zeroizing<String>>,
    tls: Option<Arc<tokio_rustls::rustls::ClientConfig>>,
}
pub(crate) struct OtlpHttpClient {
    endpoint: TelemetryPushEndpoint,
    config: TelemetryPushConfig,
    dns: AsyncMutex<Option<PendingResolution>>,
    timeout: Duration,
    signal: Signal,
    pub(crate) batch: Arc<BatchContext>,
}
impl fmt::Debug for OtlpHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpHttpClient")
            .field("signal", &self.signal)
            .field("endpoint", &"[CONFIGURED]")
            .finish()
    }
}
impl OtlpHttpClient {
    pub(crate) fn new(
        signal: Signal,
        config: &TelemetryPushConfig,
        timeout: Duration,
        batch: Arc<BatchContext>,
    ) -> Result<Self, SetupFailure> {
        let client = Self {
            endpoint: config.endpoint().clone(),
            config: config.clone(),
            dns: AsyncMutex::new(None),
            timeout,
            signal,
            batch,
        };
        client.material()?;
        Ok(client)
    }
    /// Preserve protected bearer/root rotation between exports while retaining
    /// the one pending DNS lookup and the immutable checked endpoint selection.
    fn material(&self) -> Result<Material, SetupFailure> {
        let bearer = self
            .config
            .bearer_token_path()
            .map(|path| {
                crate::telemetry_exporter::load_bearer_token(path.as_path())
                    .map_err(|_| SetupFailure::Credential)
            })
            .transpose()?;
        let tls = match &self.endpoint {
            TelemetryPushEndpoint::Https {
                host,
                ca_certificate_path,
                ..
            } => {
                rustls_pki_types::ServerName::try_from(host.clone())
                    .map_err(|_| SetupFailure::TrustRoot)?;
                Some(
                    crate::telemetry_exporter::build_tls_client_config(
                        ca_certificate_path.as_ref().map(|path| path.as_path()),
                    )
                    .map_err(|_| SetupFailure::TrustRoot)?,
                )
            }
            _ => None,
        };
        Ok(Material { bearer, tls })
    }
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ExportFailure> {
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        // Keep the one lookup alive across export timeout. Repeated batches may
        // poll it, but can never accumulate blocked resolver jobs.
        let mut pending = self.dns.lock().await;
        if pending.is_none() {
            let host = host.to_owned();
            *pending = Some(tokio::spawn(async move {
                let addresses = tokio::net::lookup_host((host.as_str(), port))
                    .await
                    .map_err(|_| ExportFailure::Dns)?
                    .take(16)
                    .collect::<Vec<_>>();
                if addresses.is_empty() {
                    Err(ExportFailure::Dns)
                } else {
                    Ok(addresses)
                }
            }));
        }
        let result = match pending.as_mut() {
            Some(task) => task
                .await
                .map_err(|_| ExportFailure::Dns)
                .and_then(|result| result),
            None => return Err(ExportFailure::Dns),
        };
        *pending = None;
        result
    }
    async fn request(
        &self,
        body: Bytes,
        material: &Material,
    ) -> Result<Response<Bytes>, ExportFailure> {
        match &self.endpoint {
            TelemetryPushEndpoint::LoopbackHttp { address, .. } => {
                let stream = tokio::net::TcpStream::connect(address)
                    .await
                    .map_err(|_| ExportFailure::Connect)?;
                self.exchange(stream, body, material).await
            }
            TelemetryPushEndpoint::Https { host, port, .. } => {
                let addresses = self.resolve(host, *port).await?;
                let mut connected = None;
                for address in addresses {
                    if let Ok(stream) = tokio::net::TcpStream::connect(address).await {
                        connected = Some(stream);
                        break;
                    }
                }
                let stream = connected.ok_or(ExportFailure::Connect)?;
                let tls = material.tls.as_ref().ok_or(ExportFailure::Tls)?;
                let name = rustls_pki_types::ServerName::try_from(host.clone())
                    .map_err(|_| ExportFailure::Tls)?;
                let stream = TlsConnector::from(Arc::clone(tls))
                    .connect(name, stream)
                    .await
                    .map_err(|_| ExportFailure::Tls)?;
                self.exchange(stream, body, material).await
            }
        }
    }
    async fn exchange<S>(
        &self,
        stream: S,
        body: Bytes,
        material: &Material,
    ) -> Result<Response<Bytes>, ExportFailure>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut builder = hyper::client::conn::http1::Builder::new();
        builder.max_buf_size(16384).max_headers(64);
        let (mut sender, connection) = builder
            .handshake(TokioIo::new(stream))
            .await
            .map_err(|_| ExportFailure::Transport)?;
        struct Driver(JoinHandle<()>);
        impl Drop for Driver {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let result = async {
            let (authority, path) = match &self.endpoint {
                TelemetryPushEndpoint::LoopbackHttp {
                    authority, path, ..
                }
                | TelemetryPushEndpoint::Https {
                    authority, path, ..
                } => (authority, path),
            };
            // SDK headers, URLs, credentials and ambient OTEL_* headers are never
            // forwarded. Only this checked configuration controls the request.
            let mut request = Request::post(path)
                .header("host", authority)
                .header("content-type", "application/json")
                .header("connection", "close")
                .body(Full::new(body))
                .map_err(|_| ExportFailure::Encoding)?;
            if let Some(token) = &material.bearer {
                let mut value =
                    axum::http::HeaderValue::from_str(&format!("Bearer {}", token.as_str()))
                        .map_err(|_| ExportFailure::Encoding)?;
                value.set_sensitive(true);
                request.headers_mut().insert("authorization", value);
            }
            let response = sender
                .send_request(request)
                .await
                .map_err(|_| ExportFailure::Transport)?;
            let (parts, mut body) = response.into_parts();
            let mut received = Vec::new();
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|_| ExportFailure::ResponseInvalid)?;
                if let Ok(bytes) = frame.into_data() {
                    if received.len().saturating_add(bytes.len()) > MAX_RESPONSE_BYTES {
                        return Err(ExportFailure::ResponseLimit);
                    }
                    received.extend_from_slice(&bytes);
                }
            }
            Ok(Response::from_parts(parts, Bytes::from(received)))
        }
        .await;
        drop(driver);
        result
    }
    async fn attempts(
        &self,
        body: Bytes,
        expected: u64,
        material: &Material,
    ) -> Result<(Response<Bytes>, DeliveryReceipt), ExportFailure> {
        for attempt in 0..3 {
            let result = self.request(body.clone(), material).await;
            let delay = match result {
                Ok(response) => {
                    let status = response.status().as_u16();
                    if response.status().is_success() {
                        let receipt = parse_receipt(self.signal, response.body(), expected)?;
                        // Count known writing only after a valid receipt. An early
                        // HTTP refusal alone cannot prove the full body was sent.
                        self.batch.written.store(true, Ordering::Release);
                        return Ok((response, receipt));
                    }
                    let retryable = matches!(status, 408 | 429 | 500 | 502 | 503 | 504);
                    if !retryable || attempt == 2 {
                        return Err(ExportFailure::Http(status));
                    }
                    match retry_delay(response.headers(), attempt) {
                        Some(delay) => delay,
                        None => return Err(ExportFailure::Http(status)),
                    }
                }
                Err(error)
                    if attempt == 2
                        || matches!(
                            error,
                            ExportFailure::ResponseInvalid
                                | ExportFailure::ResponseLimit
                                | ExportFailure::Tls
                        ) =>
                {
                    return Err(error);
                }
                Err(_) => Duration::from_millis(100u64 << attempt),
            };
            tokio::time::sleep(delay).await;
        }
        Err(ExportFailure::Transport)
    }
    async fn send(
        &self,
        body: Bytes,
        expected: u64,
        snapshot_deadline: Option<Instant>,
    ) -> Result<(Response<Bytes>, DeliveryReceipt), ExportFailure> {
        if body.len() > MAX_REQUEST_BYTES {
            return Err(ExportFailure::RequestLimit);
        }
        let deadline = Instant::now() + self.timeout;
        let deadline = snapshot_deadline.map_or(deadline, |snapshot| snapshot.min(deadline));
        let mut drain = self.batch.drain.subscribe();
        if drain.borrow().is_some_and(|drain| drain <= Instant::now()) {
            return Err(ExportFailure::Shutdown);
        }
        let material = self.material().map_err(|failure| match failure {
            SetupFailure::Credential => ExportFailure::Credential,
            SetupFailure::TrustRoot => ExportFailure::Tls,
        })?;
        let operation = self.attempts(body, expected, &material);
        tokio::pin!(operation);
        loop {
            let final_deadline = *drain.borrow_and_update();
            let deadline = final_deadline.map_or(deadline, |drain| drain.min(deadline));
            if deadline <= Instant::now() {
                return Err(if final_deadline.is_some() {
                    ExportFailure::Shutdown
                } else {
                    ExportFailure::Timeout
                });
            }
            tokio::select! {
                biased;
                result=&mut operation=>return result,
                _=tokio::time::sleep_until(deadline.into())=>return Err(if final_deadline.is_some_and(|drain|drain<=deadline){ExportFailure::Shutdown}else{ExportFailure::Timeout}),
                changed=drain.changed()=>if changed.is_err(){return Err(ExportFailure::Shutdown);},
            }
        }
    }
    pub(crate) async fn send_payload(
        &self,
        body: Bytes,
        expected: usize,
        deadline: Instant,
    ) -> Result<DeliveryReceipt, ExportFailure> {
        self.send(body, expected as u64, Some(deadline))
            .await
            .map(|(_, receipt)| receipt)
    }
}
impl HttpClient for OtlpHttpClient {
    fn send_bytes<'a, 'b>(
        &'a self,
        request: Request<Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<Bytes>, HttpError>> + Send + 'b>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            let result = self
                .send(
                    request.into_body(),
                    self.batch.count.load(Ordering::Acquire),
                    None,
                )
                .await;
            *self
                .batch
                .receipt
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(
                result
                    .as_ref()
                    .map(|(_, receipt)| *receipt)
                    .map_err(|error| *error),
            );
            result
                .map(|(response, _)| response)
                .map_err(|error| Box::new(error) as HttpError)
        })
    }
}
fn retry_delay(headers: &axum::http::HeaderMap, attempt: u32) -> Option<Duration> {
    retry_delay_at(headers, attempt, SystemTime::now())
}
fn retry_delay_at(
    headers: &axum::http::HeaderMap,
    attempt: u32,
    now: SystemTime,
) -> Option<Duration> {
    let mut values = headers.get_all("retry-after").iter();
    let Some(value) = values.next() else {
        return Some(Duration::from_millis(100u64 << attempt));
    };
    if values.next().is_some() {
        return None;
    }
    let value = value.to_str().ok()?;
    if value.len() > 64 {
        return None;
    }
    let delay = if value.bytes().all(|byte| byte.is_ascii_digit()) && !value.is_empty() {
        Duration::from_secs(value.parse().ok()?)
    } else {
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now)
            .unwrap_or_default()
    };
    // Never retry before the collector's requested delay. An excessive request
    // loses this best-effort batch rather than overriding collector backpressure.
    (delay <= Duration::from_secs(2)).then_some(delay)
}
struct MapOnly<T>(T);
impl<'de, T: Deserialize<'de>> Deserialize<'de> for MapOnly<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for ObjectVisitor<T> {
            type Value = MapOnly<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an OTLP response object")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                map: M,
            ) -> Result<Self::Value, M::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(MapOnly)
            }
        }
        deserializer.deserialize_map(ObjectVisitor(std::marker::PhantomData))
    }
}
#[derive(Deserialize)]
struct Reply {
    #[serde(default, rename = "partialSuccess", alias = "partial_success")]
    partial: Option<MapOnly<Partial>>,
}
#[derive(Deserialize)]
struct Partial {
    #[serde(default, rename = "errorMessage", alias = "error_message")]
    _message: Option<IgnoredMessage>,
    #[serde(default, rename = "rejectedLogRecords", alias = "rejected_log_records")]
    logs: Option<Count>,
    #[serde(default, rename = "rejectedSpans", alias = "rejected_spans")]
    traces: Option<Count>,
    #[serde(default, rename = "rejectedDataPoints", alias = "rejected_data_points")]
    metrics: Option<Count>,
}
/// Validate the standard field's string type without retaining collector prose.
struct IgnoredMessage;
impl<'de> Deserialize<'de> for IgnoredMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StringOnly;
        impl serde::de::Visitor<'_> for StringOnly {
            type Value = IgnoredMessage;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an OTLP message string")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<IgnoredMessage, E> {
                if value.len() <= MAX_RESPONSE_BYTES {
                    Ok(IgnoredMessage)
                } else {
                    Err(E::custom("OTLP response message exceeds its bound"))
                }
            }
        }
        deserializer.deserialize_str(StringOnly)
    }
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Count {
    Number(u64),
    Text(String),
}
impl Count {
    fn checked(self) -> Result<u64, ExportFailure> {
        match self {
            Self::Number(value) => Ok(value),
            Self::Text(value)
                if !value.is_empty()
                    && value.len() <= 20
                    && value.bytes().all(|byte| byte.is_ascii_digit()) =>
            {
                value.parse().map_err(|_| ExportFailure::ResponseInvalid)
            }
            _ => Err(ExportFailure::ResponseInvalid),
        }
    }
}
fn parse_receipt(
    signal: Signal,
    body: &[u8],
    expected: u64,
) -> Result<DeliveryReceipt, ExportFailure> {
    let MapOnly(reply): MapOnly<Reply> =
        serde_json::from_slice(body).map_err(|_| ExportFailure::ResponseInvalid)?;
    let rejected = match reply.partial {
        None => 0,
        Some(MapOnly(partial)) => {
            let count = match signal {
                Signal::Logs if partial.traces.is_none() && partial.metrics.is_none() => {
                    partial.logs
                }
                Signal::Traces if partial.logs.is_none() && partial.metrics.is_none() => {
                    partial.traces
                }
                Signal::Metrics if partial.logs.is_none() && partial.traces.is_none() => {
                    partial.metrics
                }
                _ => return Err(ExportFailure::ResponseInvalid),
            };
            count.map(Count::checked).transpose()?.unwrap_or(0)
        }
    };
    if rejected > expected {
        return Err(ExportFailure::ResponseInvalid);
    }
    Ok(DeliveryReceipt {
        accepted: expected - rejected,
        rejected,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: ID-005, NET-002, NET-003, END-007, END-008
    #[test]
    fn collector_acceptance_requires_a_complete_unambiguous_signal_specific_receipt() {
        assert_eq!(
            parse_receipt(Signal::Logs, b"{}", 7).unwrap(),
            DeliveryReceipt {
                accepted: 7,
                rejected: 0
            }
        );
        assert_eq!(
            parse_receipt(
                Signal::Logs,
                br#"{"partialSuccess":{"rejectedLogRecords":"2","errorMessage":"secret-canary"}}"#,
                7
            )
            .unwrap(),
            DeliveryReceipt {
                accepted: 5,
                rejected: 2
            }
        );
        for body in [
            b"".as_slice(),
            b"{",
            b"{} {}",
            b"[]",
            br#"{"partialSuccess":[]}"#,
            br#"{"partialSuccess":{},"partial_success":{}}"#,
            br#"{"partialSuccess":{"rejectedLogRecords":1,"rejectedLogRecords":2}}"#,
            br#"{"partialSuccess":{"rejectedLogRecords":8}}"#,
            br#"{"partialSuccess":{"rejectedLogRecords":-1}}"#,
            br#"{"partialSuccess":{"rejectedSpans":1}}"#,
            br#"{"partialSuccess":{"errorMessage":123}}"#,
            br#"{"partialSuccess":{"errorMessage":"first","error_message":"second"}}"#,
        ] {
            assert_eq!(
                parse_receipt(Signal::Logs, body, 7),
                Err(ExportFailure::ResponseInvalid)
            );
        }
        assert!(!format!("{}", ExportFailure::ResponseInvalid).contains("secret-canary"));
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    // req: ID-005, NET-002, NET-003, END-007, END-008
    #[tokio::test]
    async fn http_adapter_requires_complete_receipts_and_discards_sdk_destinations_and_headers() {
        for (reply,expected) in [
            (b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_vec(),Ok(DeliveryReceipt {accepted:1,rejected:0})),
            (b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n{}".to_vec(),Err(ExportFailure::ResponseInvalid)),
            (b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]".to_vec(),Err(ExportFailure::ResponseInvalid)),
            (b"HTTP/1.1 302 Found\r\nLocation: http://example.com/secret-canary\r\nContent-Length: 0\r\n\r\n".to_vec(),Err(ExportFailure::Http(302))),
            ([b"HTTP/1.1 200 OK\r\nContent-Length: 65537\r\n\r\n".as_slice(),&vec![b' ';65537]].concat(),Err(ExportFailure::ResponseLimit)),
        ] {
            let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
            let collector=tokio::spawn(async move {
                let (mut stream,_)=listener.accept().await.unwrap();
                let mut request=Vec::new();let mut buffer=[0u8;1024];
                while !request.ends_with(b"\r\n\r\n{}") {
                    let count=stream.read(&mut buffer).await.unwrap();assert!(count>0);request.extend_from_slice(&buffer[..count]);assert!(request.len()<4096);
                }
                let request=String::from_utf8(request).unwrap();assert!(request.starts_with("POST /selected/v1/logs HTTP/1.1\r\n"));assert!(!request.contains("secret-canary"));
                stream.write_all(&reply).await.unwrap();stream.shutdown().await.unwrap();
            });
            let config=TelemetryPushConfig::new(crate::config::parse_telemetry_push_endpoint(&format!("http://{address}/selected/v1/logs"),None).unwrap(),Duration::from_secs(1),None);
            let batch=Arc::new(BatchContext::new());batch.begin(1);
            let client=OtlpHttpClient::new(Signal::Logs,&config,Duration::from_secs(2),batch.clone()).unwrap();
            let request=Request::post("http://example.com/secret-canary").header("authorization","secret-canary").header("x-secret","secret-canary").body(Bytes::from_static(b"{}")).unwrap();
            let _=client.send_bytes(request).await;
            assert_eq!(batch.take_receipt(),Some(expected));assert_eq!(batch.written.load(Ordering::Acquire),expected.is_ok());
            collector.await.unwrap();
        }
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    // req: ID-005, NET-002, NET-003, END-007, END-008
    #[test]
    fn retry_after_uses_a_controlled_clock_and_never_overrides_collector_backpressure() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let mut headers = axum::http::HeaderMap::new();
        assert_eq!(
            retry_delay_at(&headers, 0, now),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            retry_delay_at(&headers, 1, now),
            Some(Duration::from_millis(200))
        );
        for (value, expected) in [
            ("0", Some(Duration::ZERO)),
            ("2", Some(Duration::from_secs(2))),
            ("3", None),
            ("invalid", None),
        ] {
            headers.insert("retry-after", value.parse().unwrap());
            assert_eq!(retry_delay_at(&headers, 0, now), expected);
        }
        headers.insert(
            "retry-after",
            httpdate::fmt_http_date(now + Duration::from_secs(1))
                .parse()
                .unwrap(),
        );
        assert_eq!(
            retry_delay_at(&headers, 0, now),
            Some(Duration::from_secs(1))
        );
        headers.insert(
            "retry-after",
            httpdate::fmt_http_date(now - Duration::from_secs(1))
                .parse()
                .unwrap(),
        );
        assert_eq!(retry_delay_at(&headers, 0, now), Some(Duration::ZERO));
        headers.append("retry-after", "0".parse().unwrap());
        assert_eq!(retry_delay_at(&headers, 0, now), None);
    }
    // req: ID-005, NET-002, NET-003, END-007, END-008
    #[tokio::test]
    async fn otlp_partial_rejection_and_retry_after_are_accounted_exactly() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        for (status, retry_after, requests, recover, partial, expected) in [
            (503, "0", 3, false, None, Err(ExportFailure::Http(503))),
            (429, "0", 3, false, None, Err(ExportFailure::Http(429))),
            (429, "3", 1, false, None, Err(ExportFailure::Http(429))),
            (400, "0", 1, false, None, Err(ExportFailure::Http(400))),
            (
                503,
                "0",
                2,
                true,
                None,
                Ok(DeliveryReceipt {
                    accepted: 7,
                    rejected: 0,
                }),
            ),
            (
                200,
                "0",
                1,
                false,
                Some(2),
                Ok(DeliveryReceipt {
                    accepted: 5,
                    rejected: 2,
                }),
            ),
            (
                200,
                "0",
                1,
                false,
                Some(7),
                Ok(DeliveryReceipt {
                    accepted: 0,
                    rejected: 7,
                }),
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let collector = tokio::spawn(async move {
                for index in 0..requests {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0; 1024];
                    while !request.ends_with(b"\r\n\r\n{}") {
                        let count = stream.read(&mut buffer).await.unwrap();
                        assert!(count > 0);
                        request.extend_from_slice(&buffer[..count]);
                        assert!(request.len() < 4096);
                    }
                    let code = if recover && index == requests - 1 {
                        200
                    } else {
                        status
                    };
                    let body=partial.map_or_else(||"{}".to_owned(),|rejected|format!(r#"{{"partialSuccess":{{"rejectedDataPoints":"{rejected}","errorMessage":"collector-secret-canary"}}}}"#));
                    let response = format!(
                        "HTTP/1.1 {code} Result\r\nRetry-After: {retry_after}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            });
            let config = TelemetryPushConfig::new(
                crate::config::parse_telemetry_push_endpoint(
                    &format!("http://{address}/v1/metrics"),
                    None,
                )
                .unwrap(),
                Duration::from_secs(1),
                None,
            );
            let client = OtlpHttpClient::new(
                Signal::Metrics,
                &config,
                Duration::from_secs(2),
                Arc::new(BatchContext::new()),
            )
            .unwrap();
            assert_eq!(
                client
                    .send_payload(
                        Bytes::from_static(b"{}"),
                        7,
                        Instant::now() + Duration::from_secs(2)
                    )
                    .await,
                expected
            );
            tokio::time::timeout(Duration::from_secs(3), collector)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[cfg(all(test, unix))]
mod material_tests {
    use super::*;
    use axum::{Router, extract::State, routing::post};
    use std::os::unix::fs::PermissionsExt as _;
    // req: ID-005, NET-002, NET-003, END-007, END-008
    #[tokio::test]
    async fn protected_collector_credentials_rotate_between_exports_and_invalid_material_fails_closed()
     {
        let directory = tempfile::tempdir().unwrap();
        let token = directory.path().join("collector.token");
        std::fs::write(&token, "first-secret-canary").unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        async fn collect(
            State(sender): State<tokio::sync::mpsc::Sender<String>>,
            headers: axum::http::HeaderMap,
            _body: axum::body::Bytes,
        ) -> &'static str {
            sender
                .send(headers["authorization"].to_str().unwrap().to_owned())
                .await
                .unwrap();
            "{}"
        }
        let router = Router::new()
            .route("/v1/metrics", post(collect))
            .with_state(sender);
        let collector = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let config = TelemetryPushConfig::new(
            crate::config::parse_telemetry_push_endpoint(
                &format!("http://{address}/v1/metrics"),
                None,
            )
            .unwrap(),
            Duration::from_secs(1),
            Some(riffdb_config::ProtectedFilePath::new(token.clone()).unwrap()),
        );
        let client = OtlpHttpClient::new(
            Signal::Metrics,
            &config,
            Duration::from_secs(2),
            Arc::new(BatchContext::new()),
        )
        .unwrap();
        for secret in ["first-secret-canary", "second-secret-canary"] {
            std::fs::write(&token, secret).unwrap();
            assert_eq!(
                client
                    .send_payload(
                        Bytes::from_static(b"{}"),
                        1,
                        Instant::now() + Duration::from_secs(2)
                    )
                    .await
                    .unwrap()
                    .accepted,
                1
            );
            assert_eq!(receiver.recv().await.unwrap(), format!("Bearer {secret}"));
        }
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = client
            .send_payload(
                Bytes::from_static(b"{}"),
                1,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert_eq!(error, ExportFailure::Credential);
        assert!(!error.to_string().contains("canary"));
        assert!(!format!("{client:?}").contains("canary"));
        assert!(receiver.try_recv().is_err());
        collector.abort();
    }
}

//! Optional W3C identity and retained tracing context; never transport authority.
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
};
use riffdb_config::W3cTraceParent;
use riffdb_types::ServiceIngressKindV1;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

pub(crate) const TARGET: &str = "riffdb.transport";
fn parent(headers: &axum::http::HeaderMap) -> Option<W3cTraceParent> {
    let mut values = headers.get_all("traceparent").iter();
    let first = values.next()?;
    if values.next().is_some() {
        return None;
    }
    W3cTraceParent::parse(first.to_str().ok()?).ok()
}
pub(crate) async fn grpc(request: Request, next: Next) -> Response {
    observe(request, next, ServiceIngressKindV1::Grpc).await
}
pub(crate) async fn mcp(request: Request, next: Next) -> Response {
    observe(request, next, ServiceIngressKindV1::McpHttp).await
}
async fn observe(mut request: Request, next: Next, transport: ServiceIngressKindV1) -> Response {
    let Some(permit) = riffdb_observability::try_admit_diagnostic_span().map(std::sync::Arc::new)
    else {
        return next.run(request).await;
    };
    let span = riffdb_observability::with_diagnostic_span_admission(
        permit,
        || tracing::info_span!(target:TARGET,"riffdb.ingress",transport=u64::from(transport.tag())),
    )
    .unwrap_or_else(tracing::Span::none);
    if let Some(parent) = parent(request.headers()) {
        let context = opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_bytes(parent.trace_id_bytes()),
            SpanId::from_bytes(parent.parent_span_id_bytes()),
            if parent.sampled() {
                TraceFlags::SAMPLED
            } else {
                TraceFlags::default()
            },
            true,
            TraceState::default(),
        ));
        // An absent trace layer is normal when this optional signal is disabled.
        let _ = span.set_parent(context);
    }
    let parent = span.in_scope(riffdb_observability::RequestSpanParent::capture);
    request.extensions_mut().insert(parent);
    next.run(request).instrument(span).await
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: ID-005, END-007, END-008
    #[test]
    fn only_a_single_exact_traceparent_is_observed_without_baggage() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
                .parse()
                .unwrap(),
        );
        headers.insert("baggage", "secret-canary".parse().unwrap());
        headers.insert("tracestate", "secret-canary".parse().unwrap());
        assert!(parent(&headers).is_some());
        headers.append(
            "traceparent",
            "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-00"
                .parse()
                .unwrap(),
        );
        assert!(parent(&headers).is_none());
        headers.remove("traceparent");
        headers.insert("traceparent", "secret-canary".parse().unwrap());
        assert!(parent(&headers).is_none());
    }
}

//! Split existing cumulative OTLP snapshots without changing their value model.
use crate::telemetry_http::{ExportFailure, MAX_REQUEST_BYTES};
use opentelemetry_http::Bytes;
use serde_json::Value;
const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCHES: usize = 64;
const BATCH_BYTES: usize = 900 * 1024;
const BATCH_POINTS: usize = 512;
pub(crate) struct MetricBatch {
    pub(crate) body: Bytes,
    pub(crate) points: usize,
}
struct Building {
    metrics: Vec<Value>,
    points: usize,
    bytes: usize,
}
fn points_mut<'a>(metric: &'a mut Value, kind: &str) -> Result<&'a mut Vec<Value>, ExportFailure> {
    metric
        .get_mut(kind)
        .and_then(|value| value.get_mut("dataPoints"))
        .and_then(Value::as_array_mut)
        .ok_or(ExportFailure::Encoding)
}
fn finish(
    base: &Value,
    current: &mut Building,
    batches: &mut Vec<MetricBatch>,
) -> Result<(), ExportFailure> {
    if current.metrics.is_empty() {
        return Ok(());
    }
    if batches.len() >= MAX_BATCHES {
        return Err(ExportFailure::RequestLimit);
    }
    let mut body = base.clone();
    body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"] =
        Value::Array(std::mem::take(&mut current.metrics));
    let body = serde_json::to_vec(&body).map_err(|_| ExportFailure::Encoding)?;
    if body.len() > MAX_REQUEST_BYTES {
        return Err(ExportFailure::RequestLimit);
    }
    batches.push(MetricBatch {
        body: Bytes::from(body),
        points: current.points,
    });
    current.points = 0;
    current.bytes = serde_json::to_vec(base)
        .map_err(|_| ExportFailure::Encoding)?
        .len();
    Ok(())
}
pub(crate) fn split_snapshot(snapshot: &str) -> Result<Vec<MetricBatch>, ExportFailure> {
    if snapshot.len() > MAX_SNAPSHOT_BYTES {
        return Err(ExportFailure::RequestLimit);
    }
    let mut base: Value = serde_json::from_str(snapshot).map_err(|_| ExportFailure::Encoding)?;
    // The existing frozen encoder emits exactly this bounded envelope.
    if base["resourceMetrics"].as_array().map(Vec::len) != Some(1)
        || base["resourceMetrics"][0]["scopeMetrics"]
            .as_array()
            .map(Vec::len)
            != Some(1)
    {
        return Err(ExportFailure::Encoding);
    }
    let families = base["resourceMetrics"][0]["scopeMetrics"][0]
        .get_mut("metrics")
        .and_then(Value::as_array_mut)
        .ok_or(ExportFailure::Encoding)?;
    let families = std::mem::take(families);
    let base_bytes = serde_json::to_vec(&base)
        .map_err(|_| ExportFailure::Encoding)?
        .len();
    if base_bytes > BATCH_BYTES {
        return Err(ExportFailure::RequestLimit);
    }
    let mut current = Building {
        metrics: Vec::new(),
        points: 0,
        bytes: base_bytes,
    };
    let mut batches = Vec::new();
    for mut family in families {
        let mut kinds = ["sum", "gauge", "histogram"]
            .into_iter()
            .filter(|kind| family.get(*kind).is_some());
        let kind = kinds.next().ok_or(ExportFailure::Encoding)?;
        if kinds.next().is_some() {
            return Err(ExportFailure::Encoding);
        }
        let values = std::mem::take(points_mut(&mut family, kind)?);
        let family_bytes = serde_json::to_vec(&family)
            .map_err(|_| ExportFailure::Encoding)?
            .len();
        if values.is_empty() {
            if current.bytes.saturating_add(family_bytes + 1) > BATCH_BYTES {
                finish(&base, &mut current, &mut batches)?;
            }
            if current.bytes.saturating_add(family_bytes + 1) > BATCH_BYTES {
                return Err(ExportFailure::RequestLimit);
            }
            current.metrics.push(family);
            current.bytes += family_bytes + 1;
            continue;
        }
        let mut admitted = false;
        for value in values {
            let bytes = serde_json::to_vec(&value)
                .map_err(|_| ExportFailure::Encoding)?
                .len()
                + 1;
            let overhead = if admitted { 0 } else { family_bytes + 1 };
            if current.points >= BATCH_POINTS
                || current.bytes.saturating_add(overhead).saturating_add(bytes) > BATCH_BYTES
            {
                finish(&base, &mut current, &mut batches)?;
                admitted = false;
            }
            if !admitted {
                if current
                    .bytes
                    .saturating_add(family_bytes)
                    .saturating_add(bytes)
                    > BATCH_BYTES
                {
                    return Err(ExportFailure::RequestLimit);
                }
                current.metrics.push(family.clone());
                current.bytes += family_bytes + 1;
                admitted = true;
            }
            let last = current.metrics.last_mut().ok_or(ExportFailure::Encoding)?;
            points_mut(last, kind)?.push(value);
            current.points += 1;
            current.bytes += bytes;
        }
    }
    finish(&base, &mut current, &mut batches)?;
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: ID-005, END-007, END-008, PERF-001
    #[test]
    fn cumulative_metric_batches_preserve_every_point_type_bucket_label_and_timestamp() {
        let mut families = Vec::new();
        for (name, kind) in [
            ("counter", "sum"),
            ("gauge", "gauge"),
            ("latency", "histogram"),
        ] {
            let points=(0..1200).map(|i| {
                let mut point=serde_json::json!({"attributes":[{"key":"operation","value":{"intValue":i.to_string()}}],"timeUnixNano":"1700000000000000000"});
                if kind=="histogram" {point["count"]=serde_json::json!("10");point["sum"]=serde_json::json!(i);point["explicitBounds"]=serde_json::json!([1,2,4]);point["bucketCounts"]=serde_json::json!(["1","2","3","4"]);}else{point["asInt"]=serde_json::json!(i.to_string());}
                point
            }).collect::<Vec<_>>();
            let mut family = serde_json::json!({"name":name});
            family[kind] = serde_json::json!({"dataPoints":points});
            if kind == "sum" {
                family[kind]["aggregationTemporality"] = serde_json::json!(2);
                family[kind]["isMonotonic"] = serde_json::json!(true);
            }
            families.push(family);
        }
        families.push(serde_json::json!({"name":"empty","sum":{"isMonotonic":true,"aggregationTemporality":2,"dataPoints":[]}}));
        let source = serde_json::json!({"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"riffdbd"}}]},"scopeMetrics":[{"scope":{"name":"riffdb-observability","version":"v1"},"metrics":families}]}]});
        let batches = split_snapshot(&source.to_string()).unwrap();
        assert!(batches.len() > 1);
        let mut restored: std::collections::BTreeMap<String, Value> = Default::default();
        for batch in batches {
            assert!(batch.body.len() <= MAX_REQUEST_BYTES);
            assert!(batch.points <= BATCH_POINTS);
            let decoded: Value = serde_json::from_slice(&batch.body).unwrap();
            assert_eq!(
                decoded["resourceMetrics"][0]["resource"],
                source["resourceMetrics"][0]["resource"]
            );
            assert_eq!(
                decoded["resourceMetrics"][0]["scopeMetrics"][0]["scope"],
                source["resourceMetrics"][0]["scopeMetrics"][0]["scope"]
            );
            let mut count = 0;
            for family in decoded["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
                .as_array()
                .unwrap()
            {
                let name = family["name"].as_str().unwrap().to_owned();
                let kind = if name == "counter" || name == "empty" {
                    "sum"
                } else if name == "gauge" {
                    "gauge"
                } else {
                    "histogram"
                };
                let values = family[kind]["dataPoints"].as_array().unwrap();
                count += values.len();
                if let Some(previous) = restored.get_mut(&name) {
                    points_mut(previous, kind).unwrap().extend(values.clone());
                } else {
                    restored.insert(name, family.clone());
                }
            }
            assert_eq!(count, batch.points);
        }
        for family in source["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
        {
            assert_eq!(restored[family["name"].as_str().unwrap()], *family);
        }
    }
}

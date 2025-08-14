//! Metric exporters for different formats
//!
//! Provides exporters for various metric formats including Prometheus, JSON, and CSV.

use super::{MetricFamily, MetricValue, MetricType};
use std::collections::BTreeMap;
use std::fmt::Write;
use crate::{Error, Result};
use serde_json;

/// Prometheus text format exporter
pub struct PrometheusExporter;

impl PrometheusExporter {
    /// Export metrics families to Prometheus text format
    pub fn export(families: &[MetricFamily]) -> String {
        let mut output = String::new();
        
        for family in families {
            Self::export_family(&mut output, family);
        }
        
        output
    }
    
    fn export_family(output: &mut String, family: &MetricFamily) {
        // Write help line
        writeln!(output, "# HELP {} {}", family.name, family.help).unwrap();
        
        // Write type line
        let type_str = match family.metric_type {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge", 
            MetricType::Histogram => "histogram",
            MetricType::Summary => "summary",
        };
        writeln!(output, "# TYPE {} {}", family.name, type_str).unwrap();
        
        // Write metrics
        for metric in &family.metrics {
            Self::export_metric(output, &family.name, metric);
        }
        
        writeln!(output).unwrap();
    }
    
    fn export_metric(output: &mut String, family_name: &str, metric: &crate::metrics::Metric) {
        let labels = Self::format_labels(&metric.metadata.labels);
        
        match &metric.value {
            MetricValue::Counter(value) => {
                writeln!(output, "{}{} {}", family_name, labels, value).unwrap();
            }
            
            MetricValue::Gauge(value) => {
                writeln!(output, "{}{} {}", family_name, labels, value).unwrap();
            }
            
            MetricValue::Histogram { count, sum, buckets } => {
                // Write buckets
                for bucket in buckets {
                    let bucket_labels = if bucket.upper_bound == f64::INFINITY {
                        format!("{}le=\"+Inf\"", if labels.is_empty() { "{".to_string() } else { format!("{},", &labels[1..labels.len()-1]) })
                    } else {
                        format!("{}le=\"{}\"", if labels.is_empty() { "{".to_string() } else { format!("{},", &labels[1..labels.len()-1]) }, bucket.upper_bound)
                    };
                    writeln!(output, "{}_bucket{} {}", family_name, bucket_labels, bucket.count).unwrap();
                }
                
                // Write sum and count
                writeln!(output, "{}_sum{} {}", family_name, labels, sum).unwrap();
                writeln!(output, "{}_count{} {}", family_name, labels, count).unwrap();
            }
            
            MetricValue::Summary { count, sum, quantiles } => {
                // Write quantiles
                for quantile in quantiles {
                    let quantile_labels = format!("{}quantile=\"{}\"", 
                        if labels.is_empty() { "{".to_string() } else { format!("{},", &labels[1..labels.len()-1]) }, 
                        quantile.quantile
                    );
                    writeln!(output, "{}{} {}", family_name, quantile_labels, quantile.value).unwrap();
                }
                
                // Write sum and count
                writeln!(output, "{}_sum{} {}", family_name, labels, sum).unwrap();
                writeln!(output, "{}_count{} {}", family_name, labels, count).unwrap();
            }
        }
    }
    
    fn format_labels(labels: &BTreeMap<String, String>) -> String {
        if labels.is_empty() {
            return String::new();
        }
        
        let mut result = String::from("{");
        let mut first = true;
        
        for (key, value) in labels {
            if !first {
                result.push(',');
            }
            write!(result, "{}=\"{}\"", key, Self::escape_label_value(value)).unwrap();
            first = false;
        }
        
        result.push('}');
        result
    }
    
    fn escape_label_value(value: &str) -> String {
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    }
}

/// JSON format exporter
pub struct JsonExporter;

impl JsonExporter {
    /// Export metric families to JSON format
    pub fn export(families: &[MetricFamily]) -> Result<String> {
        serde_json::to_string_pretty(families)
            .map_err(|e| Error::Serialization(format!("JSON serialization failed: {}", e)))
    }
    
    /// Export single metric family to JSON
    pub fn export_family(family: &MetricFamily) -> Result<String> {
        serde_json::to_string_pretty(family)
            .map_err(|e| Error::Serialization(format!("JSON serialization failed: {}", e)))
    }
}

/// CSV format exporter (for time series data)
pub struct CsvExporter;

impl CsvExporter {
    /// Export metrics to CSV format
    pub fn export(families: &[MetricFamily]) -> String {
        let mut output = String::new();
        
        // CSV Header
        writeln!(output, "timestamp,metric_name,metric_type,labels,value,unit").unwrap();
        
        for family in families {
            for metric in &family.metrics {
                Self::export_metric_csv(&mut output, family, metric);
            }
        }
        
        output
    }
    
    fn export_metric_csv(output: &mut String, family: &MetricFamily, metric: &crate::metrics::Metric) {
        let timestamp = metric.metadata.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        
        let labels = Self::format_labels_csv(&metric.metadata.labels);
        let unit = metric.metadata.unit.as_deref().unwrap_or("");
        
        match &metric.value {
            MetricValue::Counter(value) => {
                writeln!(output, "{},{},{},\"{}\",{},\"{}\"", 
                    timestamp, family.name, "counter", labels, value, unit).unwrap();
            }
            
            MetricValue::Gauge(value) => {
                writeln!(output, "{},{},{},\"{}\",{},\"{}\"", 
                    timestamp, family.name, "gauge", labels, value, unit).unwrap();
            }
            
            MetricValue::Histogram { count, sum, buckets: _ } => {
                // For CSV, we'll export the summary statistics
                writeln!(output, "{},{}_count,{},\"{}\",{},\"{}\"", 
                    timestamp, family.name, "histogram", labels, count, unit).unwrap();
                writeln!(output, "{},{}_sum,{},\"{}\",{},\"{}\"", 
                    timestamp, family.name, "histogram", labels, sum, unit).unwrap();
            }
            
            MetricValue::Summary { count, sum, quantiles: _ } => {
                // For CSV, we'll export the summary statistics
                writeln!(output, "{},{}_count,{},\"{}\",{},\"{}\"", 
                    timestamp, family.name, "summary", labels, count, unit).unwrap();
                writeln!(output, "{},{}_sum,{},\"{}\",{},\"{}\"", 
                    timestamp, family.name, "summary", labels, sum, unit).unwrap();
            }
        }
    }
    
    fn format_labels_csv(labels: &BTreeMap<String, String>) -> String {
        if labels.is_empty() {
            return String::new();
        }
        
        labels.iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// OpenMetrics format exporter
pub struct OpenMetricsExporter;

impl OpenMetricsExporter {
    /// Export metrics to OpenMetrics format
    pub fn export(families: &[MetricFamily]) -> String {
        let mut output = String::new();
        
        for family in families {
            Self::export_family(&mut output, family);
        }
        
        // EOF marker
        writeln!(output, "# EOF").unwrap();
        
        output
    }
    
    fn export_family(output: &mut String, family: &MetricFamily) {
        // OpenMetrics uses same format as Prometheus with some extensions
        PrometheusExporter::export_family(output, family);
    }
}

/// InfluxDB line protocol exporter
pub struct InfluxExporter;

impl InfluxExporter {
    /// Export metrics to InfluxDB line protocol format
    pub fn export(families: &[MetricFamily]) -> String {
        let mut output = String::new();
        
        for family in families {
            for metric in &family.metrics {
                Self::export_metric(&mut output, family, metric);
            }
        }
        
        output
    }
    
    fn export_metric(output: &mut String, family: &MetricFamily, metric: &crate::metrics::Metric) {
        let timestamp = metric.metadata.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        
        match &metric.value {
            MetricValue::Counter(value) => {
                writeln!(output, "{}{} value={}i {}", 
                    family.name, 
                    Self::format_tags(&metric.metadata.labels), 
                    value, 
                    timestamp
                ).unwrap();
            }
            
            MetricValue::Gauge(value) => {
                writeln!(output, "{}{} value={}i {}", 
                    family.name,
                    Self::format_tags(&metric.metadata.labels), 
                    value, 
                    timestamp
                ).unwrap();
            }
            
            MetricValue::Histogram { count, sum, buckets: _ } => {
                writeln!(output, "{}_count{} value={}i {}", 
                    family.name, 
                    Self::format_tags(&metric.metadata.labels), 
                    count, 
                    timestamp
                ).unwrap();
                writeln!(output, "{}_sum{} value={} {}", 
                    family.name, 
                    Self::format_tags(&metric.metadata.labels), 
                    sum, 
                    timestamp
                ).unwrap();
            }
            
            MetricValue::Summary { count, sum, quantiles: _ } => {
                writeln!(output, "{}_count{} value={}i {}", 
                    family.name, 
                    Self::format_tags(&metric.metadata.labels), 
                    count, 
                    timestamp
                ).unwrap();
                writeln!(output, "{}_sum{} value={} {}", 
                    family.name, 
                    Self::format_tags(&metric.metadata.labels), 
                    sum, 
                    timestamp
                ).unwrap();
            }
        }
    }
    
    fn format_tags(labels: &BTreeMap<String, String>) -> String {
        if labels.is_empty() {
            return String::new();
        }
        
        let tags = labels.iter()
            .map(|(k, v)| format!("{}={}", k, Self::escape_tag_value(v)))
            .collect::<Vec<_>>()
            .join(",");
            
        format!(",{}", tags)
    }
    
    fn escape_tag_value(value: &str) -> String {
        // InfluxDB tag values need to be escaped
        if value.contains(' ') || value.contains(',') || value.contains('=') {
            format!("\"{}\"", value.replace('"', "\\\""))
        } else {
            value.to_string()
        }
    }
}

/// Multi-format exporter that can export to different formats
pub struct MultiFormatExporter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Prometheus,
    Json,
    Csv,
    OpenMetrics,
    InfluxDB,
}

impl MultiFormatExporter {
    /// Export metrics in the specified format
    pub fn export(families: &[MetricFamily], format: ExportFormat) -> Result<String> {
        match format {
            ExportFormat::Prometheus => Ok(PrometheusExporter::export(families)),
            ExportFormat::Json => JsonExporter::export(families),
            ExportFormat::Csv => Ok(CsvExporter::export(families)),
            ExportFormat::OpenMetrics => Ok(OpenMetricsExporter::export(families)),
            ExportFormat::InfluxDB => Ok(InfluxExporter::export(families)),
        }
    }
    
    /// Get content type for the format
    pub fn content_type(format: ExportFormat) -> &'static str {
        match format {
            ExportFormat::Prometheus => "text/plain; version=0.0.4; charset=utf-8",
            ExportFormat::Json => "application/json; charset=utf-8",
            ExportFormat::Csv => "text/csv; charset=utf-8",
            ExportFormat::OpenMetrics => "application/openmetrics-text; version=1.0.0; charset=utf-8",
            ExportFormat::InfluxDB => "text/plain; charset=utf-8",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{Counter, Gauge, Histogram, MetricRegistry};
    
    #[test]
    fn test_prometheus_exporter() {
        let registry = MetricRegistry::new();
        
        let counter = Counter::new(
            "test_counter_total".to_string(),
            "A test counter".to_string(),
        );
        counter.add(42);
        registry.register(counter).unwrap();
        
        let families = registry.gather();
        let output = PrometheusExporter::export(&families);
        
        assert!(output.contains("# HELP test_counter_total A test counter"));
        assert!(output.contains("# TYPE test_counter_total counter"));
        assert!(output.contains("test_counter_total 42"));
    }
    
    #[test]
    fn test_json_exporter() {
        let registry = MetricRegistry::new();
        
        let gauge = Gauge::new(
            "test_gauge".to_string(),
            "A test gauge".to_string(),
        );
        gauge.set(123);
        registry.register(gauge).unwrap();
        
        let families = registry.gather();
        let output = JsonExporter::export(&families).unwrap();
        
        assert!(output.contains("test_gauge"));
        assert!(output.contains("123"));
    }
    
    #[test] 
    fn test_csv_exporter() {
        let registry = MetricRegistry::new();
        
        let counter = Counter::new(
            "csv_test_counter".to_string(),
            "A CSV test counter".to_string(),
        );
        counter.inc();
        registry.register(counter).unwrap();
        
        let families = registry.gather();
        let output = CsvExporter::export(&families);
        
        assert!(output.contains("timestamp,metric_name,metric_type,labels,value,unit"));
        assert!(output.contains("csv_test_counter"));
    }
}
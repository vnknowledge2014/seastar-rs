//! Test reporting and result analysis

use crate::orchestration::TestExecutionResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum ReportingError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Template error: {0}")]
    Template(String),
    #[error("Export error: {0}")]
    Export(String),
}

pub type ReportingResult<T> = Result<T, ReportingError>;

/// Test execution results summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResults {
    pub total_tests: usize,
    pub successful_tests: usize,
    pub failed_tests: usize,
    pub skipped_tests: usize,
    pub total_duration_ms: u64,
    pub test_results: Vec<TestExecutionResult>,
    pub summary: String,
}

impl TestResults {
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_tests == 0 {
            0.0
        } else {
            self.successful_tests as f64 / self.total_tests as f64
        }
    }

    /// Get failed test results
    pub fn failed_tests(&self) -> Vec<&TestExecutionResult> {
        self.test_results.iter().filter(|r| !r.success).collect()
    }

    /// Get tests by tag
    pub fn tests_by_tag(&self, _tag: &str) -> Vec<&TestExecutionResult> {
        // TODO: Add tag support to TestExecutionResult
        vec![]
    }

    /// Get performance statistics
    pub fn performance_stats(&self) -> PerformanceStats {
        let durations: Vec<u64> = self.test_results.iter().map(|r| r.duration_ms).collect();
        
        if durations.is_empty() {
            return PerformanceStats::default();
        }

        let total_duration: u64 = durations.iter().sum();
        let avg_duration = total_duration as f64 / durations.len() as f64;
        
        let mut sorted_durations = durations.clone();
        sorted_durations.sort_unstable();
        
        let median = if sorted_durations.len() % 2 == 0 {
            let mid = sorted_durations.len() / 2;
            (sorted_durations[mid - 1] + sorted_durations[mid]) as f64 / 2.0
        } else {
            sorted_durations[sorted_durations.len() / 2] as f64
        };

        PerformanceStats {
            total_duration_ms: self.total_duration_ms,
            avg_test_duration_ms: avg_duration,
            median_test_duration_ms: median,
            min_test_duration_ms: sorted_durations[0],
            max_test_duration_ms: sorted_durations[sorted_durations.len() - 1],
            tests_per_second: self.total_tests as f64 / (self.total_duration_ms as f64 / 1000.0),
        }
    }

    /// Group results by test type
    pub fn group_by_type(&self) -> HashMap<String, Vec<&TestExecutionResult>> {
        let mut groups: HashMap<String, Vec<&TestExecutionResult>> = HashMap::new();
        
        for result in &self.test_results {
            // Extract test type from test name or ID (simplified approach)
            let test_type = if result.test_name.contains("integration") {
                "Integration"
            } else if result.test_name.contains("performance") {
                "Performance"
            } else if result.test_name.contains("property") {
                "Property"
            } else if result.test_name.contains("chaos") {
                "Chaos"
            } else {
                "Unit"
            };
            
            groups.entry(test_type.to_string()).or_insert_with(Vec::new).push(result);
        }
        
        groups
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub total_duration_ms: u64,
    pub avg_test_duration_ms: f64,
    pub median_test_duration_ms: f64,
    pub min_test_duration_ms: u64,
    pub max_test_duration_ms: u64,
    pub tests_per_second: f64,
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            total_duration_ms: 0,
            avg_test_duration_ms: 0.0,
            median_test_duration_ms: 0.0,
            min_test_duration_ms: 0,
            max_test_duration_ms: 0,
            tests_per_second: 0.0,
        }
    }
}

/// Trait for test reporters
#[async_trait]
pub trait TestReporter: Send + Sync {
    /// Generate a test report
    async fn generate_report(&self, results: &TestResults) -> ReportingResult<()>;
    
    /// Get supported output formats
    fn supported_formats(&self) -> Vec<String>;
}

/// JSON test reporter
pub struct JsonReporter {
    output_path: PathBuf,
    pretty_print: bool,
}

impl JsonReporter {
    pub fn new(output_path: PathBuf) -> Self {
        Self {
            output_path,
            pretty_print: true,
        }
    }

    pub fn with_pretty_print(mut self, pretty: bool) -> Self {
        self.pretty_print = pretty;
        self
    }
}

#[async_trait]
impl TestReporter for JsonReporter {
    async fn generate_report(&self, results: &TestResults) -> ReportingResult<()> {
        info!("Generating JSON report to: {}", self.output_path.display());

        let report = TestReport {
            metadata: ReportMetadata {
                generated_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                generator: "seastar-rs-testing".to_string(),
                version: "0.1.0".to_string(),
            },
            summary: TestSummary {
                total_tests: results.total_tests,
                successful_tests: results.successful_tests,
                failed_tests: results.failed_tests,
                skipped_tests: results.skipped_tests,
                success_rate: results.success_rate(),
                total_duration_ms: results.total_duration_ms,
                performance_stats: results.performance_stats(),
            },
            test_results: results.test_results.clone(),
            analysis: TestAnalysis::from_results(results),
        };

        let json_content = if self.pretty_print {
            serde_json::to_string_pretty(&report)
        } else {
            serde_json::to_string(&report)
        }.map_err(|e| ReportingError::Serialization(e.to_string()))?;

        // Ensure parent directory exists
        if let Some(parent) = self.output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.output_path)
            .await?;

        file.write_all(json_content.as_bytes()).await?;
        file.flush().await?;

        info!("JSON report generated successfully");
        Ok(())
    }

    fn supported_formats(&self) -> Vec<String> {
        vec!["json".to_string()]
    }
}

/// HTML test reporter
pub struct HtmlReporter {
    output_path: PathBuf,
    include_charts: bool,
    theme: HtmlTheme,
}

#[derive(Debug, Clone)]
pub enum HtmlTheme {
    Light,
    Dark,
    Auto,
}

impl HtmlReporter {
    pub fn new(output_path: PathBuf) -> Self {
        Self {
            output_path,
            include_charts: true,
            theme: HtmlTheme::Auto,
        }
    }

    pub fn with_charts(mut self, include: bool) -> Self {
        self.include_charts = include;
        self
    }

    pub fn with_theme(mut self, theme: HtmlTheme) -> Self {
        self.theme = theme;
        self
    }
}

#[async_trait]
impl TestReporter for HtmlReporter {
    async fn generate_report(&self, results: &TestResults) -> ReportingResult<()> {
        info!("Generating HTML report to: {}", self.output_path.display());

        let html_content = self.generate_html_content(results)?;

        // Ensure parent directory exists
        if let Some(parent) = self.output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.output_path)
            .await?;

        file.write_all(html_content.as_bytes()).await?;
        file.flush().await?;

        info!("HTML report generated successfully");
        Ok(())
    }

    fn supported_formats(&self) -> Vec<String> {
        vec!["html".to_string()]
    }
}

impl HtmlReporter {
    fn generate_html_content(&self, results: &TestResults) -> ReportingResult<String> {
        let perf_stats = results.performance_stats();
        let groups = results.group_by_type();

        let mut html = String::new();
        html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
        html.push_str("    <meta charset=\"UTF-8\">\n");
        html.push_str("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.push_str("    <title>Seastar-RS Test Report</title>\n");
        html.push_str("    <style>\n");
        html.push_str(&self.get_css());
        html.push_str("    </style>\n");
        html.push_str("</head>\n<body>\n");

        // Header
        html.push_str("    <header class=\"header\">\n");
        html.push_str("        <h1>Seastar-RS Test Report</h1>\n");
        html.push_str(&format!("        <p>Generated on {}</p>\n", 
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));
        html.push_str("    </header>\n");

        // Summary
        html.push_str("    <section class=\"summary\">\n");
        html.push_str("        <h2>Test Summary</h2>\n");
        html.push_str("        <div class=\"summary-grid\">\n");
        html.push_str(&format!("            <div class=\"metric\"><strong>Total Tests:</strong> {}</div>\n", results.total_tests));
        html.push_str(&format!("            <div class=\"metric success\"><strong>Passed:</strong> {}</div>\n", results.successful_tests));
        html.push_str(&format!("            <div class=\"metric failure\"><strong>Failed:</strong> {}</div>\n", results.failed_tests));
        html.push_str(&format!("            <div class=\"metric\"><strong>Success Rate:</strong> {:.2}%</div>\n", results.success_rate() * 100.0));
        html.push_str(&format!("            <div class=\"metric\"><strong>Total Duration:</strong> {:.2}s</div>\n", results.total_duration_ms as f64 / 1000.0));
        html.push_str(&format!("            <div class=\"metric\"><strong>Avg Duration:</strong> {:.2}ms</div>\n", perf_stats.avg_test_duration_ms));
        html.push_str("        </div>\n");
        html.push_str("    </section>\n");

        // Test Groups
        html.push_str("    <section class=\"test-groups\">\n");
        html.push_str("        <h2>Tests by Type</h2>\n");
        for (test_type, test_results) in groups {
            let success_count = test_results.iter().filter(|r| r.success).count();
            html.push_str(&format!("        <h3>{} Tests ({}/{})</h3>\n", test_type, success_count, test_results.len()));
            html.push_str("        <table class=\"results-table\">\n");
            html.push_str("            <thead>\n");
            html.push_str("                <tr><th>Test ID</th><th>Name</th><th>Status</th><th>Duration</th><th>Retries</th></tr>\n");
            html.push_str("            </thead>\n");
            html.push_str("            <tbody>\n");
            
            for result in test_results {
                let status_class = if result.success { "success" } else { "failure" };
                let status_text = if result.success { "PASS" } else { "FAIL" };
                
                html.push_str(&format!(
                    "                <tr class=\"{}\"><td>{}</td><td>{}</td><td>{}</td><td>{}ms</td><td>{}</td></tr>\n",
                    status_class, result.test_id, result.test_name, status_text, result.duration_ms, result.retry_count
                ));
            }
            
            html.push_str("            </tbody>\n");
            html.push_str("        </table>\n");
        }
        html.push_str("    </section>\n");

        // Failed Tests Details
        let failed_tests = results.failed_tests();
        if !failed_tests.is_empty() {
            html.push_str("    <section class=\"failed-tests\">\n");
            html.push_str("        <h2>Failed Test Details</h2>\n");
            
            for result in failed_tests {
                html.push_str(&format!("        <div class=\"failed-test\">\n"));
                html.push_str(&format!("            <h3>{} - {}</h3>\n", result.test_id, result.test_name));
                if let Some(ref error) = result.error_message {
                    html.push_str(&format!("            <pre class=\"error-message\">{}</pre>\n", error));
                }
                html.push_str("        </div>\n");
            }
            
            html.push_str("    </section>\n");
        }

        // Charts (if enabled)
        if self.include_charts {
            html.push_str("    <section class=\"charts\">\n");
            html.push_str("        <h2>Test Results Visualization</h2>\n");
            html.push_str("        <div class=\"chart-placeholder\">Charts would be rendered here with Chart.js</div>\n");
            html.push_str("    </section>\n");
        }

        html.push_str("</body>\n</html>");

        Ok(html)
    }

    fn get_css(&self) -> String {
        r#"
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .header h1 { margin: 0; }
        .summary { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .metric { padding: 10px; background: #ecf0f1; border-radius: 4px; }
        .metric.success { background: #d5f4e6; }
        .metric.failure { background: #fadbd8; }
        .test-groups { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .results-table { width: 100%; border-collapse: collapse; margin: 15px 0; }
        .results-table th, .results-table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        .results-table th { background-color: #f2f2f2; }
        .results-table tr.success { background-color: #d5f4e6; }
        .results-table tr.failure { background-color: #fadbd8; }
        .failed-tests { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .failed-test { margin: 15px 0; padding: 15px; background: #fadbd8; border-left: 4px solid #e74c3c; border-radius: 4px; }
        .error-message { background: #f8f9fa; padding: 10px; border-radius: 4px; overflow-x: auto; }
        .charts { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .chart-placeholder { padding: 50px; text-align: center; background: #ecf0f1; border-radius: 4px; }
        "#.to_string()
    }
}

/// JUnit XML reporter for CI/CD integration
pub struct JunitReporter {
    output_path: PathBuf,
}

impl JunitReporter {
    pub fn new(output_path: PathBuf) -> Self {
        Self { output_path }
    }
}

#[async_trait]
impl TestReporter for JunitReporter {
    async fn generate_report(&self, results: &TestResults) -> ReportingResult<()> {
        info!("Generating JUnit XML report to: {}", self.output_path.display());

        let mut xml = String::new();
        xml.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
        xml.push('\n');
        
        xml.push_str(&format!(
            r#"<testsuites tests="{}" failures="{}" time="{:.3}">"#,
            results.total_tests,
            results.failed_tests,
            results.total_duration_ms as f64 / 1000.0
        ));
        xml.push('\n');

        // Group by test type
        let groups = results.group_by_type();
        for (test_type, test_results) in groups {
            let suite_failures = test_results.iter().filter(|r| !r.success).count();
            let suite_time: u64 = test_results.iter().map(|r| r.duration_ms).sum();
            
            xml.push_str(&format!(
                r#"  <testsuite name="{}" tests="{}" failures="{}" time="{:.3}">"#,
                test_type,
                test_results.len(),
                suite_failures,
                suite_time as f64 / 1000.0
            ));
            xml.push('\n');

            for result in test_results {
                xml.push_str(&format!(
                    r#"    <testcase classname="{}" name="{}" time="{:.3}">"#,
                    test_type,
                    result.test_name,
                    result.duration_ms as f64 / 1000.0
                ));
                xml.push('\n');

                if !result.success {
                    if let Some(ref error_msg) = result.error_message {
                        xml.push_str(&format!(r#"      <failure message="{}">{}</failure>"#, 
                            error_msg.replace('"', "&quot;").chars().take(100).collect::<String>(),
                            error_msg.replace('<', "&lt;").replace('>', "&gt;").replace('&', "&amp;")
                        ));
                        xml.push('\n');
                    }
                }

                xml.push_str("    </testcase>");
                xml.push('\n');
            }

            xml.push_str("  </testsuite>");
            xml.push('\n');
        }

        xml.push_str("</testsuites>");
        xml.push('\n');

        // Ensure parent directory exists
        if let Some(parent) = self.output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.output_path)
            .await?;

        file.write_all(xml.as_bytes()).await?;
        file.flush().await?;

        info!("JUnit XML report generated successfully");
        Ok(())
    }

    fn supported_formats(&self) -> Vec<String> {
        vec!["junit".to_string(), "xml".to_string()]
    }
}

/// Composite reporter that outputs multiple formats
pub struct CompositeReporter {
    reporters: Vec<Box<dyn TestReporter>>,
}

impl CompositeReporter {
    pub fn new() -> Self {
        Self {
            reporters: Vec::new(),
        }
    }

    pub fn add_reporter(mut self, reporter: Box<dyn TestReporter>) -> Self {
        self.reporters.push(reporter);
        self
    }

    pub fn with_json(self, output_path: PathBuf) -> Self {
        self.add_reporter(Box::new(JsonReporter::new(output_path)))
    }

    pub fn with_html(self, output_path: PathBuf) -> Self {
        self.add_reporter(Box::new(HtmlReporter::new(output_path)))
    }

    pub fn with_junit(self, output_path: PathBuf) -> Self {
        self.add_reporter(Box::new(JunitReporter::new(output_path)))
    }
}

impl Default for CompositeReporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TestReporter for CompositeReporter {
    async fn generate_report(&self, results: &TestResults) -> ReportingResult<()> {
        let mut errors = Vec::new();

        for reporter in &self.reporters {
            if let Err(e) = reporter.generate_report(results).await {
                warn!("Reporter failed: {}", e);
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            return Err(ReportingError::Export(
                format!("Some reporters failed: {:?}", errors)
            ));
        }

        Ok(())
    }

    fn supported_formats(&self) -> Vec<String> {
        let mut formats = Vec::new();
        for reporter in &self.reporters {
            formats.extend(reporter.supported_formats());
        }
        formats.sort();
        formats.dedup();
        formats
    }
}

/// Structured test report format
#[derive(Debug, Serialize, Deserialize)]
struct TestReport {
    metadata: ReportMetadata,
    summary: TestSummary,
    test_results: Vec<TestExecutionResult>,
    analysis: TestAnalysis,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReportMetadata {
    generated_at: u64,
    generator: String,
    version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TestSummary {
    total_tests: usize,
    successful_tests: usize,
    failed_tests: usize,
    skipped_tests: usize,
    success_rate: f64,
    total_duration_ms: u64,
    performance_stats: PerformanceStats,
}

#[derive(Debug, Serialize, Deserialize)]
struct TestAnalysis {
    slowest_tests: Vec<String>,
    most_retried_tests: Vec<String>,
    common_failure_patterns: Vec<String>,
    recommendations: Vec<String>,
}

impl TestAnalysis {
    fn from_results(results: &TestResults) -> Self {
        let mut analysis = Self {
            slowest_tests: Vec::new(),
            most_retried_tests: Vec::new(),
            common_failure_patterns: Vec::new(),
            recommendations: Vec::new(),
        };

        // Find slowest tests
        let mut test_durations: Vec<_> = results.test_results.iter()
            .map(|r| (r.test_name.clone(), r.duration_ms))
            .collect();
        test_durations.sort_by(|a, b| b.1.cmp(&a.1));
        analysis.slowest_tests = test_durations.into_iter()
            .take(5)
            .map(|(name, duration)| format!("{} ({}ms)", name, duration))
            .collect();

        // Find most retried tests
        let mut test_retries: Vec<_> = results.test_results.iter()
            .filter(|r| r.retry_count > 0)
            .map(|r| (r.test_name.clone(), r.retry_count))
            .collect();
        test_retries.sort_by(|a, b| b.1.cmp(&a.1));
        analysis.most_retried_tests = test_retries.into_iter()
            .take(5)
            .map(|(name, retries)| format!("{} ({} retries)", name, retries))
            .collect();

        // Analyze failure patterns
        let failed_tests = results.failed_tests();
        let mut error_patterns: HashMap<String, usize> = HashMap::new();
        for result in failed_tests {
            if let Some(ref error) = result.error_message {
                // Simplified pattern extraction
                if error.contains("timeout") {
                    *error_patterns.entry("Timeout".to_string()).or_insert(0) += 1;
                } else if error.contains("connection") {
                    *error_patterns.entry("Connection".to_string()).or_insert(0) += 1;
                } else if error.contains("assertion") {
                    *error_patterns.entry("Assertion".to_string()).or_insert(0) += 1;
                }
            }
        }
        analysis.common_failure_patterns = error_patterns.into_iter()
            .map(|(pattern, count)| format!("{}: {} occurrences", pattern, count))
            .collect();

        // Generate recommendations
        if results.success_rate() < 0.9 {
            analysis.recommendations.push("Consider reviewing failed tests for common issues".to_string());
        }
        if results.performance_stats().avg_test_duration_ms > 5000.0 {
            analysis.recommendations.push("Some tests are running slowly - consider optimization".to_string());
        }
        if results.test_results.iter().any(|r| r.retry_count > 2) {
            analysis.recommendations.push("High retry counts detected - investigate flaky tests".to_string());
        }

        analysis
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn sample_test_results() -> TestResults {
        TestResults {
            total_tests: 5,
            successful_tests: 3,
            failed_tests: 2,
            skipped_tests: 0,
            total_duration_ms: 5000,
            test_results: vec![
                TestExecutionResult {
                    test_id: "test1".to_string(),
                    test_name: "Unit Test 1".to_string(),
                    success: true,
                    duration_ms: 100,
                    retry_count: 0,
                    error_message: None,
                    metrics: HashMap::new(),
                    chaos_faults_injected: 0,
                },
                TestExecutionResult {
                    test_id: "test2".to_string(),
                    test_name: "Integration Test 2".to_string(),
                    success: false,
                    duration_ms: 2000,
                    retry_count: 1,
                    error_message: Some("Connection timeout".to_string()),
                    metrics: HashMap::new(),
                    chaos_faults_injected: 0,
                },
            ],
            summary: "Test run completed".to_string(),
        }
    }

    #[tokio::test]
    async fn test_json_reporter() {
        let temp_file = NamedTempFile::new().unwrap();
        let reporter = JsonReporter::new(temp_file.path().to_path_buf());
        
        let results = sample_test_results();
        let result = reporter.generate_report(&results).await;
        
        assert!(result.is_ok());
        assert!(temp_file.path().exists());
    }

    #[tokio::test]
    async fn test_html_reporter() {
        let temp_file = NamedTempFile::new().unwrap();
        let reporter = HtmlReporter::new(temp_file.path().to_path_buf());
        
        let results = sample_test_results();
        let result = reporter.generate_report(&results).await;
        
        assert!(result.is_ok());
        assert!(temp_file.path().exists());
    }

    #[tokio::test]
    async fn test_junit_reporter() {
        let temp_file = NamedTempFile::new().unwrap();
        let reporter = JunitReporter::new(temp_file.path().to_path_buf());
        
        let results = sample_test_results();
        let result = reporter.generate_report(&results).await;
        
        assert!(result.is_ok());
        assert!(temp_file.path().exists());
    }

    #[tokio::test]
    async fn test_composite_reporter() {
        let json_file = NamedTempFile::new().unwrap();
        let html_file = NamedTempFile::new().unwrap();
        
        let reporter = CompositeReporter::new()
            .with_json(json_file.path().to_path_buf())
            .with_html(html_file.path().to_path_buf());
        
        let results = sample_test_results();
        let result = reporter.generate_report(&results).await;
        
        assert!(result.is_ok());
        assert!(json_file.path().exists());
        assert!(html_file.path().exists());
    }

    #[test]
    fn test_performance_stats() {
        let results = sample_test_results();
        let stats = results.performance_stats();
        
        assert_eq!(stats.total_duration_ms, 5000);
        assert!(stats.avg_test_duration_ms > 0.0);
        assert!(stats.tests_per_second > 0.0);
    }

    #[test]
    fn test_test_analysis() {
        let results = sample_test_results();
        let analysis = TestAnalysis::from_results(&results);
        
        assert!(!analysis.slowest_tests.is_empty());
        assert!(!analysis.common_failure_patterns.is_empty());
        assert!(!analysis.recommendations.is_empty());
    }
}
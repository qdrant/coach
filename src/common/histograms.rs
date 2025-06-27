use hdrhistogram::Histogram;

pub fn render_histogram(histogram: &Histogram<u64>) -> String {
    let min = histogram.min();
    let p50 = histogram.value_at_quantile(0.5);
    let p95 = histogram.value_at_quantile(0.95);
    let p99 = histogram.value_at_quantile(0.99);
    let max = histogram.max();
    format!("[min: {min}ms, p50: {p50}ms, p95: {p95}ms, p99: {p99}ms, max: {max}ms]")
}

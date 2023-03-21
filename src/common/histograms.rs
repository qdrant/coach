use hdrhistogram::Histogram;

pub fn render_histogram(histogram: &Histogram<u64>) -> String {
    let min = histogram.min();
    let p50 = histogram.value_at_quantile(0.5);
    let p95 = histogram.value_at_quantile(0.95);
    let p99 = histogram.value_at_quantile(0.99);
    let max = histogram.max();
    format!(
        "[min: {}ms, p50: {}ms, p95: {}ms, p99: {}ms, max: {}ms]",
        min, p50, p95, p99, max
    )
}

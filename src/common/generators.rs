use core::option::Option;
use core::option::Option::{None, Some};
use qdrant_client::Payload;
use qdrant_client::qdrant::r#match::MatchValue;
use qdrant_client::qdrant::{Condition, Filter};
use rand::RngExt;
use rand::rngs::SmallRng;
use std::collections::HashMap;

pub const KEYWORD_PAYLOAD_KEY: &str = "a";

pub fn random_keyword(rng: &mut SmallRng, num_variants: usize) -> String {
    let variant = rng.random_range(0..num_variants);
    format!("keyword_{variant}")
}

pub fn random_payload(rng: &mut SmallRng, keyword_variants: Option<usize>) -> Payload {
    let mut payload = Payload::new();
    if let Some(n) = keyword_variants
        && n > 0
    {
        payload.insert(KEYWORD_PAYLOAD_KEY, random_keyword(rng, n));
    }
    payload
}

pub fn random_filter(rng: &mut SmallRng, keyword_variants: Option<usize>) -> Option<Filter> {
    let mut filter = Filter {
        should: vec![],
        must: vec![],
        must_not: vec![],
        min_should: None,
    };
    let mut have_any = false;
    if let Some(n) = keyword_variants {
        have_any = true;
        filter.must.push(Condition::matches(
            KEYWORD_PAYLOAD_KEY.to_string(),
            MatchValue::Keyword(random_keyword(rng, n)),
        ))
    }
    if have_any { Some(filter) } else { None }
}

pub fn random_vector(rng: &mut SmallRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect()
}

pub fn random_named_vector(
    rng: &mut SmallRng,
    named: String,
    dim: usize,
) -> HashMap<String, Vec<f32>> {
    if dim == 0 {
        return HashMap::new();
    }
    let vec = random_vector(rng, dim);
    let mut map = HashMap::with_capacity(1);
    map.insert(named, vec);
    map
}

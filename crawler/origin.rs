use chrono::Utc;
use std::cmp::max;
use std::cmp::min;

const ORIGIN_REQ_COUNT_WINDOW_MS: i64 = 1000;
const ORIGIN_REQ_COUNT_WINDOW_MAX: usize = 24;
const MAX_FAILURE_COUNT: i8 = 8;

#[derive(Default)]
pub(crate) struct Origin {
  rate_limited_until_ms: i64,
  failures: i8,
  request_count_window_ms: i64,
  request_count: usize,
}

impl Origin {
  pub fn can_request(&mut self) -> bool {
    let now = Utc::now().timestamp_millis();
    if self.rate_limited_until_ms > now {
      return false;
    };

    let window = now / ORIGIN_REQ_COUNT_WINDOW_MS;
    if window != self.request_count_window_ms {
      self.request_count_window_ms = window;
      self.request_count = 0;
    };
    if self.request_count > ORIGIN_REQ_COUNT_WINDOW_MAX {
      self.rate_limited_until_ms = now + ORIGIN_REQ_COUNT_WINDOW_MS;
      return false;
    };
    self.request_count += 1;
    true
  }

  pub fn incr_failures(&mut self) {
    self.failures = min(MAX_FAILURE_COUNT, self.failures + 1);
    self.rate_limited_until_ms = Utc::now().timestamp_millis() + ((1 << self.failures) * 1000);
  }

  pub fn decr_failures(&mut self) {
    self.failures = max(0, self.failures - 1);
  }
}

#[cfg(test)]
mod tests {
  use super::Origin;
  use crate::origin::ORIGIN_REQ_COUNT_WINDOW_MAX;

  #[test]
  fn test_origin_rate_limiting() {
    let mut origin = Origin::default();
    // We get 200 crawl tasks, all with URLs pointing to the same origin.
    // We send the requests before any of them receive their response.
    for i in 0..200 {
      let can_req = origin.can_request();
      if i <= ORIGIN_REQ_COUNT_WINDOW_MAX {
        assert!(can_req);
      } else {
        assert!(!can_req);
      };
    }
  }
}

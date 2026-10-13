use hickory_net::proto::op::{DnsRequest, DnsResponse, Message, ResponseCode};
use hickory_net::proto::rr::RecordType;
use moka::{Expiry, sync::Cache};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct Entry {
    message: Message,
    inserted: tokio::time::Instant,
    ttl: Duration,
}

struct EntryExpiry;

impl Expiry<Vec<u8>, Entry> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &Vec<u8>,
        value: &Entry,
        _created: Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }

    fn expire_after_update(
        &self,
        _key: &Vec<u8>,
        value: &Entry,
        _updated: Instant,
        _remaining: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

pub(crate) struct ResponseCache(Cache<Vec<u8>, Entry>);

impl ResponseCache {
    pub fn new(capacity: u32) -> Self {
        Self(
            Cache::builder()
                .max_capacity(u64::from(capacity))
                .expire_after(EntryExpiry)
                .build(),
        )
    }

    pub fn key(request: &DnsRequest) -> Option<Vec<u8>> {
        // Client-specific EDNS options and authenticated messages must reach the
        // upstream. All other flags and EDNS settings remain part of the key.
        if request.signature.is_some()
            || !request.additionals.is_empty()
            || request
                .edns
                .as_ref()
                .is_some_and(|edns| !edns.options().as_ref().is_empty())
        {
            return None;
        }

        let (mut message, _) = request.clone().into_parts();
        message.metadata.id = 0;
        message.to_vec().ok()
    }

    pub fn get(&self, key: &Vec<u8>, request: &DnsRequest) -> Option<DnsResponse> {
        let entry = self.0.get(key)?;
        let elapsed = entry.inserted.elapsed();
        if elapsed >= entry.ttl {
            self.0.invalidate(key);
            return None;
        }

        let mut message = entry.message;
        message.metadata.id = request.metadata.id;
        message.queries = request.queries.clone();
        let seconds = elapsed.as_secs().min(u64::from(u32::MAX)) as u32;
        for record in message
            .answers
            .iter_mut()
            .chain(message.authorities.iter_mut())
            .chain(message.additionals.iter_mut())
        {
            record.ttl = record.ttl.saturating_sub(seconds);
        }
        DnsResponse::from_message(message).ok()
    }

    pub fn insert(&self, key: Vec<u8>, response: &DnsResponse) {
        if response.metadata.truncation
            || response.signature.is_some()
            || !matches!(
                response.metadata.response_code,
                ResponseCode::NoError | ResponseCode::NXDomain
            )
            || response
                .edns
                .as_ref()
                .is_some_and(|edns| !edns.options().as_ref().is_empty())
        {
            return;
        }

        let query_type = response.queries.first().map(|query| query.query_type);
        let positive = response.metadata.response_code == ResponseCode::NoError
            && response.answers.iter().any(|record| {
                Some(record.record_type()) == query_type || query_type == Some(RecordType::ANY)
            });
        let records_ttl = response
            .answers
            .iter()
            .chain(response.authorities.iter())
            .chain(response.additionals.iter())
            .map(|record| record.ttl)
            .min();
        let ttl = if positive {
            records_ttl
        } else {
            response
                .negative_ttl()
                .map(|ttl| records_ttl.map_or(ttl, |records| records.min(ttl)))
        };
        let Some(ttl) = ttl.filter(|ttl| *ttl > 0) else {
            return;
        };

        let mut message = response.clone().into_message();
        if !positive {
            for record in &mut message.authorities {
                if record.record_type() == RecordType::SOA {
                    record.ttl = record.ttl.min(ttl);
                }
            }
        }

        self.0.insert(
            key,
            Entry {
                message,
                inserted: tokio::time::Instant::now(),
                ttl: Duration::from_secs(u64::from(ttl)),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_net::proto::{
        op::Query,
        rr::{
            Name, RData, Record,
            rdata::{A, CNAME, SOA},
        },
    };

    #[tokio::test(start_paused = true)]
    async fn cache_ages_all_sections_and_expires_at_positive_and_negative_ttl() {
        let cache = ResponseCache::new(8);
        let mut message = Message::query();
        message.queries.push(Query::query(
            Name::from_ascii("alias.example.").unwrap(),
            RecordType::A,
        ));
        let mut request = DnsRequest::from(message);
        let key = ResponseCache::key(&request).unwrap();

        let mut response = Message::response(1, hickory_net::proto::op::OpCode::Query);
        response.queries = request.queries.clone();
        response.answers.push(Record::from_rdata(
            Name::from_ascii("alias.example.").unwrap(),
            2,
            RData::CNAME(CNAME(Name::from_ascii("target.example.").unwrap())),
        ));
        response.answers.push(Record::from_rdata(
            Name::from_ascii("target.example.").unwrap(),
            60,
            RData::A(A([192, 0, 2, 1].into())),
        ));
        cache.insert(key.clone(), &DnsResponse::from_message(response).unwrap());
        tokio::time::advance(Duration::from_secs(1)).await;
        request.metadata.id = 42;
        let cached = cache.get(&key, &request).unwrap();
        assert_eq!(cached.metadata.id, 42);
        assert_eq!(cached.answers[0].ttl, 1);
        assert_eq!(cached.answers[1].ttl, 59);
        assert_eq!(
            Message::from_vec(cached.as_buffer()).unwrap().metadata.id,
            42
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(cache.get(&key, &request).is_none());

        let mut response = Message::response(1, hickory_net::proto::op::OpCode::Query);
        response.queries = request.queries.clone();
        response.metadata.response_code = ResponseCode::NXDomain;
        response.authorities.push(Record::from_rdata(
            Name::from_ascii("example.").unwrap(),
            60,
            RData::SOA(SOA::new(
                Name::from_ascii("ns.example.").unwrap(),
                Name::from_ascii("hostmaster.example.").unwrap(),
                1,
                60,
                60,
                60,
                2,
            )),
        ));
        cache.insert(key.clone(), &DnsResponse::from_message(response).unwrap());
        tokio::time::advance(Duration::from_secs(1)).await;
        let cached = cache.get(&key, &request).unwrap();
        assert_eq!(cached.metadata.response_code, ResponseCode::NXDomain);
        assert_eq!(cached.authorities[0].ttl, 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(cache.get(&key, &request).is_none());
    }
}

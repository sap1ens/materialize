// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A load generator for the [Nexmark] streaming benchmark.
//!
//! The data generation is a port of Feldera's Rust generator (`dbsp_nexmark`),
//! which itself follows the Java generator from the Nexmark repository. With
//! the same configuration it produces the same events as Feldera's Nexmark
//! input connector: each event is drawn from a `SmallRng` seeded with the
//! event's id, with Nexmark's default generator options, a single event
//! generator, and event times starting at the Unix epoch.
//!
//! Because an event depends only on its id, batch `n` holds events
//! `n * batch_size .. (n + 1) * batch_size` and can be generated without
//! replaying earlier batches.
//!
//! [Nexmark]: https://github.com/nexmark/nexmark

use std::cmp;
use std::iter;

use chrono::DateTime;
use mz_ore::now::NowFn;
use mz_repr::{Datum, Diff, Row};
use mz_storage_types::sources::MzOffset;
use mz_storage_types::sources::load_generator::{
    Event, Generator, LoadGeneratorOutput, NexmarkLoadGenerator, NexmarkView,
};
use rand_8::distributions::{Alphanumeric, DistString};
use rand_8::rngs::SmallRng;
use rand_8::seq::SliceRandom;
use rand_8::{Rng, SeedableRng};

// Nexmark's default generator options. Out of every `TOTAL_PROPORTION`
// consecutive events, the first `PERSON_PROPORTION` are people, the next
// `AUCTION_PROPORTION` are auctions, and the rest are bids.
const PERSON_PROPORTION: u64 = 1;
const AUCTION_PROPORTION: u64 = 3;
const BID_PROPORTION: u64 = 46;
const TOTAL_PROPORTION: u64 = PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;
const AVG_PERSON_BYTE_SIZE: usize = 200;
const AVG_AUCTION_BYTE_SIZE: usize = 500;
const AVG_BID_BYTE_SIZE: usize = 100;
/// Event time between consecutive events, in milliseconds.
const EVENT_INTERVAL_MS: u64 = 10;
// A bid goes to a hot auction, a bid comes from a hot bidder, and an auction
// comes from a hot seller with probability `1 - 1 / ratio`.
const HOT_AUCTION_RATIO: usize = 2;
const HOT_BIDDERS_RATIO: usize = 4;
const HOT_SELLERS_RATIO: usize = 4;
/// The number of most recent people who place bids and open auctions.
const NUM_ACTIVE_PEOPLE: u64 = 1000;
/// The average number of auctions open at any time.
const NUM_IN_FLIGHT_AUCTIONS: u64 = 100;

/// The number of person ids past the last generated person that bids and
/// auctions may reference, so that they appear to arrive before their person.
const PERSON_ID_LEAD: u64 = 10;
// Ids start at these values so that queries find matches even on small
// datasets.
const FIRST_PERSON_ID: u64 = 1000;
const FIRST_AUCTION_ID: u64 = 1000;
const FIRST_CATEGORY_ID: u64 = 10;
const NUM_CATEGORIES: u64 = 5;
// The hot seller, bidder, or auction is the first of the most recent block of
// this many people or auctions.
const HOT_SELLERS_BLOCK: u64 = 100;
const HOT_BIDDERS_BLOCK: u64 = 100;
const HOT_AUCTIONS_BLOCK: u64 = 100;
/// One in this many bids comes from one of `CHANNELS_NUMBER` regular channels,
/// the rest from one of the `HOT_CHANNELS`.
const HOT_CHANNELS_RATIO: usize = 100;
const CHANNELS_NUMBER: u32 = 10_000;
const MIN_STRING_LENGTH: usize = 3;
const BASE_URL_PATH_LENGTH: usize = 5;

const HOT_CHANNELS: [&str; 4] = ["Google", "Facebook", "Baidu", "Apple"];
const HOT_URLS: [&str; 4] = [
    "https://www.nexmark.com/googl/item.htm?query=1",
    "https://www.nexmark.com/meta/item.htm?query=1",
    "https://www.nexmark.com/bidu/item.htm?query=1",
    "https://www.nexmark.com/aapl/item.htm?query=1",
];
const US_STATES: [&str; 6] = ["AZ", "CA", "ID", "OR", "WA", "WY"];
const US_CITIES: [&str; 10] = [
    "Phoenix",
    "Los Angeles",
    "San Francisco",
    "Boise",
    "Portland",
    "Bend",
    "Redmond",
    "Seattle",
    "Kent",
    "Cheyenne",
];
const FIRST_NAMES: [&str; 11] = [
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
];
const LAST_NAMES: [&str; 9] = [
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
];

/// CREATE TABLE person
///   (
///      id            BIGINT PRIMARY KEY,
///      name          TEXT,
///      email_address TEXT,
///      credit_card   TEXT,
///      city          TEXT,
///      state         TEXT,
///      date_time     TIMESTAMP(3),
///      extra         TEXT
///   );
///
/// CREATE TABLE auction
///   (
///      id          BIGINT PRIMARY KEY,
///      item_name   TEXT,
///      description TEXT,
///      initial_bid BIGINT,
///      reserve     BIGINT,
///      date_time   TIMESTAMP(3),
///      expires     TIMESTAMP(3),
///      seller      BIGINT,
///      category    BIGINT,
///      extra       TEXT
///   );
///
/// CREATE TABLE bid
///   (
///      auction   BIGINT,
///      bidder    BIGINT,
///      price     BIGINT,
///      channel   TEXT,
///      url       TEXT,
///      date_time TIMESTAMP(3),
///      extra     TEXT
///   );
pub struct Nexmark {
    pub config: NexmarkLoadGenerator,
    /// The source's `AS OF`. Batches up to it are all emitted at this offset.
    pub as_of: u64,
}

// Note that this generator never issues retractions; if you change this,
// `mz_storage_types::sources::LoadGenerator::is_monotonic`
// must be updated.
impl Generator for Nexmark {
    fn by_seed(
        &self,
        _now: NowFn,
        _seed: Option<u64>,
        resume_offset: MzOffset,
    ) -> Box<dyn Iterator<Item = (LoadGeneratorOutput, Event<Option<MzOffset>, (Row, Diff)>)>> {
        let NexmarkLoadGenerator {
            batch_size,
            max_events,
        } = self.config;
        let max_events = max_events.unwrap_or(u64::MAX);
        // Batches at or below `as_of` are all emitted at offset `as_of`, so
        // none of them is durable until the resume offset passes `as_of`. Past
        // it, every batch below the resume offset is durable.
        let first_batch = if resume_offset.offset > self.as_of {
            resume_offset.offset
        } else {
            0
        };

        Box::new(
            (first_batch..)
                .map_while(move |batch| {
                    let start = batch.checked_mul(batch_size)?;
                    (start < max_events).then(|| {
                        (
                            batch,
                            start,
                            start.saturating_add(batch_size).min(max_events),
                        )
                    })
                })
                .flat_map(|(batch, start, end)| {
                    let offset = MzOffset::from(batch);
                    (start..end)
                        .map(move |event_id| {
                            let (view, row) = generate_event(event_id);
                            (
                                LoadGeneratorOutput::Nexmark(view),
                                Event::Message(offset, (row, Diff::ONE)),
                            )
                        })
                        .chain(iter::once((
                            LoadGeneratorOutput::Default,
                            Event::Progress(Some(offset + 1)),
                        )))
                }),
        )
    }
}

/// Generates the event with the given id as a row of the table it belongs to.
fn generate_event(event_id: u64) -> (NexmarkView, Row) {
    let mut rng = SmallRng::seed_from_u64(event_id);
    let timestamp = event_id * EVENT_INTERVAL_MS;
    let rem = event_id % TOTAL_PROPORTION;
    if rem < PERSON_PROPORTION {
        (
            NexmarkView::Person,
            next_person(&mut rng, event_id, timestamp),
        )
    } else if rem < PERSON_PROPORTION + AUCTION_PROPORTION {
        (
            NexmarkView::Auction,
            next_auction(&mut rng, event_id, timestamp),
        )
    } else {
        (NexmarkView::Bid, next_bid(&mut rng, event_id, timestamp))
    }
}

// NOTE: The draws from `rng` below follow the reference generator call for
// call, including the integer types of `gen_range`, which decide how many
// bits each draw consumes. Reordering them, or changing a range's type,
// changes the data.

fn next_person(rng: &mut SmallRng, event_id: u64, timestamp: u64) -> Row {
    let id = last_base0_person_id(event_id) + FIRST_PERSON_ID;
    let name = format!(
        "{} {}",
        FIRST_NAMES.choose(rng).unwrap(),
        LAST_NAMES.choose(rng).unwrap()
    );
    let email_address = format!("{}@{}.com", next_string(rng, 7), next_string(rng, 5));
    let credit_card = format!(
        "{:04} {:04} {:04} {:04}",
        rng.gen_range(0..10_000),
        rng.gen_range(0..10_000),
        rng.gen_range(0..10_000),
        rng.gen_range(0..10_000)
    );
    let city = US_CITIES.choose(rng).unwrap();
    let state = US_STATES.choose(rng).unwrap();
    // The reference sizes leave out `date_time`.
    let current_size = size_of::<u64>()
        + name.len()
        + email_address.len()
        + credit_card.len()
        + city.len()
        + state.len();
    let extra = next_extra(rng, current_size, AVG_PERSON_BYTE_SIZE);

    let mut row = Row::default();
    row.packer().extend([
        Datum::Int64(to_i64(id)),
        Datum::String(&name),
        Datum::String(&email_address),
        Datum::String(&credit_card),
        Datum::String(city),
        Datum::String(state),
        timestamp_datum(timestamp),
        Datum::String(&extra),
    ]);
    row
}

fn next_auction(rng: &mut SmallRng, event_id: u64, timestamp: u64) -> Row {
    let id = last_base0_auction_id(event_id) + FIRST_AUCTION_ID;
    let seller = match rng.gen_range(0..HOT_SELLERS_RATIO) {
        0 => next_base0_person_id(rng, event_id),
        _ => (last_base0_person_id(event_id) / HOT_SELLERS_BLOCK) * HOT_SELLERS_BLOCK,
    } + FIRST_PERSON_ID;
    let category = FIRST_CATEGORY_ID + rng.gen_range(0..NUM_CATEGORIES);
    let initial_bid = next_price(rng);
    let expires = timestamp + next_auction_length_ms(rng, event_id, timestamp);
    let item_name = next_string(rng, 20);
    let description = next_string(rng, 100);
    let reserve = initial_bid + next_price(rng);
    // The reference sizes leave out `date_time` and count `id`, `initial_bid`,
    // `reserve`, `category`, `seller`, and `expires` as 8 bytes each.
    let current_size =
        size_of::<u64>() + item_name.len() + description.len() + size_of::<u64>() * 5;
    let extra = next_extra(rng, current_size, AVG_AUCTION_BYTE_SIZE);

    let mut row = Row::default();
    row.packer().extend([
        Datum::Int64(to_i64(id)),
        Datum::String(&item_name),
        Datum::String(&description),
        Datum::Int64(to_i64(initial_bid)),
        Datum::Int64(to_i64(reserve)),
        timestamp_datum(timestamp),
        timestamp_datum(expires),
        Datum::Int64(to_i64(seller)),
        Datum::Int64(to_i64(category)),
        Datum::String(&extra),
    ]);
    row
}

fn next_bid(rng: &mut SmallRng, event_id: u64, timestamp: u64) -> Row {
    let auction = match rng.gen_range(0..HOT_AUCTION_RATIO) {
        0 => next_base0_auction_id(rng, event_id),
        _ => (last_base0_auction_id(event_id) / HOT_AUCTIONS_BLOCK) * HOT_AUCTIONS_BLOCK,
    } + FIRST_AUCTION_ID;
    let bidder = match rng.gen_range(0..HOT_BIDDERS_RATIO) {
        0 => next_base0_person_id(rng, event_id),
        // The second person of the block, so that hot bidders and hot sellers
        // differ.
        _ => (last_base0_person_id(event_id) / HOT_BIDDERS_BLOCK) * HOT_BIDDERS_BLOCK + 1,
    } + FIRST_PERSON_ID;
    let price = next_price(rng);
    let channel_number = rng.gen_range(0..CHANNELS_NUMBER);
    let (channel, url) = match rng.gen_range(1..=HOT_CHANNELS_RATIO) {
        HOT_CHANNELS_RATIO => channel_instance(channel_number),
        _ => {
            let hot_index = rng.gen_range(0..HOT_CHANNELS.len());
            (
                HOT_CHANNELS[hot_index].to_string(),
                HOT_URLS[hot_index].to_string(),
            )
        }
    };
    // The reference sizes count only `auction`, `bidder`, `price`, and
    // `date_time`.
    let current_size = size_of::<u64>() * 4;
    let extra = next_extra(rng, current_size, AVG_BID_BYTE_SIZE);

    let mut row = Row::default();
    row.packer().extend([
        Datum::Int64(to_i64(auction)),
        Datum::Int64(to_i64(bidder)),
        Datum::Int64(to_i64(price)),
        Datum::String(&channel),
        Datum::String(&url),
        timestamp_datum(timestamp),
        Datum::String(&extra),
    ]);
    row
}

/// Returns the name and URL of a regular (not hot) channel, which depend only
/// on the channel's number.
fn channel_instance(channel_number: u32) -> (String, String) {
    let mut rng = SmallRng::seed_from_u64(u64::from(channel_number));
    let url = format!(
        "https://www.nexmark.com/{}/item.htm?query=1",
        next_string(&mut rng, BASE_URL_PATH_LENGTH),
    );
    let url = match rng.gen_range(0..10) {
        9 => url,
        _ => format!("{url}&channel_id={}", channel_number.reverse_bits()),
    };
    (format!("channel-{channel_number}"), url)
}

/// Returns the id, without `FIRST_PERSON_ID`, of the last person generated at
/// or before `event_id`.
fn last_base0_person_id(event_id: u64) -> u64 {
    let epoch = event_id / TOTAL_PROPORTION;
    let offset = cmp::min(event_id % TOTAL_PROPORTION, PERSON_PROPORTION - 1);
    epoch * PERSON_PROPORTION + offset
}

/// Returns a random id, without `FIRST_PERSON_ID`, among the
/// `NUM_ACTIVE_PEOPLE` most recent people and the `PERSON_ID_LEAD` people
/// after them.
fn next_base0_person_id(rng: &mut SmallRng, event_id: u64) -> u64 {
    let num_people = last_base0_person_id(event_id) + 1;
    let active_people = cmp::min(num_people, NUM_ACTIVE_PEOPLE);
    let n = rng.gen_range(0..(active_people + PERSON_ID_LEAD));
    num_people - active_people + n
}

/// Returns the id, without `FIRST_AUCTION_ID`, of the last auction generated at
/// or before `event_id`.
fn last_base0_auction_id(event_id: u64) -> u64 {
    let mut epoch = event_id / TOTAL_PROPORTION;
    let mut offset = event_id % TOTAL_PROPORTION;
    if offset < PERSON_PROPORTION {
        // The last auction is in the previous epoch.
        epoch = match epoch.checked_sub(1) {
            Some(epoch) => epoch,
            None => return 0,
        };
        offset = AUCTION_PROPORTION - 1;
    } else if offset >= PERSON_PROPORTION + AUCTION_PROPORTION {
        offset = AUCTION_PROPORTION - 1;
    } else {
        offset -= PERSON_PROPORTION;
    }
    epoch * AUCTION_PROPORTION + offset
}

/// Returns a random id, without `FIRST_AUCTION_ID`, among the auctions likely
/// to still be open.
fn next_base0_auction_id(rng: &mut SmallRng, event_id: u64) -> u64 {
    let max_auction = last_base0_auction_id(event_id);
    let min_auction = max_auction.saturating_sub(NUM_IN_FLIGHT_AUCTIONS);
    min_auction + rng.gen_range(0..(max_auction - min_auction + 1))
}

/// Returns a random auction length in milliseconds, averaging the event time
/// it takes to generate `NUM_IN_FLIGHT_AUCTIONS` more auctions.
fn next_auction_length_ms(rng: &mut SmallRng, event_id: u64, timestamp: u64) -> u64 {
    let num_events_for_auctions = (NUM_IN_FLIGHT_AUCTIONS * TOTAL_PROPORTION) / AUCTION_PROPORTION;
    let future_auction = (event_id + num_events_for_auctions) * EVENT_INTERVAL_MS;
    let horizon = future_auction.saturating_sub(timestamp);
    1 + rng.gen_range(0..cmp::max(horizon * 2, 1))
}

/// Returns a random price in cents, log-uniformly distributed between $1 and
/// $1,000,000.
fn next_price(rng: &mut SmallRng) -> u64 {
    let exponent: f32 = rng.gen_range(0.0..1.0) * 6.0;
    let price = (10.0_f32.powf(exponent) * 100.0).ceil();
    // The float to integer cast saturates, and `price` is at most 10^8.
    #[allow(clippy::as_conversions)]
    let price = price as u64;
    price
}

/// Returns a random alphanumeric string of `MIN_STRING_LENGTH..=max_length`
/// characters.
fn next_string(rng: &mut SmallRng, max_length: usize) -> String {
    let len = rng.gen_range(MIN_STRING_LENGTH..=max_length);
    Alphanumeric.sample_string(rng, len)
}

/// Returns a random string whose length makes the event size average
/// `desired_average_size` bytes, given the `current_size` of its other fields.
fn next_extra(rng: &mut SmallRng, current_size: usize, desired_average_size: usize) -> String {
    if current_size > desired_average_size {
        return String::new();
    }
    let avg_extra_size = desired_average_size - current_size;
    // Rounds 20% of the average to the nearest integer.
    let delta = (avg_extra_size + 2) / 5;
    if delta == 0 {
        return String::new();
    }
    let desired_size = rng.gen_range(avg_extra_size.saturating_sub(delta)..=avg_extra_size + delta);
    Alphanumeric.sample_string(rng, desired_size)
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).expect("Nexmark values fit in i64")
}

fn timestamp_datum(millis: u64) -> Datum<'static> {
    let timestamp = DateTime::from_timestamp_millis(to_i64(millis))
        .expect("Nexmark event times are in range")
        .naive_utc();
    Datum::Timestamp(
        timestamp
            .try_into()
            .expect("Nexmark event times are in range"),
    )
}

#[cfg(test)]
mod tests {
    use mz_ore::now::SYSTEM_TIME;

    use super::*;

    fn events(
        config: NexmarkLoadGenerator,
        as_of: u64,
        resume_offset: u64,
    ) -> Vec<(LoadGeneratorOutput, Event<Option<MzOffset>, (Row, Diff)>)> {
        Nexmark { config, as_of }
            .by_seed(SYSTEM_TIME.clone(), None, MzOffset::from(resume_offset))
            .collect()
    }

    #[mz_ore::test]
    fn proportions_and_ids() {
        let views: Vec<_> = (0..100).map(|id| generate_event(id).0).collect();
        for (epoch, events) in views.chunks(50).enumerate() {
            assert_eq!(events[0], NexmarkView::Person, "epoch {epoch}");
            assert!(events[1..4].iter().all(|v| *v == NexmarkView::Auction));
            assert!(events[4..].iter().all(|v| *v == NexmarkView::Bid));
        }

        let id = |event_id| {
            generate_event(event_id)
                .1
                .iter()
                .next()
                .unwrap()
                .unwrap_int64()
        };
        assert_eq!(id(0), 1000, "first person");
        assert_eq!(id(50), 1001, "second person");
        assert_eq!(id(1), 1000, "first auction");
        assert_eq!(id(3), 1002, "third auction");
        assert_eq!(id(51), 1003, "fourth auction");
    }

    /// Compares a digest of the first 20,000 events against one computed from
    /// Feldera's generator (`dbsp_nexmark`) as its Nexmark input connector
    /// configures it. Each event is hashed as `<table>|<field>|...\n`, with
    /// event times in milliseconds since the epoch.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn matches_reference_generator() {
        let mut digest: u64 = 0xcbf29ce484222325;
        for event_id in 0..20_000 {
            let (view, row) = generate_event(event_id);
            let mut line = match view {
                NexmarkView::Person => "person",
                NexmarkView::Auction => "auction",
                NexmarkView::Bid => "bid",
            }
            .to_string();
            for datum in row.iter() {
                line.push('|');
                match datum {
                    Datum::Int64(i) => line.push_str(&i.to_string()),
                    Datum::String(s) => line.push_str(s),
                    Datum::Timestamp(ts) => {
                        line.push_str(&ts.and_utc().timestamp_millis().to_string())
                    }
                    datum => panic!("unexpected datum {datum:?}"),
                }
            }
            line.push('\n');
            // FNV-1a.
            for byte in line.bytes() {
                digest ^= u64::from(byte);
                digest = digest.wrapping_mul(0x100000001b3);
            }
        }
        assert_eq!(digest, 0x5d633915fcb71089);
    }

    #[mz_ore::test]
    fn deterministic() {
        for event_id in [0, 1, 7, 49, 50, 12_345] {
            assert_eq!(generate_event(event_id), generate_event(event_id));
        }
    }

    #[mz_ore::test]
    fn bids_reference_known_ids() {
        for event_id in 4..5_000 {
            let (view, row) = generate_event(event_id);
            if view != NexmarkView::Bid {
                continue;
            }
            let mut datums = row.iter();
            let auction = u64::try_from(datums.next().unwrap().unwrap_int64()).unwrap();
            let bidder = u64::try_from(datums.next().unwrap().unwrap_int64()).unwrap();
            assert!(auction - FIRST_AUCTION_ID <= last_base0_auction_id(event_id));
            assert!(
                bidder - FIRST_PERSON_ID <= last_base0_person_id(event_id) + PERSON_ID_LEAD,
                "bid {event_id} from person {bidder}"
            );
        }
    }

    #[mz_ore::test]
    fn batches() {
        let config = NexmarkLoadGenerator {
            batch_size: 4,
            max_events: Some(10),
        };
        let mut expected = Vec::new();
        for (batch, ids) in [(0, 0..4), (1, 4..8), (2, 8..10)] {
            for id in ids {
                let (view, row) = generate_event(id);
                expected.push((
                    LoadGeneratorOutput::Nexmark(view),
                    Event::Message(MzOffset::from(batch), (row, Diff::ONE)),
                ));
            }
            expected.push((
                LoadGeneratorOutput::Default,
                Event::Progress(Some(MzOffset::from(batch + 1))),
            ));
        }
        let to_debug = |events: &[(_, _)]| format!("{events:?}");
        assert_eq!(to_debug(&events(config.clone(), 0, 0)), to_debug(&expected));

        // Resuming skips the batches that are already durable.
        let after_first_batch = expected.split_off(5);
        assert_eq!(
            to_debug(&events(config.clone(), 0, 1)),
            to_debug(&after_first_batch)
        );

        // Batches at or below `as_of` share its offset, so they are not durable
        // until the resume offset passes it.
        assert_eq!(events(config.clone(), 1, 1).len(), 13);
        assert_eq!(events(config, 1, 2).len(), 3);
    }
}

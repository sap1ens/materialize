# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

! CREATE SOURCE g
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR NEXMARK (SCALE FACTOR 1)
exact:NEXMARK load generators do not support SCALE FACTOR values

! CREATE SOURCE g
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR NEXMARK (BATCH SIZE 0)
contains:LOAD GENERATOR NEXMARK requires a non-zero BATCH SIZE

! CREATE SOURCE g
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR NEXMARK (TICK INTERVAL '0s')
contains:LOAD GENERATOR NEXMARK requires a TICK INTERVAL of at least 1us

> CREATE SOURCE nexmark
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR NEXMARK (TICK INTERVAL '10ms', BATCH SIZE 100, MAX EVENTS 1000)

> CREATE TABLE person FROM SOURCE nexmark (REFERENCE person);
> CREATE TABLE auction FROM SOURCE nexmark (REFERENCE auction);
> CREATE TABLE bid FROM SOURCE nexmark (REFERENCE bid);

# Out of every 50 events, 1 is a person, 3 are auctions, and 46 are bids.
> SELECT (SELECT count(*) FROM person), (SELECT count(*) FROM auction), (SELECT count(*) FROM bid)
20 60 920

# The first events match Feldera's Nexmark generator.
> SELECT id, name, email_address, credit_card, city, state, (extract(epoch FROM date_time) * 1000)::bigint
  FROM person WHERE id = 1000
1000 "Vicky Noris" Z0PpD@OTy.com "5821 1864 2539 3903" Cheyenne AZ 0

> SELECT id, initial_bid, reserve, (extract(epoch FROM date_time) * 1000)::bigint, (extract(epoch FROM expires) * 1000)::bigint, seller, category
  FROM auction WHERE id = 1000
1000 56275576 56275713 10 3713 1002 12

> SELECT auction, bidder, price, channel, url
  FROM bid WHERE date_time = '1970-01-01 00:00:00.04'
1000 1001 73134520 Facebook https://www.nexmark.com/meta/item.htm?query=1

> SELECT min(id), max(id) FROM person
1000 1019

> DROP SOURCE nexmark CASCADE

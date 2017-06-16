# state-export

A Clojure implementation of a State Delta Subscription Client for Hyperledger
Sawtooth Distributed Ledger.

## Usage

This application assumes you have a Sawtooth validator running, with an
IntegerKey Transaction Processor, as well as a running PostgreSQL database.

From the REPL:

```
user=> (go)
```

After the application has connected to the validator, you can query the current
state using several methods in `state-export.core`.  For example:

```
user=> (def db-spec {:connection-uri "jdbc:postgresql://localhost:5432/intkey?user=intkey_app&password=intkey_proto"})
#'user/db-spec

user=> (require '[state-export.core :as se])
nil

user=> (se/current-block db-spec)
{:block_id "7ebd52d0...",
 :block_num 3,
 :state_root_hash "f04f077097330c0a612c2329d2a69c9c3096cf91514e03f4a28537219f903fca"}

user=> (take 3 (se/current-intkeys db-spec))
({:name "bBcjAX", :value 20450}
 {:name "seUcYf", :value 89932}
 {:name "HLxNHR", :value 82128})
```

### Database

This application requires a PostgreSQL instance running.  This can easily be
done using a docker container.  Run the following commands to get started:

```
$ docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=mypassword -d postgres
$ docker run -it --rm --link postgres:postgres postgres psql -h postgres -U postgres
```

Run the set of SQL statements found in `src/state_export/intke.sql` to
configure the database and user, before starting the application.

## License

Copyright © 2017 Intel, Inc

Distributed under the Apache 2.0 License, same as Hyperledger Sawtooth.

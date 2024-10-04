# Deno KV Python

_Connect to [Deno KV] cloud and [self-hosted] databases from Python._

[Deno KV]: https://deno.com/kv
[self-hosted]: https://deno.com/blog/kv-is-open-source-with-continuous-backup
[denokv server]: https://github.com/denoland/denokv

The `denokv` package is an unofficial Python client for the Deno KV database. It can connect to
both the distributed cloud KV service, or self-hosted [denokv server] (which can be a replica of a cloud KV database, or standalone).

It implements version 3 of the [KV Connect protocol spec, published by Deno](https://github.com/denoland/denokv/blob/main/proto/kv-connect.md).

## Status

The package is under active development and is not yet stable or feature-complete.

**Working**:

- [x] Reading data with kv.get(), kv.list()

**To-do**:

- [ ] [Writing data / transactions](https://docs.deno.com/deploy/kv/manual/transactions/)
- [ ] [Watching for changes](https://docs.deno.com/deploy/kv/manual/operations/#watch)
- [ ] [Queues](https://deno.com/blog/queues)

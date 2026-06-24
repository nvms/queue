# @prsm/queue

redis-backed distributed task queue with grouped concurrency, retries, and rate limiting.

## structure

```
src/
  index.js    - default export
  queue.js    - Queue class
tests/
  queue.test.js
```

## dev

```
make up        # start redis via docker compose
make test      # run tests
make down      # stop redis
```

redis must be running on localhost:6379 for tests.

## key decisions

- plain javascript, ESM, no build step
- single file implementation
- uses `redis` npm package (node-redis), not ioredis
- `@prsm/ms` for parsing duration strings ("100ms", "5s", "1m")
- types generated from JSDoc via `make types`
- cleanup timer is unref'd so it won't keep the process alive
- pushAndWait is distributed via redis pub/sub (queue:result:<uuid>)
- group workers are discovered cross-instance via redis pub/sub (queue:group:notify) and key scan at startup

## namespace (multiple queues)

every redis key is built from `this._keys` (see `buildKeys` in queue.js), never hardcoded. all keys live under the reserved `queue` root. the `namespace` option nests a sub-scope beneath it: default (omitted) keys under `queue:*`, `namespace: "downloads"` keys under `queue:downloads:*`. this lets several queues with different concurrency policies share one redis without colliding, and a namespace can never escape into a sibling package's keyspace (e.g. realtime's `rt:*`). namespace is validated against `/^[A-Za-z0-9._:-]+$/`. the global concurrency semaphore key is also namespaced (`<prefix>:active`), so each namespace gets its own budget. if you add a new redis key, add it to `buildKeys` - do not inline a literal.

## tsc declaration gotcha (TS2742)

`prepublishOnly` runs `tsc -p tsconfig.json` to emit declarations. tsc emits a full `.d.ts` for the exported `Queue` class including private `_`-prefixed fields. `this._semaphore` is assigned `createSemaphore(...)` from `@prsm/lock`; its return type references `@prsm/lock/types/mutex.js` and `semaphore.js`, which tsc cannot name portably, so it raises TS2742 and the publish aborts. fix: `this._semaphore` carries a `/** @type {any} */` annotation so the internal type never leaks into the declaration. do not remove it. any sibling package that depends on `@prsm/lock` and stores a mutex/semaphore on an exported class is exposed to the same trap - annotate the field if its publish fails on TS2742.

## testing

tests use vitest. each test flushes redis in beforeEach. sequential execution.

## publishing

```
npm publish --access public
```

prepublishOnly generates types automatically.

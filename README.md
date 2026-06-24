<p align="center">
  <img src=".github/logo.svg" width="80" height="80" alt="queue logo">
</p>

<h1 align="center">@prsm/queue</h1>

Redis-backed distributed task queue with grouped concurrency, retries, and rate limiting.

## Installation

```bash
npm install @prsm/queue
```

## Quick Start

```js
import Queue from '@prsm/queue'

const queue = new Queue({
  concurrency: 2,
  maxRetries: 3
})

queue.process(async (payload) => {
  return await doWork(payload)
})

queue.on('complete', ({ task, result }) => {
  console.log('Done:', task.uuid, result)
})

queue.on('failed', ({ task, error }) => {
  console.log('Failed after retries:', task.uuid, error.message)
})

await queue.ready()
await queue.push({ userId: 123, action: 'sync' })
```

## Options

```js
const queue = new Queue({
  namespace: 'downloads',   // sub-scope for this queue's keys, nested under the "queue" root
  concurrency: 2,           // max concurrent tasks per instance
  globalConcurrency: 10,    // max concurrent tasks across all instances (Redis-backed)
  delay: '100ms',           // pause between tasks (string or ms)
  timeout: '30s',           // max task duration
  maxRetries: 3,            // attempts before failing
  connectTimeout: '10s',    // max wait for the initial Redis connection (string or ms)

  groups: {
    concurrency: 1,         // max concurrent tasks per group
    delay: '50ms',
    timeout: '10s',
    maxRetries: 3
  },

  redisOptions: {
    host: 'localhost',
    port: 6379
  }
})
```

## Concurrency

Three independent limits compose together. A task must pass all applicable gates before processing.

**`concurrency`** - per-instance limit. Controls how many tasks this server can process simultaneously. This is the number of worker loops created for the main queue, and also caps total active tasks (including grouped) on this instance via an in-memory semaphore. Default: `1`.

**`globalConcurrency`** - cross-instance limit. Controls how many tasks can run across all servers sharing the same Redis. Uses a Redis-backed semaphore with automatic lease expiry for crash safety. If an instance crashes, its slots are reclaimed after 60 seconds. Default: `0` (disabled).

**`groups.concurrency`** - per-group limit. Controls how many tasks can run concurrently within a single group. Default: `1`.

### Examples

Protect local resources (CPU/memory bound):

```js
const queue = new Queue({
  concurrency: 5,
  groups: { concurrency: 1 }
})
```

100 groups each with 1 task - only 5 run at a time on this server.

Protect an external API (shared rate across servers):

```js
const queue = new Queue({
  concurrency: 10,
  globalConcurrency: 20,
  groups: { concurrency: 2 }
})
```

3 servers, each can handle 10 concurrent tasks, but only 20 total across all servers. Each group (tenant) gets up to 2 concurrent slots.

## Multiple Queues

Run several independent queues, each with its own concurrency, retry, and timeout policy, by giving each a distinct `namespace`. Every key a queue touches (tasks, groups, in-flight tracking, results, and the global concurrency budget) lives under the reserved `queue` root; the namespace nests beneath it, so two queues on the same Redis database never see each other's work.

```js
const downloads = new Queue({ namespace: 'downloads', concurrency: 2 })
const llm = new Queue({ namespace: 'llm', concurrency: 10 })

downloads.process(async ({ url, dest }, task, signal) => {
  return await downloadFile(url, dest, signal)
})

llm.process(async ({ prompt }, task, signal) => {
  return await callLLM(prompt, { signal })
})

await Promise.all([downloads.ready(), llm.ready()])

await downloads.push({ url: '...', dest: '...' })
await llm.push({ prompt: 'summarize this' })
```

Both queues run at the same time against one Redis connection target. Downloads stay capped at 2 concurrent transfers while the LLM queue runs up to 10, and a backlog in one never starves the other. Here the keys are `queue:downloads:*` and `queue:llm:*`. An unnamed queue keeps using the plain `queue:*` keys it always has.

Because every key stays under the `queue` root, a namespace can never collide with another package's keyspace on the same Redis - `namespace: 'records'` keys under `queue:records:*`, not `records:*`. A namespace must be a non-empty string of letters, digits, dot, colon, underscore, or dash. Each distinct concurrency policy is one queue, one namespace; if every job type can share the same limits, prefer a single queue and branch inside the handler on a field in the payload.

## Process Handler

```js
queue.process(async (payload, task) => {
  console.log('Task:', task.uuid, 'Attempt:', task.attempts)
  return await someWork(payload)
})
```

Throw an error to trigger retry. After `maxRetries`, the task fails permanently.

The handler receives an `AbortSignal` as its third argument (also available as `task.signal`) that aborts when the per-task `timeout` fires. Pass it to anything that supports cancellation so the work stops instead of running detached while a retry begins.

```js
queue.process(async (payload, task, signal) => {
  const res = await fetch(payload.url, { signal })
  return await res.json()
})
```

## Grouped Queues

Isolated concurrency per key - perfect for per-tenant throttling. Pass `{ group }` as the second argument to `push` or `pushAndWait`.

```js
const queue = new Queue({
  concurrency: 5,
  groups: { concurrency: 1, delay: '50ms' }
})

queue.process(async (payload) => {
  return await callExternalAPI(payload)
})

await queue.ready()

await queue.push({ action: 'sync' }, { group: 'tenant-123' })
await queue.push({ action: 'sync' }, { group: 'tenant-456' })
```

Each tenant processes independently. One slow tenant won't block others. Total concurrent tasks across all tenants is capped by `concurrency`. When the group is conditional, just omit the option - no branching needed.

Groups are fully distributed - any instance can push to any group, and any instance with available concurrency will automatically discover and process tasks for that group. New groups are announced via Redis pub/sub, and existing groups are discovered at startup.

## Push and Wait

Push a task and wait for its result. Works across instances - instance A can push a task that instance B processes, and the result comes back to instance A via Redis pub/sub.

```js
const result = await queue.pushAndWait({ prompt: 'summarize this' })
```

With timeout and groups:

```js
const result = await queue.pushAndWait(
  { prompt: 'summarize this' },
  { group: 'tenant-123', timeout: '30s' }
)
```

Throws if the task fails (after retries are exhausted) or if the timeout is reached. Retries are transparent - if the handler fails twice then succeeds on the third attempt, `pushAndWait` resolves with the successful result.

Cross-instance delivery is at-least-once. The result is sent over Redis pub/sub for low latency and also written to a short-lived key the waiter reads as a fallback, so a dropped pub/sub message does not turn a completed task into a spurious timeout.

## Events

```js
queue.on('new', ({ task }) => {})
queue.on('complete', ({ task, result }) => {})
queue.on('retry', ({ task, error, attempt }) => {})
queue.on('failed', ({ task, error }) => {})
queue.on('drain', () => {})
```

## Task Object

```js
{
  uuid: string,
  payload: any,
  createdAt: number,
  group?: string,     // present when pushed with { group }
  attempts: number,
  signal?: AbortSignal // aborts when the per-task timeout fires
}
```

## Throttling Example

Throttle LLM calls to external providers per tenant:

```js
const queue = new Queue({
  concurrency: 20,
  groups: { concurrency: 2, delay: '50ms' },
  maxRetries: 3
})

queue.process(async ({ prompt }) => {
  return await llm.complete(prompt)
})

app.post('/api/generate', async (req, res) => {
  const { tenantId, prompt } = req.body
  const taskId = await queue.push({ prompt }, { group: tenantId })
  res.json({ queued: true, taskId })
})
```

Each tenant gets up to 2 concurrent LLM calls with a 50ms pause between them. Total concurrent calls across all tenants is capped at 20, protecting your server and API budget from any single tenant overwhelming the system.

## Fan-out with Groups

Use groups to fan out a single event to multiple independent handlers. Each group processes and retries independently.

```js
const queue = new Queue({
  concurrency: 10,
  groups: { concurrency: 1 },
})

const handlers = {
  "email": {
    "user:created": (data) => sendWelcomeEmail(data.email),
    "user:deleted": (data) => sendGoodbyeEmail(data.email),
  },
  "workspace": {
    "user:created": (data) => createDefaultWorkspace(data.userId),
  },
  "slack": {
    "user:created": (data) => notifySlack(`new user: ${data.email}`),
  },
}

queue.process(async ({ event, data }, task) => {
  await handlers[task.group]?.[event]?.(data)
})

await queue.ready()

// emit to all groups
await Promise.all(
  Object.keys(handlers).map((group) =>
    queue.push({ event: "user:created", data: { userId: "u1", email: "a@b.com" } }, { group })
  )
)
```

The `handlers` object is both the registry and the routing logic. Adding a new subscriber is adding a key.

## WebSocket Integration

Queue events are local-only - only the server that processes a task emits `complete`/`failed`. Use [@prsm/realtime](https://github.com/nvms/realtime) to push results to connected clients in real time.

```js
import Queue from '@prsm/queue'
import { RealtimeServer } from '@prsm/realtime'

const realtime = new RealtimeServer({ redis: { host: 'localhost', port: 6379 } })
const queue = new Queue({ concurrency: 5, groups: { concurrency: 1 } })

queue.process(async (payload) => {
  return await generateReport(payload)
})

queue.on('complete', ({ task, result }) => {
  realtime.sendTo(task.payload.connectionId, 'job:complete', result)
})

queue.on('failed', ({ task, error }) => {
  realtime.sendTo(task.payload.connectionId, 'job:failed', { error: error.message })
})

realtime.exposeCommand('generate-report', async (ctx) => {
  const taskId = await queue.push({
    connectionId: ctx.connection.id,
    ...ctx.payload,
  }, { group: ctx.connection.id })
  return { queued: true, taskId }
})

await queue.ready()
await realtime.listen(8080)
```

Both queue and realtime use the same Redis instance. No key conflicts (`queue:*` vs `rt:*`).

## Horizontal Scaling

Multiple servers can push to the same queue. Redis coordinates via atomic operations - no duplicate processing. Use `globalConcurrency` to enforce a hard limit across all instances.

## Startup

```js
await queue.ready()
```

`ready()` resolves once connected. If Redis is unreachable, it rejects after `connectTimeout` (default 10 seconds) instead of hanging, so a process that gates startup on `await queue.ready()` gets a real error to handle. Set `connectTimeout: 0` to wait indefinitely. Once connected, the client follows Redis's default reconnect behavior and rides out transient outages.

## Cleanup

```js
await queue.close()
```

## License

MIT

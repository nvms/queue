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
  concurrency: 2,           // worker count
  delay: '100ms',           // pause between tasks (string or ms)
  timeout: '30s',           // max task duration
  maxRetries: 3,            // attempts before failing

  groups: {
    concurrency: 1,         // workers per group
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

## Process Handler

```js
queue.process(async (payload, task) => {
  console.log('Task:', task.uuid, 'Attempt:', task.attempts)
  return await someWork(payload)
})
```

Throw an error to trigger retry. After `maxRetries`, the task fails permanently.

## Grouped Queues

Isolated concurrency per key - perfect for per-tenant rate limiting.

```js
const queue = new Queue({
  groups: { concurrency: 1, delay: '50ms' }
})

queue.process(async (payload) => {
  return await callExternalAPI(payload)
})

await queue.ready()

await queue.group('tenant-123').push({ action: 'sync' })
await queue.group('tenant-456').push({ action: 'sync' })
```

Each tenant processes independently. One slow tenant won't block others.

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
  groupKey?: string,  // present when pushed via group()
  attempts: number
}
```

## Rate Limiting Example

20 LLM calls/sec per tenant:

```js
const queue = new Queue({
  groups: { concurrency: 20, delay: '50ms' },
  maxRetries: 3
})

queue.process(async ({ prompt }) => {
  return await llm.complete(prompt)
})

app.post('/api/generate', async (req, res) => {
  const { tenantId, prompt } = req.body
  const taskId = await queue.group(tenantId).push({ prompt })
  res.json({ queued: true, taskId })
})
```

## WebSocket Integration with [mesh](https://github.com/nvms/mesh)

Queue events are local-only - only the server that processes a task emits `complete`/`failed`. Use [mesh](https://github.com/nvms/mesh) to push results to connected clients in real time.

Send results to a specific client:

```js
import Queue from '@prsm/queue'
import { MeshServer } from '@mesh-kit/server'

const mesh = new MeshServer({ redis: { host: 'localhost', port: 6379 } })
const queue = new Queue({ groups: { concurrency: 1 } })

queue.process(async (payload) => {
  return await generateReport(payload)
})

queue.on('complete', ({ task, result }) => {
  mesh.sendTo(task.payload.connectionId, 'job:complete', result)
})

queue.on('failed', ({ task, error }) => {
  mesh.sendTo(task.payload.connectionId, 'job:failed', { error: error.message })
})

mesh.exposeCommand('generate-report', async (ctx) => {
  const taskId = await queue.group(ctx.connection.id).push({
    connectionId: ctx.connection.id,
    ...ctx.payload,
  })
  return { queued: true, taskId }
})

await queue.ready()
await mesh.listen(8080)
```

Both queue and mesh use the same Redis instance. No key conflicts (`queue:*` vs `mesh:*`).

## Horizontal Scaling

Multiple servers can push to the same queue. Redis coordinates via atomic operations - no duplicate processing.

## Cleanup

```js
await queue.close()
```

## License

MIT

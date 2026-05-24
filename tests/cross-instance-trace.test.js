import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createClient } from 'redis'
import { createTracer } from '@prsm/trace'
import Queue from '../src/index.js'

const REDIS = {}
let admin

async function flush() {
  if (!admin) {
    admin = createClient()
    admin.on('error', () => {})
    await admin.connect()
  }
  await admin.flushAll()
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const queues = []
function make(opts) {
  const q = new Queue({ redisOptions: REDIS, ...opts })
  queues.push(q)
  return q
}

beforeEach(async () => { await flush() })

afterEach(async () => {
  while (queues.length) await queues.pop().close().catch(() => {})
})

describe('cross-instance trace propagation', () => {
  it('producer span on one instance, consumer span on another, share traceId', async () => {
    const tracer = createTracer({ service: 'svc' })
    const spans = []
    tracer.onSpan((s) => spans.push(s))

    const producer = make({ tracer, concurrency: 0 })
    const consumer = make({ tracer, concurrency: 2 })

    await producer.ready()
    await consumer.ready()

    let processed = null
    consumer.process(async (payload) => { processed = payload; return { ok: true } })

    let pushedTraceId = null
    await tracer.span('http.POST /order', async () => {
      pushedTraceId = tracer.current().traceId
      await producer.push({ orderId: 42 })
    })

    await sleep(400)

    expect(processed).toEqual({ orderId: 42 })

    const pushSpan = spans.find((s) => s.name === 'queue.push')
    const processSpan = spans.find((s) => s.name === 'queue.process')
    expect(pushSpan).toBeTruthy()
    expect(processSpan).toBeTruthy()
    expect(pushSpan.traceId).toBe(pushedTraceId)
    expect(processSpan.traceId).toBe(pushedTraceId)
    expect(processSpan.parentSpanId).toBe(pushSpan.spanId)
  })

  it('producer with no upstream context: push is root, process is its child', async () => {
    const tracer = createTracer({ service: 'svc' })
    const spans = []
    tracer.onSpan((s) => spans.push(s))

    const producer = make({ tracer, concurrency: 0 })
    const consumer = make({ tracer, concurrency: 2 })
    await producer.ready()
    await consumer.ready()
    consumer.process(async () => 'done')

    // push outside any tracer.span — push becomes the trace root
    await producer.push({ x: 1 })
    await sleep(300)

    const pushSpan = spans.find((s) => s.name === 'queue.push')
    const processSpan = spans.find((s) => s.name === 'queue.process')
    expect(pushSpan).toBeTruthy()
    expect(processSpan).toBeTruthy()
    expect(pushSpan.parentSpanId).toBeNull()
    expect(processSpan.parentSpanId).toBe(pushSpan.spanId)
    expect(processSpan.traceId).toBe(pushSpan.traceId)
  })

  it('grouped job carries trace context through the grouped queue path', async () => {
    const tracer = createTracer({ service: 'svc' })
    const spans = []
    tracer.onSpan((s) => spans.push(s))

    const producer = make({ tracer, concurrency: 0 })
    const consumer = make({ tracer, groups: { concurrency: 1 } })
    await producer.ready()
    await consumer.ready()
    consumer.process(async () => 'done')

    let pushedTraceId = null
    await tracer.span('grouped-entry', async () => {
      pushedTraceId = tracer.current().traceId
      await producer.push({ p: 1 }, { group: 'tenant-a' })
    })

    await sleep(400)

    const pushSpan = spans.find((s) => s.name === 'queue.push')
    const processSpan = spans.find((s) => s.name === 'queue.process')
    expect(pushSpan.traceId).toBe(pushedTraceId)
    expect(processSpan.traceId).toBe(pushedTraceId)
  })
})

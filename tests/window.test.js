import { describe, it, expect, afterEach } from "vitest"
import { randomUUID } from "crypto"
import Queue from "../src/index.js"
import { createWindow } from "../src/window.js"

const redisOptions = { host: "127.0.0.1", port: Number(process.env.REDIS_PORT ?? 6379) }

function waitForEvent(emitter, event, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for "${event}"`)), timeout)
    emitter.once(event, (data) => { clearTimeout(timer); resolve(data) })
  })
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

describe("createWindow", () => {
  const at = (iso) => new Date(iso)

  it("opens inside a same-day span and closes outside it", () => {
    const w = createWindow({ from: "09:00", to: "17:00", tz: "UTC" })
    expect(w.isOpen(at("2026-01-01T09:00:00Z"))).toBe(true)
    expect(w.isOpen(at("2026-01-01T16:59:59Z"))).toBe(true)
    expect(w.isOpen(at("2026-01-01T17:00:00Z"))).toBe(false)
    expect(w.isOpen(at("2026-01-01T08:59:59Z"))).toBe(false)
  })

  it("spans midnight when to is earlier than from", () => {
    const w = createWindow({ from: "22:00", to: "05:00", tz: "UTC" })
    expect(w.isOpen(at("2026-01-01T23:30:00Z"))).toBe(true)
    expect(w.isOpen(at("2026-01-01T04:59:00Z"))).toBe(true)
    expect(w.isOpen(at("2026-01-01T05:00:00Z"))).toBe(false)
    expect(w.isOpen(at("2026-01-01T12:00:00Z"))).toBe(false)
  })

  it("reads wall-clock time in the configured zone", () => {
    const w = createWindow({ from: "00:00", to: "05:00", tz: "America/New_York" })
    expect(w.isOpen(at("2026-01-01T06:00:00Z"))).toBe(true)
    expect(w.isOpen(at("2026-01-01T10:00:00Z"))).toBe(false)
  })

  it("computes time until the next boundary", () => {
    const w = createWindow({ from: "22:00", to: "05:00", tz: "UTC" })
    expect(w.msUntilChange(at("2026-01-01T23:30:10Z"))).toBe((5 * 60 + 29) * 60000 + 50000)
    expect(w.msUntilChange(at("2026-01-01T12:00:00Z"))).toBe(10 * 3600000)
  })

  it("rejects malformed input", () => {
    expect(() => createWindow({ from: "9:00", to: "17:00" })).toThrow(/window.from/)
    expect(() => createWindow({ from: "09:00", to: "24:00" })).toThrow(/window.to/)
    expect(() => createWindow({ from: "09:00", to: "09:00" })).toThrow(/differ/)
    expect(() => createWindow({ from: "09:00", to: "17:00", tz: "Nope/Zone" })).toThrow(/window.tz/)
    expect(() => createWindow("09:00-17:00")).toThrow(/Invalid window/)
  })
})

describe("Queue window and pause", () => {
  let queue
  afterEach(async () => { await queue?.close() })

  it("holds tasks while paused and processes them after resume", async () => {
    queue = new Queue({ namespace: randomUUID(), redisOptions })
    const seen = []
    queue.process(async (p) => { seen.push(p) })
    await queue.ready()
    queue.pause()
    expect(queue.paused).toBe(true)
    await queue.push(1)
    await sleep(300)
    expect(seen).toEqual([])
    expect(queue.active).toBe(false)
    const done = waitForEvent(queue, "complete")
    queue.resume()
    await done
    expect(seen).toEqual([1])
    expect(queue.active).toBe(true)
  })

  it("emits pause and resume once per transition across workers", async () => {
    queue = new Queue({ namespace: randomUUID(), concurrency: 3, redisOptions })
    const events = []
    queue.on("pause", () => events.push("pause"))
    queue.on("resume", () => events.push("resume"))
    queue.process(async () => {})
    await queue.ready()
    queue.pause()
    await sleep(100)
    queue.resume()
    await sleep(100)
    expect(events).toEqual(["pause", "resume"])
  })

  it("lets a running task finish after pause", async () => {
    queue = new Queue({ namespace: randomUUID(), redisOptions })
    queue.process(async () => { await sleep(200); return "ok" })
    await queue.ready()
    const done = waitForEvent(queue, "complete")
    await queue.push(1)
    await sleep(50)
    queue.pause()
    const { result } = await done
    expect(result).toBe("ok")
  })

  it("gates workers on a predicate window and re-evaluates it", async () => {
    let open = false
    queue = new Queue({ namespace: randomUUID(), window: () => open, windowInterval: 50, redisOptions })
    const seen = []
    queue.process(async (p) => { seen.push(p) })
    await queue.ready()
    await queue.push("a", { group: "g" })
    await queue.push("b")
    await sleep(300)
    expect(seen).toEqual([])
    expect(queue.active).toBe(false)
    open = true
    await sleep(400)
    expect(seen.sort()).toEqual(["a", "b"])
    expect(queue.active).toBe(true)
  })

  it("treats a throwing predicate as closed", async () => {
    queue = new Queue({ namespace: randomUUID(), window: () => { throw new Error("boom") }, windowInterval: 50, redisOptions })
    queue.process(async () => {})
    await queue.ready()
    await queue.push(1)
    await sleep(200)
    expect(queue.active).toBe(false)
    expect(queue.inFlight).toBe(0)
  })

  it("respects a wall-clock window", async () => {
    const now = new Date()
    const hhmm = (d) => `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`
    const from = hhmm(new Date(now.getTime() + 2 * 60000))
    const to = hhmm(new Date(now.getTime() + 4 * 60000))
    queue = new Queue({ namespace: randomUUID(), window: { from, to, tz: "UTC" }, redisOptions })
    queue.process(async () => {})
    await queue.ready()
    await queue.push(1)
    await sleep(200)
    expect(queue.active).toBe(false)
    const snap = await queue.snapshot()
    expect(snap.window).toEqual({ type: "time", from, to, tz: "UTC" })
    expect(snap.defaultDepth).toBe(1)
  })

  it("closes promptly while paused", async () => {
    queue = new Queue({ namespace: randomUUID(), redisOptions })
    queue.process(async () => {})
    await queue.ready()
    queue.pause()
    const started = Date.now()
    await queue.close()
    queue = null
    expect(Date.now() - started).toBeLessThan(1500)
  })
})

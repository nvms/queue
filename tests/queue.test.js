import { describe, it, expect, beforeEach, afterEach } from "vitest"
import { randomUUID } from "crypto"
import Queue from "../src/index.js"
import { createClient } from "redis"

function waitForEvent(emitter, event, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for "${event}" event`)), timeout)
    emitter.once(event, (data) => { clearTimeout(timer); resolve(data) })
  })
}

function collectEvents(emitter, event) {
  const events = []
  emitter.on(event, (data) => events.push(data))
  return events
}

function waitForN(emitter, event, n, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${n} "${event}" events (got ${count})`)), timeout)
    let count = 0
    emitter.on(event, () => {
      count++
      if (count === n) { clearTimeout(timer); resolve() }
    })
  })
}

function waitForNAcross(emitters, event, n, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${n} "${event}" events across emitters (got ${count})`)), timeout)
    let count = 0
    for (const emitter of emitters) {
      emitter.on(event, () => {
        count++
        if (count === n) { clearTimeout(timer); resolve() }
      })
    }
  })
}

function waitForSettled(emitter, n, timeout = 10000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${n} settled events (got ${count})`)), timeout)
    let count = 0
    const check = () => { count++; if (count === n) { clearTimeout(timer); resolve() } }
    emitter.on("complete", check)
    emitter.on("failed", check)
  })
}

describe("Queue", () => {
  let queue
  let extraQueues = []
  let redis

  beforeEach(async () => {
    redis = createClient()
    await redis.connect()
    await redis.flushAll()
    extraQueues = []
  })

  afterEach(async () => {
    if (queue) await queue.close()
    for (const q of extraQueues) await q.close()
    if (redis?.isOpen) await redis.quit()
  })

  describe("basic functionality", () => {
    it("should push a task and emit new event", async () => {
      queue = new Queue()
      await queue.ready()

      const newPromise = waitForEvent(queue, "new")
      const uuid = await queue.push({ message: "test" })
      const { task } = await newPromise

      expect(uuid).toBeDefined()
      expect(typeof uuid).toBe("string")
      expect(task.uuid).toBe(uuid)
      expect(task.payload).toEqual({ message: "test" })
      expect(task.createdAt).toBeDefined()
      expect(task.attempts).toBe(0)
    })

    it("should process tasks with handler and emit complete with result", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async (payload) => ({ echo: payload.message }))
      await queue.ready()

      const completePromise = waitForEvent(queue, "complete")
      await queue.push({ message: "hello" })
      const { task, result } = await completePromise

      expect(task.payload).toEqual({ message: "hello" })
      expect(task.attempts).toBe(1)
      expect(result).toEqual({ echo: "hello" })
    })

    it("should complete without result when no handler set", async () => {
      queue = new Queue({ concurrency: 1 })
      await queue.ready()

      const completePromise = waitForEvent(queue, "complete")
      await queue.push({ message: "test" })
      const { task, result } = await completePromise

      expect(task).toBeDefined()
      expect(result).toBeUndefined()
    })
  })

  describe("retry logic", () => {
    it("should retry failed tasks up to maxRetries", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 3 })
      queue.process(async () => { attempts++; throw new Error("simulated failure") })

      const retries = collectEvents(queue, "retry")
      const failedPromise = waitForEvent(queue, "failed")

      await queue.ready()
      await queue.push({ message: "retry test" })
      const { task } = await failedPromise

      expect(attempts).toBe(3)
      expect(retries.length).toBe(2)
      expect(retries[0].attempt).toBe(1)
      expect(retries[1].attempt).toBe(2)
      expect(task.attempts).toBe(3)
    })

    it("should succeed on retry if handler recovers", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 3 })
      queue.process(async () => { attempts++; if (attempts < 2) throw new Error("fail first time"); return "success" })

      const completePromise = waitForEvent(queue, "complete")
      await queue.ready()
      await queue.push({ message: "eventual success" })
      const { result } = await completePromise

      expect(attempts).toBe(2)
      expect(result).toBe("success")
    })

    it("should respect custom maxRetries", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 5 })
      queue.process(async () => { attempts++; throw new Error("always fails") })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ message: "max retry test" })
      await failedPromise

      expect(attempts).toBe(5)
    })

    it("should skip retries during shutdown and fail immediately", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 5 })
      queue.process(async () => {
        attempts++
        if (attempts === 2) queue.close()
        throw new Error("fails")
      })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ message: "shutdown retry" })
      await failedPromise

      expect(attempts).toBe(2)

      queue = null
    })
  })

  describe("grouped queues", () => {
    it("should create grouped tasks with group", async () => {
      queue = new Queue()
      await queue.ready()

      const newPromise = waitForEvent(queue, "new")
      const uuid = await queue.push({ message: "grouped test" }, { group: "test-group" })
      const { task } = await newPromise

      expect(uuid).toBeDefined()
      expect(task.group).toBe("test-group")
      expect(task.payload).toEqual({ message: "grouped test" })
    })

    it("should process grouped tasks with handler", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })

      const results = []
      queue.process(async (payload) => { results.push(payload.id); return payload.id })

      const bothDone = waitForN(queue, "complete", 2)
      await queue.ready()
      await queue.push({ id: "a" }, { group: "tenant-1" })
      await queue.push({ id: "b" }, { group: "tenant-1" })
      await bothDone

      expect(results).toContain("a")
      expect(results).toContain("b")
    })

    it("should retry grouped tasks with group-specific maxRetries", async () => {
      let attempts = 0
      queue = new Queue({ groups: { concurrency: 1, maxRetries: 2 }, cleanupInterval: 0 })
      queue.process(async () => { attempts++; throw new Error("group task fails") })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ message: "test" }, { group: "retry-group" })
      const { task } = await failedPromise

      expect(attempts).toBe(2)
      expect(task.group).toBe("retry-group")
    })

    it("should process different groups concurrently", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 50))
        running--
      })

      const allDone = waitForN(queue, "complete", 2)
      await queue.ready()
      await queue.push({ id: 1 }, { group: "g1" })
      await queue.push({ id: 2 }, { group: "g2" })
      await allDone

      expect(maxRunning).toBe(2)
    })
  })

  describe("timeout", () => {
    it("should emit failed on timeout after retries exhausted", async () => {
      queue = new Queue({ timeout: 50, concurrency: 1, maxRetries: 2 })
      queue.process(async () => { await new Promise((resolve) => setTimeout(resolve, 200)); return "should timeout" })

      const retries = collectEvents(queue, "retry")
      const failedPromise = waitForEvent(queue, "failed")

      await queue.ready()
      await queue.push({ message: "timeout test" })
      const { task, error } = await failedPromise

      expect(task).toBeDefined()
      expect(error.message).toBe("Task timeout")
      expect(retries.length).toBe(1)
    })

    it("should use group-specific timeout for grouped tasks", async () => {
      queue = new Queue({ timeout: 5000, groups: { concurrency: 1, timeout: 50 }, maxRetries: 1, cleanupInterval: 0 })
      queue.process(async () => new Promise((r) => setTimeout(r, 200)))

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ id: 1 }, { group: "g1" })
      const { error } = await failedPromise

      expect(error.message).toBe("Task timeout")
    })
  })

  describe("task timeout signal", () => {
    it("aborts the handler signal when the task times out", async () => {
      queue = new Queue({ concurrency: 1, timeout: 50, maxRetries: 1 })

      let sawSignal = false
      let abortReason
      queue.process(async (payload, task, signal) => {
        sawSignal = signal instanceof AbortSignal && signal === task.signal
        await new Promise((resolve) => {
          signal.addEventListener("abort", () => { abortReason = signal.reason; resolve() }, { once: true })
        })
        return "finished-after-abort"
      })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ id: 1 })
      const { error } = await failedPromise

      expect(sawSignal).toBe(true)
      expect(error.message).toBe("Task timeout")
      expect(abortReason?.message).toBe("Task timeout")
    })

    it("does not abort the signal when the handler finishes in time", async () => {
      queue = new Queue({ concurrency: 1, timeout: 1000 })

      let abortedDuringRun = true
      queue.process(async (payload, task, signal) => {
        await new Promise((r) => setTimeout(r, 20))
        abortedDuringRun = signal.aborted
        return "ok"
      })

      const done = waitForEvent(queue, "complete")
      await queue.ready()
      await queue.push({ id: 1 })
      await done

      expect(abortedDuringRun).toBe(false)
    })
  })

  describe("concurrency", () => {
    it("should process tasks in parallel up to concurrency limit", async () => {
      queue = new Queue({ concurrency: 2 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 50))
        running--
      })

      const allDone = waitForN(queue, "complete", 4)
      await queue.ready()
      for (let i = 0; i < 4; i++) await queue.push({ id: i })
      await allDone

      expect(maxRunning).toBe(2)
    })

    it("should cap grouped tasks to local concurrency", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 100))
        running--
      })

      const allDone = waitForN(queue, "complete", 5)
      await queue.ready()
      for (let i = 0; i < 5; i++) await queue.push({ id: i }, { group: `g${i}` })
      await allDone

      expect(maxRunning).toBeLessThanOrEqual(2)
    })

    it("should cap mixed main queue and group tasks to local concurrency", async () => {
      queue = new Queue({ concurrency: 3, groups: { concurrency: 1 }, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 100))
        running--
      })

      const allDone = waitForN(queue, "complete", 6)
      await queue.ready()
      await queue.push({ id: "main-1" })
      await queue.push({ id: "main-2" })
      await queue.push({ id: "group-1" }, { group: "g1" })
      await queue.push({ id: "group-2" }, { group: "g2" })
      await queue.push({ id: "group-3" }, { group: "g3" })
      await queue.push({ id: "group-4" }, { group: "g4" })
      await allDone

      expect(maxRunning).toBeLessThanOrEqual(3)
    })

    it("should enforce per-group concurrency within local limit", async () => {
      queue = new Queue({ concurrency: 10, groups: { concurrency: 1 }, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 100))
        running--
      })

      const allDone = waitForN(queue, "complete", 3)
      await queue.ready()
      await queue.push({ id: 1 }, { group: "single" })
      await queue.push({ id: 2 }, { group: "single" })
      await queue.push({ id: 3 }, { group: "single" })
      await allDone

      expect(maxRunning).toBe(1)
    })
  })

  describe("global concurrency", () => {
    it("should enforce global concurrency across queue instances", async () => {
      const opts = { concurrency: 5, globalConcurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 }
      queue = new Queue(opts)
      const queue2 = new Queue(opts)
      extraQueues.push(queue2)

      let running = 0
      let maxRunning = 0
      const handler = async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 150))
        running--
      }

      queue.process(handler)
      queue2.process(handler)

      const allDone = waitForNAcross([queue, queue2], "complete", 4)
      await queue.ready()
      await queue2.ready()

      await queue.push({ id: 1 }, { group: "g1" })
      await queue.push({ id: 2 }, { group: "g2" })
      await queue2.push({ id: 3 }, { group: "g3" })
      await queue2.push({ id: 4 }, { group: "g4" })
      await allDone

      expect(maxRunning).toBeLessThanOrEqual(2)
    })

    it("should enforce global concurrency on the main queue", async () => {
      const opts = { concurrency: 5, globalConcurrency: 2, cleanupInterval: 0 }
      queue = new Queue(opts)
      const queue2 = new Queue(opts)
      extraQueues.push(queue2)

      let running = 0
      let maxRunning = 0
      const handler = async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 150))
        running--
      }

      queue.process(handler)
      queue2.process(handler)

      const allDone = waitForNAcross([queue, queue2], "complete", 4)
      await queue.ready()
      await queue2.ready()

      await queue.push({ id: 1 })
      await queue.push({ id: 2 })
      await queue2.push({ id: 3 })
      await queue2.push({ id: 4 })
      await allDone

      expect(maxRunning).toBeLessThanOrEqual(2)
    })

    it("should release global slots on close", async () => {
      queue = new Queue({ concurrency: 5, globalConcurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => {
        await new Promise((resolve) => setTimeout(resolve, 100))
        return "done"
      })
      await queue.ready()

      const firstDone = waitForEvent(queue, "complete")
      await queue.push({ id: 1 }, { group: "g1" })
      await firstDone
      await queue.close()
      queue = null

      const activeCount = await redis.zCard("queue:active")
      expect(activeCount).toBe(0)

      queue = new Queue({ concurrency: 5, globalConcurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => "second")
      await queue.ready()

      const secondDone = waitForEvent(queue, "complete")
      await queue.push({ id: 2 }, { group: "g2" })
      const { result } = await secondDone

      expect(result).toBe("second")
    })

    it("local concurrency should be effective cap when lower than global", async () => {
      queue = new Queue({ concurrency: 2, globalConcurrency: 10, groups: { concurrency: 1 }, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 100))
        running--
      })

      const allDone = waitForN(queue, "complete", 5)
      await queue.ready()
      for (let i = 0; i < 5; i++) await queue.push({ id: i }, { group: `g${i}` })
      await allDone

      expect(maxRunning).toBeLessThanOrEqual(2)
    })

    it("global concurrency should be effective cap when lower than local", async () => {
      queue = new Queue({ concurrency: 10, globalConcurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((resolve) => setTimeout(resolve, 100))
        running--
      })

      const allDone = waitForN(queue, "complete", 5)
      await queue.ready()
      for (let i = 0; i < 5; i++) await queue.push({ id: i }, { group: `g${i}` })
      await allDone

      expect(maxRunning).toBeLessThanOrEqual(2)
    })
  })

  describe("semaphore release", () => {
    it("should release semaphore slots during retries", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, maxRetries: 3, cleanupInterval: 0 })
      queue.process(async () => { throw new Error("always fails") })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ id: 1 }, { group: "g1" })
      await failedPromise

      queue.process(async () => "recovered")
      const completeDone = waitForEvent(queue, "complete")
      await queue.push({ id: 2 }, { group: "g2" })
      const { result } = await completeDone
      expect(result).toBe("recovered")
    })

    it("should release semaphore slots on timeout", async () => {
      queue = new Queue({ concurrency: 1, groups: { concurrency: 1, timeout: 50 }, maxRetries: 1, cleanupInterval: 0 })
      queue.process(async () => new Promise((r) => setTimeout(r, 200)))

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ id: 1 }, { group: "g1" })
      await failedPromise

      queue.process(async () => "after-timeout")
      const completeDone = waitForEvent(queue, "complete")
      await queue.push({ id: 2 }, { group: "g2" })
      const { result } = await completeDone
      expect(result).toBe("after-timeout")
    })

    it("should release global semaphore slots during retries", async () => {
      queue = new Queue({ concurrency: 5, globalConcurrency: 1, groups: { concurrency: 1 }, maxRetries: 3, cleanupInterval: 0 })
      queue.process(async () => { throw new Error("fails") })

      const failedPromise = waitForEvent(queue, "failed")
      await queue.ready()
      await queue.push({ id: 1 }, { group: "g1" })
      await failedPromise

      queue.process(async () => "recovered")
      const completeDone = waitForEvent(queue, "complete")
      await queue.push({ id: 2 }, { group: "g2" })
      const { result } = await completeDone
      expect(result).toBe("recovered")
    })
  })

  describe("inFlight tracking", () => {
    it("should track inFlight correctly through success, retry, and failure", async () => {
      queue = new Queue({ concurrency: 2, maxRetries: 2 })
      queue.process(async (payload) => {
        if (payload.shouldFail) throw new Error("fail")
        return "ok"
      })

      await queue.ready()

      const allDone = waitForSettled(queue, 3)
      await queue.push({ id: 1 })
      await queue.push({ id: 2, shouldFail: true })
      await queue.push({ id: 3 })
      await allDone

      expect(queue.inFlight).toBe(0)
    })

    it("should not corrupt inFlight when a listener throws", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async () => "done")

      let listenerCalled = false
      queue.on("complete", () => {
        listenerCalled = true
        throw new Error("bad listener")
      })

      await queue.ready()
      await queue.push({ id: 1 })
      await new Promise((r) => setTimeout(r, 200))

      expect(listenerCalled).toBe(true)
      expect(queue.inFlight).toBe(0)
    })

    it("should recover from listener throws and continue processing", async () => {
      queue = new Queue({ concurrency: 1 })

      let processed = 0
      queue.process(async () => {
        processed++
        return "done"
      })

      let throwOnce = true
      queue.on("complete", () => {
        if (throwOnce) {
          throwOnce = false
          throw new Error("bad listener")
        }
      })

      await queue.ready()
      await queue.push({ id: 1 })
      await new Promise((r) => setTimeout(r, 100))

      const secondDone = waitForEvent(queue, "drain")
      await queue.push({ id: 2 })
      await secondDone

      expect(processed).toBe(2)
      expect(queue.inFlight).toBe(0)
    })
  })

  describe("close behavior", () => {
    it("should wait for in-flight tasks to complete on close", async () => {
      queue = new Queue({ concurrency: 2, cleanupInterval: 0 })

      let completed = 0
      queue.process(async () => {
        await new Promise((r) => setTimeout(r, 200))
        completed++
        return "done"
      })

      await queue.ready()
      await queue.push({ id: 1 })
      await queue.push({ id: 2 })

      await new Promise((r) => setTimeout(r, 50))
      expect(queue.inFlight).toBeGreaterThan(0)

      await queue.close()
      expect(completed).toBeGreaterThan(0)
      queue = null
    })

    it("should release all semaphore slots when closed mid-processing", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => {
        await new Promise((r) => setTimeout(r, 200))
        return "done"
      })
      await queue.ready()

      await queue.push({ id: 1 }, { group: "g1" })
      await queue.push({ id: 2 }, { group: "g2" })
      await new Promise((r) => setTimeout(r, 50))

      await queue.close()
      queue = null

      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => "after-close")
      await queue.ready()

      const done = waitForEvent(queue, "complete")
      await queue.push({ id: 3 }, { group: "g3" })
      const { result } = await done
      expect(result).toBe("after-close")
    })

    it("should release global slots when closed mid-processing", async () => {
      queue = new Queue({ concurrency: 2, globalConcurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => {
        await new Promise((r) => setTimeout(r, 200))
        return "done"
      })
      await queue.ready()

      await queue.push({ id: 1 }, { group: "g1" })
      await new Promise((r) => setTimeout(r, 50))

      await queue.close()
      queue = null

      queue = new Queue({ concurrency: 2, globalConcurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => "after-close")
      await queue.ready()

      const done = waitForEvent(queue, "complete")
      await queue.push({ id: 2 }, { group: "g2" })
      const { result } = await done
      expect(result).toBe("after-close")
    })

    it("should re-queue tasks popped but blocked on local semaphore during close", async () => {
      queue = new Queue({ concurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => {
        await new Promise((r) => setTimeout(r, 500))
      })
      await queue.ready()

      await queue.push({ id: 1 }, { group: "g1" })
      await queue.push({ id: 2 }, { group: "g2" })
      await queue.push({ id: 3 }, { group: "g3" })

      await new Promise((r) => setTimeout(r, 100))
      await queue.close()
      queue = null

      let remaining = 0
      for (const g of ["g1", "g2", "g3"]) {
        remaining += await redis.lLen(`queue:groups:${g}`)
      }

      expect(remaining).toBeGreaterThanOrEqual(2)
    })

    it("should re-queue tasks blocked on global acquire during close", async () => {
      queue = new Queue({ concurrency: 5, globalConcurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => {
        await new Promise((r) => setTimeout(r, 500))
      })
      await queue.ready()

      await queue.push({ id: 1 }, { group: "g1" })
      await queue.push({ id: 2 }, { group: "g2" })

      await new Promise((r) => setTimeout(r, 100))
      await queue.close()
      queue = null

      const remaining = await redis.lLen("queue:groups:g2")
      expect(remaining).toBe(1)
    })

    it("should reject push after close", async () => {
      queue = new Queue()
      await queue.ready()
      await queue.close()

      await expect(queue.push({ id: 1 })).rejects.toThrow("Queue is closed")
      await expect(queue.push({ id: 1 }, { group: "g1" })).rejects.toThrow("Queue is closed")

      queue = null
    })
  })

  describe("drain", () => {
    it("should emit drain after all tasks complete", async () => {
      queue = new Queue({ concurrency: 2 })
      queue.process(async () => "done")
      await queue.ready()

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ id: 1 })
      await queue.push({ id: 2 })
      await drainPromise

      expect(queue.inFlight).toBe(0)
    })

    it("should emit drain after retries exhaust and task fails", async () => {
      queue = new Queue({ concurrency: 1, maxRetries: 2 })
      queue.process(async () => { throw new Error("always fails") })
      await queue.ready()

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ message: "will fail" })
      await drainPromise

      expect(queue.inFlight).toBe(0)
    })

    it("should emit drain after grouped tasks complete", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async () => "done")
      await queue.ready()

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ id: 1 }, { group: "g1" })
      await queue.push({ id: 2 }, { group: "g2" })
      await drainPromise

      expect(queue.inFlight).toBe(0)
    })

    it("should emit drain after the last complete event, not before", async () => {
      queue = new Queue({ concurrency: 2 })
      // gate the handler so neither task can complete until both are pushed and
      // in-flight. otherwise task 1 can finish before push 2's redis round-trip
      // returns, and the queue legitimately drains between the two pushes
      let release
      const gate = new Promise((resolve) => { release = resolve })
      queue.process(async () => { await gate; return "done" })
      await queue.ready()

      const events = []
      queue.on("complete", () => events.push("complete"))
      queue.on("drain", () => events.push("drain"))

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ id: 1 })
      await queue.push({ id: 2 })
      release()
      await drainPromise

      expect(events.filter((e) => e === "complete").length).toBe(2)
      expect(events[events.length - 1]).toBe("drain")
    })

    it("should emit drain after the last failed event, not before", async () => {
      queue = new Queue({ concurrency: 1, maxRetries: 1 })
      queue.process(async () => { throw new Error("fail") })
      await queue.ready()

      const events = []
      queue.on("failed", () => events.push("failed"))
      queue.on("drain", () => events.push("drain"))

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ id: 1 })
      await drainPromise

      expect(events).toEqual(["failed", "drain"])
    })

    it("should emit drain even when a complete listener throws", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async () => "done")
      await queue.ready()

      queue.on("complete", () => { throw new Error("bad listener") })
      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ id: 1 })
      await drainPromise

      expect(queue.inFlight).toBe(0)
    })

    it("should emit drain after mixed success and failure", async () => {
      queue = new Queue({ concurrency: 2, maxRetries: 2 })
      queue.process(async (payload) => {
        if (payload.fail) throw new Error("fail")
        return "ok"
      })
      await queue.ready()

      const drainPromise = waitForEvent(queue, "drain")
      await queue.push({ id: 1 })
      await queue.push({ id: 2, fail: true })
      await queue.push({ id: 3 })
      await drainPromise

      expect(queue.inFlight).toBe(0)
    })
  })

  describe("periodic cleanup", () => {
    it("should not orphan tasks that are retrying when cleanup runs", async () => {
      let attempts = 0
      queue = new Queue({
        concurrency: 1,
        groups: { concurrency: 1, maxRetries: 3 },
        cleanupInterval: 10,
      })
      queue.process(async () => {
        attempts++
        await new Promise((r) => setTimeout(r, 30))
        if (attempts <= 2) throw new Error("fail")
        return "success"
      })

      const completePromise = waitForEvent(queue, "complete")
      await queue.ready()
      await queue.push({ id: 1 }, { group: "g1" })
      const { result } = await completePromise

      expect(result).toBe("success")
      expect(attempts).toBe(3)
    })
  })

  describe("pushAndWait", () => {
    it("should resolve with the handler result", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async (payload) => ({ echo: payload.message }))
      await queue.ready()

      const result = await queue.pushAndWait({ message: "hello" })
      expect(result).toEqual({ echo: "hello" })
    })

    it("should reject when the task fails", async () => {
      queue = new Queue({ concurrency: 1, maxRetries: 1 })
      queue.process(async () => { throw new Error("boom") })
      await queue.ready()

      await expect(queue.pushAndWait({ id: 1 })).rejects.toThrow("boom")
    })

    it("should reject on timeout", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async () => new Promise((r) => setTimeout(r, 500)))
      await queue.ready()

      await expect(queue.pushAndWait({ id: 1 }, { timeout: 50 })).rejects.toThrow("pushAndWait timed out")
    })

    it("should accept duration strings for timeout", async () => {
      queue = new Queue({ concurrency: 1 })
      queue.process(async () => new Promise((r) => setTimeout(r, 500)))
      await queue.ready()

      await expect(queue.pushAndWait({ id: 1 }, { timeout: "50ms" })).rejects.toThrow("pushAndWait timed out")
    })

    it("should resolve the correct task when multiple are in flight", async () => {
      queue = new Queue({ concurrency: 3 })
      queue.process(async (payload) => {
        await new Promise((r) => setTimeout(r, 50))
        return payload.id * 10
      })
      await queue.ready()

      const results = await Promise.all([
        queue.pushAndWait({ id: 1 }),
        queue.pushAndWait({ id: 2 }),
        queue.pushAndWait({ id: 3 }),
      ])

      expect(results).toContain(10)
      expect(results).toContain(20)
      expect(results).toContain(30)
    })

    it("should work with grouped queues", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      queue.process(async (payload) => `processed-${payload.id}`)
      await queue.ready()

      const result = await queue.pushAndWait({ id: "a" }, { group: "tenant-1" })
      expect(result).toBe("processed-a")
    })

    it("should reject for grouped tasks that fail", async () => {
      queue = new Queue({ concurrency: 1, groups: { concurrency: 1, maxRetries: 1 }, cleanupInterval: 0 })
      queue.process(async () => { throw new Error("group boom") })
      await queue.ready()

      await expect(queue.pushAndWait({ id: 1 }, { group: "g1" })).rejects.toThrow("group boom")
    })

    it("should resolve after retries succeed", async () => {
      let attempts = 0
      queue = new Queue({ concurrency: 1, maxRetries: 3 })
      queue.process(async () => {
        attempts++
        if (attempts < 3) throw new Error("not yet")
        return "finally"
      })
      await queue.ready()

      const result = await queue.pushAndWait({ id: 1 })
      expect(result).toBe("finally")
      expect(attempts).toBe(3)
    })
  })

  describe("pushAndWait distributed", () => {
    it("should resolve when a different instance processes the task", async () => {
      queue = new Queue({ concurrency: 0 })
      await queue.ready()

      const worker = new Queue({ concurrency: 1 })
      extraQueues.push(worker)
      worker.process(async (payload) => ({ echo: payload.message }))
      await worker.ready()

      const result = await queue.pushAndWait({ message: "cross-instance" })
      expect(result).toEqual({ echo: "cross-instance" })
    })

    it("should reject when a different instance fails the task", async () => {
      queue = new Queue({ concurrency: 0 })
      await queue.ready()

      const worker = new Queue({ concurrency: 1, maxRetries: 1 })
      extraQueues.push(worker)
      worker.process(async () => { throw new Error("remote failure") })
      await worker.ready()

      await expect(queue.pushAndWait({ id: 1 })).rejects.toThrow("remote failure")
    })

    it("should resolve the correct task across instances with multiple in flight", async () => {
      queue = new Queue({ concurrency: 0 })
      await queue.ready()

      const worker = new Queue({ concurrency: 3 })
      extraQueues.push(worker)
      worker.process(async (payload) => {
        await new Promise((r) => setTimeout(r, 50))
        return payload.id * 10
      })
      await worker.ready()

      const results = await Promise.all([
        queue.pushAndWait({ id: 1 }),
        queue.pushAndWait({ id: 2 }),
        queue.pushAndWait({ id: 3 }),
      ])

      expect(results).toContain(10)
      expect(results).toContain(20)
      expect(results).toContain(30)
    })

    it("should resolve cross-instance with grouped queues", async () => {
      queue = new Queue({ concurrency: 0, cleanupInterval: 0 })
      await queue.ready()

      const worker = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      extraQueues.push(worker)
      worker.process(async (payload) => `processed-${payload.id}`)
      await worker.ready()

      const result = await queue.pushAndWait({ id: "a" }, { group: "tenant-1" })
      expect(result).toBe("processed-a")
    })

    it("should resolve cross-instance after retries succeed", async () => {
      queue = new Queue({ concurrency: 0 })
      await queue.ready()

      let attempts = 0
      const worker = new Queue({ concurrency: 1, maxRetries: 3 })
      extraQueues.push(worker)
      worker.process(async () => {
        attempts++
        if (attempts < 3) throw new Error("not yet")
        return "finally"
      })
      await worker.ready()

      const result = await queue.pushAndWait({ id: 1 })
      expect(result).toBe("finally")
      expect(attempts).toBe(3)
    })

    it("should respect timeout when waiting cross-instance", async () => {
      queue = new Queue({ concurrency: 0 })
      await queue.ready()

      const worker = new Queue({ concurrency: 1 })
      extraQueues.push(worker)
      worker.process(async () => new Promise((r) => setTimeout(r, 500)))
      await worker.ready()

      await expect(queue.pushAndWait({ id: 1 }, { timeout: 50 })).rejects.toThrow("pushAndWait timed out")
    })
  })

  describe("pushAndWait durability", () => {
    it("recovers the result via the durable key when the pub/sub message is lost", async () => {
      queue = new Queue({ concurrency: 0, cleanupInterval: 0 })
      await queue.ready()

      // simulate total pub/sub loss on the waiter: subscribe registers no handler
      const sub = await queue._ensureSubClient()
      const realSubscribe = sub.subscribe.bind(sub)
      sub.subscribe = (channel) => realSubscribe(channel, () => {})

      const worker = new Queue({ concurrency: 1 })
      extraQueues.push(worker)
      worker.process(async (payload) => ({ echo: payload.message }))
      await worker.ready()

      const result = await queue.pushAndWait({ message: "durable" }, { timeout: 5000 })
      expect(result).toEqual({ echo: "durable" })
    })

    it("recovers a failure via the durable key when the pub/sub message is lost", async () => {
      queue = new Queue({ concurrency: 0, cleanupInterval: 0 })
      await queue.ready()

      const sub = await queue._ensureSubClient()
      const realSubscribe = sub.subscribe.bind(sub)
      sub.subscribe = (channel) => realSubscribe(channel, () => {})

      const worker = new Queue({ concurrency: 1, maxRetries: 1 })
      extraQueues.push(worker)
      worker.process(async () => { throw new Error("remote boom") })
      await worker.ready()

      await expect(queue.pushAndWait({ id: 1 }, { timeout: 5000 })).rejects.toThrow("remote boom")
    })

    it("does not persist a durable result key for fire-and-forget push", async () => {
      queue = new Queue({ concurrency: 1, cleanupInterval: 0 })
      queue.process(async () => "ok")
      await queue.ready()

      const uuid = await new Promise((res) => {
        queue.once("complete", ({ task }) => res(task.uuid))
        queue.push({ id: 1 })
      })

      expect(await redis.exists(`queue:result:${uuid}`)).toBe(0)
    })

    it("cleans up the durable key after a waited task resolves", async () => {
      queue = new Queue({ concurrency: 1, cleanupInterval: 0 })
      queue.process(async (payload) => payload.id)
      await queue.ready()

      let uuid
      queue.once("new", ({ task }) => { uuid = task.uuid })
      const result = await queue.pushAndWait({ id: "z" })
      expect(result).toBe("z")

      await new Promise((r) => setTimeout(r, 50))
      expect(await redis.exists(`queue:result:${uuid}`)).toBe(0)
    })
  })

  describe("cross-instance group discovery", () => {
    it("should process grouped tasks pushed by another instance", async () => {
      const worker = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      extraQueues.push(worker)
      worker.process(async (payload) => `done-${payload.id}`)
      await worker.ready()

      queue = new Queue({ concurrency: 0, cleanupInterval: 0 })
      await queue.ready()

      await queue.push({ id: 1 }, { group: "remote-group" })

      const { result } = await waitForEvent(worker, "complete")
      expect(result).toBe("done-1")
    })

    it("should discover existing groups on startup", async () => {
      queue = new Queue({ concurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 0 })
      await queue.ready()

      await redis.lPush("queue:groups:preexisting", JSON.stringify({
        uuid: randomUUID(),
        payload: { id: "pre" },
        createdAt: Date.now(),
        group: "preexisting",
        attempts: 0,
      }))

      const worker = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      extraQueues.push(worker)
      worker.process(async (payload) => `found-${payload.id}`)
      await worker.ready()

      const { result } = await waitForEvent(worker, "complete")
      expect(result).toBe("found-pre")
    })

    it("should enforce per-group concurrency across instances", async () => {
      queue = new Queue({ concurrency: 0, cleanupInterval: 0 })
      await queue.ready()

      const worker = new Queue({ concurrency: 10, groups: { concurrency: 1 }, cleanupInterval: 0 })
      extraQueues.push(worker)

      let running = 0
      let maxRunning = 0
      worker.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((r) => setTimeout(r, 100))
        running--
      })
      await worker.ready()

      const allDone = waitForN(worker, "complete", 3)
      await queue.push({ id: 1 }, { group: "serial" })
      await queue.push({ id: 2 }, { group: "serial" })
      await queue.push({ id: 3 }, { group: "serial" })
      await allDone

      expect(maxRunning).toBe(1)
    })
  })

  describe("group worker recovery", () => {
    it("respawns a group worker that broke while waiting on a global lease and drains the backlog", async () => {
      queue = new Queue({ concurrency: 5, globalConcurrency: 1, groups: { concurrency: 1 }, cleanupInterval: 100 })

      const processed = []
      queue.process(async (payload) => { processed.push(payload.id) })

      // force the first global-lease acquisition to fail exactly as a redis
      // disconnect would: _acquireGlobal returns null, the worker re-pushes the
      // task and breaks out of its loop, leaving the group with no live workers
      const realAcquire = queue._acquireGlobal.bind(queue)
      let forcedBreak = false
      queue._acquireGlobal = async (workerId, activeMap) => {
        if (!forcedBreak) { forcedBreak = true; return null }
        return realAcquire(workerId, activeMap)
      }

      await queue.ready()

      const done = waitForEvent(queue, "complete", 8000)
      await queue.push({ id: "x" }, { group: "g1" })
      await done

      expect(forcedBreak).toBe(true)
      expect(processed).toContain("x")
    })

    it("respawns workers for a group that is registered but has no live workers", async () => {
      queue = new Queue({ concurrency: 2, groups: { concurrency: 1 }, cleanupInterval: 0 })
      const processed = []
      queue.process(async (payload) => { processed.push(payload.id) })
      await queue.ready()

      // reproduce the wedged state a broken worker leaves behind: the group is
      // still registered, has zero live workers, and a task sits in its backlog
      queue._groupWorkers.set("g1", new Map())
      queue._groupInFlight.set("g1", 0)
      await redis.lPush("queue:groups:g1", JSON.stringify({
        uuid: randomUUID(),
        payload: { id: "stranded" },
        createdAt: Date.now(),
        group: "g1",
        attempts: 0,
      }))

      // with cleanup disabled, a fresh push to the same group must respawn
      // workers (via notify) and drain both the stranded and the new task
      const both = waitForN(queue, "complete", 2)
      await queue.push({ id: "new" }, { group: "g1" })
      await both

      expect(processed).toContain("stranded")
      expect(processed).toContain("new")
    })
  })

  describe("groups.concurrency defaults", () => {
    it("should default groups.concurrency to 1 regardless of main concurrency", async () => {
      queue = new Queue({ concurrency: 5, cleanupInterval: 0 })

      let running = 0
      let maxRunning = 0
      queue.process(async () => {
        running++
        maxRunning = Math.max(maxRunning, running)
        await new Promise((r) => setTimeout(r, 100))
        running--
      })

      const allDone = waitForN(queue, "complete", 3)
      await queue.ready()
      await queue.push({ id: 1 }, { group: "single" })
      await queue.push({ id: 2 }, { group: "single" })
      await queue.push({ id: 3 }, { group: "single" })
      await allDone

      expect(maxRunning).toBe(1)
    })
  })

  describe("delay", () => {
    it("should pause between tasks", async () => {
      queue = new Queue({ concurrency: 1, delay: 100 })

      const timestamps = []
      queue.process(async () => { timestamps.push(Date.now()) })

      const allDone = waitForN(queue, "complete", 3)
      await queue.ready()
      await queue.push({ id: 1 })
      await queue.push({ id: 2 })
      await queue.push({ id: 3 })
      await allDone

      for (let i = 1; i < timestamps.length; i++) {
        expect(timestamps[i] - timestamps[i - 1]).toBeGreaterThanOrEqual(80)
      }
    })

    it("should use group-specific delay for grouped tasks", async () => {
      queue = new Queue({ concurrency: 1, groups: { concurrency: 1, delay: 100 }, cleanupInterval: 0 })

      const timestamps = []
      queue.process(async () => { timestamps.push(Date.now()) })

      const allDone = waitForN(queue, "complete", 3)
      await queue.ready()
      await queue.push({ id: 1 }, { group: "g1" })
      await queue.push({ id: 2 }, { group: "g1" })
      await queue.push({ id: 3 }, { group: "g1" })
      await allDone

      for (let i = 1; i < timestamps.length; i++) {
        expect(timestamps[i] - timestamps[i - 1]).toBeGreaterThanOrEqual(80)
      }
    })
  })

  describe("handler metadata", () => {
    it("should pass task metadata as second argument to handler", async () => {
      queue = new Queue({ concurrency: 1 })

      let receivedTask
      queue.process(async (payload, task) => {
        receivedTask = task
        return "done"
      })

      await queue.ready()
      const uuid = await queue.push({ msg: "hello" }, { group: "g1" })
      await waitForEvent(queue, "complete")

      expect(receivedTask.uuid).toBe(uuid)
      expect(receivedTask.payload).toEqual({ msg: "hello" })
      expect(receivedTask.group).toBe("g1")
      expect(receivedTask.attempts).toBe(1)
      expect(typeof receivedTask.createdAt).toBe("number")
    })
  })

  describe("pushAndWait after close", () => {
    it("should reject pushAndWait after close", async () => {
      queue = new Queue()
      await queue.ready()
      await queue.close()

      await expect(queue.pushAndWait({ id: 1 })).rejects.toThrow("Queue is closed")
      queue = null
    })
  })

  describe("startup connection", () => {
    it("should reject ready() instead of hanging when redis is unreachable", async () => {
      const q = new Queue({
        redisOptions: { url: "redis://127.0.0.1:6399" },
        connectTimeout: 500,
        cleanupInterval: 0,
      })

      const start = Date.now()
      await expect(q.ready()).rejects.toThrow(/timed out/i)
      expect(Date.now() - start).toBeLessThan(3000)

      await q.close().catch(() => {})
    })

    it("should accept a duration string for connectTimeout", async () => {
      const q = new Queue({
        redisOptions: { url: "redis://127.0.0.1:6399" },
        connectTimeout: "400ms",
        cleanupInterval: 0,
      })

      await expect(q.ready()).rejects.toThrow(/timed out/i)

      await q.close().catch(() => {})
    })
  })
})

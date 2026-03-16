import { createClient } from "redis"
import { EventEmitter } from "events"
import { randomUUID } from "crypto"
import ms from "@prsm/ms"

/**
 * @typedef {Object} QueueOptions
 * @property {number} [concurrency] - max concurrent tasks per instance (default 1)
 * @property {number} [globalConcurrency] - max concurrent tasks across all instances, Redis-backed (default 0, disabled)
 * @property {number|string} [delay] - pause between tasks, ms or string like "100ms" (default 0)
 * @property {number|string} [timeout] - max task duration, ms or string like "30s" (default 0, no limit)
 * @property {number} [maxRetries] - attempts before failing (default 3)
 * @property {{concurrency?: number, delay?: number|string, timeout?: number|string, maxRetries?: number}} [groups] - overrides for grouped queues
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redisOptions]
 * @property {number} [cleanupInterval] - ms between empty group cleanup (default 30000, 0 to disable)
 */

/**
 * @typedef {Object} Task
 * @property {string} uuid
 * @property {any} payload
 * @property {number} createdAt
 * @property {string} [groupKey]
 * @property {number} attempts
 */

/**
 * @callback TaskHandler
 * @param {any} payload
 * @param {Task} task
 * @returns {Promise<any>|any}
 */

const ACQUIRE_SCRIPT = `
local key = KEYS[1]
local max = tonumber(ARGV[1])
local id = ARGV[2]
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - ttl)
if redis.call('ZCARD', key) < max then
  redis.call('ZADD', key, now, id)
  return 1
end
return 0
`

const RELEASE_SCRIPT = `
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

const RENEW_SCRIPT = `
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
  return 1
end
return 0
`

const LEASE_TTL = 60000
const HEARTBEAT_INTERVAL = 15000

class LocalSemaphore {
  constructor(max) {
    this._max = max
    this._current = 0
    this._waiting = []
  }

  acquire() {
    if (this._current < this._max) {
      this._current++
      return Promise.resolve(true)
    }
    return new Promise((resolve) => this._waiting.push(resolve))
  }

  release() {
    if (this._waiting.length > 0) {
      this._waiting.shift()(true)
    } else {
      this._current--
    }
  }

  releaseAll() {
    for (const resolve of this._waiting) resolve(false)
    this._waiting = []
  }
}

export default class Queue extends EventEmitter {
  /** @param {QueueOptions} [options] */
  constructor(options = {}) {
    super()

    this._options = {
      concurrency: options.concurrency ?? 1,
      globalConcurrency: options.globalConcurrency ?? 0,
      delay: ms(options.delay ?? 0),
      timeout: ms(options.timeout ?? 0),
      maxRetries: options.maxRetries ?? 3,
      groups: {
        concurrency: options.groups?.concurrency ?? 1,
        delay: ms(options.groups?.delay ?? options.delay ?? 0),
        timeout: ms(options.groups?.timeout ?? options.timeout ?? 0),
        maxRetries: options.groups?.maxRetries ?? options.maxRetries ?? 3,
      },
      redisOptions: options.redisOptions ?? {},
      cleanupInterval: options.cleanupInterval ?? 30000,
    }

    this._handler = null
    this._workers = new Map()
    this._groupWorkers = new Map()
    this._workerClients = []
    this._cleanupTimer = null
    this._inFlight = 0
    this._totalSettled = 0
    this._closed = false
    this._localSemaphore = new LocalSemaphore(this._options.concurrency)
    this._activeLeases = new Set()
    this._heartbeats = new Map()

    this._redis = createClient(this._options.redisOptions)
    this._redis.on("error", () => {})
    this._readyPromise = this._initialize()
  }

  /** @returns {Promise<void>} */
  ready() {
    return this._readyPromise
  }

  /** @returns {number} */
  get inFlight() {
    return this._inFlight
  }

  /** @param {TaskHandler} handler */
  process(handler) {
    this._handler = handler
  }

  /**
   * @param {any} payload
   * @returns {Promise<string>}
   */
  async push(payload) {
    const task = { uuid: randomUUID(), payload, createdAt: Date.now(), attempts: 0 }
    await this._redis.lPush("queue:tasks", JSON.stringify(task))
    this.emit("new", { task })
    return task.uuid
  }

  /**
   * @param {string} key
   * @returns {{ push: (payload: any) => Promise<string> }}
   */
  group(key) {
    return {
      push: async (payload) => {
        const task = { uuid: randomUUID(), payload, createdAt: Date.now(), groupKey: key, attempts: 0 }
        await this._redis.lPush(`queue:groups:${key}`, JSON.stringify(task))
        this.emit("new", { task })
        if (!this._groupWorkers.has(key)) {
          this._groupWorkers.set(key, new Map())
          await this._startGroupWorkers(key)
        }
        return task.uuid
      },
    }
  }

  /** @returns {Promise<void>} */
  async close() {
    this._closed = true
    await this._readyPromise.catch(() => {})
    if (this._cleanupTimer) clearInterval(this._cleanupTimer)
    this._workers.clear()
    for (const groupWorkers of this._groupWorkers.values()) groupWorkers.clear()
    this._groupWorkers.clear()
    this._localSemaphore.releaseAll()
    for (const [, interval] of this._heartbeats) clearInterval(interval)
    if (this._redis.isOpen && this._activeLeases.size > 0) {
      await Promise.all(
        Array.from(this._activeLeases).map((id) => this._releaseGlobal(id).catch(() => {}))
      )
    }
    this._heartbeats.clear()
    this._activeLeases.clear()
    for (const client of this._workerClients) {
      if (client.isOpen) await client.disconnect()
    }
    this._workerClients = []
    if (this._redis.isOpen) await this._redis.quit()
  }

  async _initialize() {
    await this._redis.connect()
    await this._startWorkers()
    if (this._options.cleanupInterval > 0) {
      this._cleanupTimer = setInterval(() => this._periodicCleanup(), this._options.cleanupInterval)
      this._cleanupTimer.unref()
    }
  }

  async _createWorkerClient() {
    const client = this._redis.duplicate()
    client.on("error", () => {})
    await client.connect()
    this._workerClients.push(client)
    return client
  }

  async _startWorkers() {
    const ready = []
    for (let i = 0; i < this._options.concurrency; i++) {
      ready.push(this._startWorker(`worker-${i}`))
    }
    await Promise.all(ready)
  }

  async _startGroupWorkers(groupKey) {
    const groupWorkers = this._groupWorkers.get(groupKey)
    const ready = []
    for (let i = 0; i < this._options.groups.concurrency; i++) {
      const workerId = `group-${groupKey}-worker-${i}`
      groupWorkers.set(workerId, true)
      ready.push(this._startGroupWorker(workerId, groupKey))
    }
    await Promise.all(ready)
  }

  async _startWorker(workerId) {
    this._workers.set(workerId, true)
    const client = await this._createWorkerClient()
    this._runWorkerLoop(workerId, client, "queue:tasks", this._workers, (task) => this._processTask(task))
  }

  async _startGroupWorker(workerId, groupKey) {
    const groupWorkers = this._groupWorkers.get(groupKey)
    const client = await this._createWorkerClient()
    this._runWorkerLoop(workerId, client, `queue:groups:${groupKey}`, groupWorkers, (task) => this._processGroupTask(task))
  }

  async _runWorkerLoop(workerId, client, key, activeMap, processFn) {
    const isGrouped = key.startsWith("queue:groups:")
    const delay = isGrouped ? this._options.groups.delay : this._options.delay

    while (activeMap.get(workerId)) {
      try {
        if (!client.isOpen) break
        const taskData = await client.brPop(key, 1)
        if (!taskData) continue

        const task = JSON.parse(taskData.element)

        const localAcquired = await this._localSemaphore.acquire()
        if (!localAcquired) break

        let leaseId = null
        if (this._options.globalConcurrency > 0) {
          leaseId = await this._acquireGlobal(workerId, activeMap)
          if (!leaseId) {
            this._localSemaphore.release()
            break
          }
        }

        this._inFlight++
        try {
          await processFn(task)
        } finally {
          if (leaseId) await this._releaseGlobal(leaseId).catch(() => {})
          this._localSemaphore.release()
        }

        if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay))
      } catch (err) {
        if (err.message?.includes("closed") || err.message?.includes("ClientClosedError")) break
      }
    }
  }

  async _acquireGlobal(workerId, activeMap) {
    const leaseId = randomUUID()
    while (activeMap.get(workerId) && !this._closed) {
      if (!this._redis.isOpen) return null
      const acquired = await this._redis.eval(ACQUIRE_SCRIPT, {
        keys: ["queue:active"],
        arguments: [String(this._options.globalConcurrency), leaseId, String(Date.now()), String(LEASE_TTL)],
      })
      if (acquired) {
        this._activeLeases.add(leaseId)
        const heartbeat = setInterval(() => this._renewGlobal(leaseId).catch(() => {}), HEARTBEAT_INTERVAL)
        heartbeat.unref()
        this._heartbeats.set(leaseId, heartbeat)
        return leaseId
      }
      await new Promise((r) => setTimeout(r, 50))
    }
    return null
  }

  async _releaseGlobal(leaseId) {
    this._activeLeases.delete(leaseId)
    const heartbeat = this._heartbeats.get(leaseId)
    if (heartbeat) {
      clearInterval(heartbeat)
      this._heartbeats.delete(leaseId)
    }
    if (this._redis.isOpen) {
      await this._redis.eval(RELEASE_SCRIPT, {
        keys: ["queue:active"],
        arguments: [leaseId],
      })
    }
  }

  async _renewGlobal(leaseId) {
    if (this._redis.isOpen) {
      await this._redis.eval(RENEW_SCRIPT, {
        keys: ["queue:active"],
        arguments: [leaseId, String(Date.now())],
      })
    }
  }

  async _processTask(task) {
    task.attempts++
    try {
      if (!this._handler) {
        this.emit("complete", { task, result: undefined })
        this._settle()
        return
      }
      const timeoutPromise = this._options.timeout > 0
        ? new Promise((_, reject) => setTimeout(() => reject(new Error("Task timeout")), this._options.timeout))
        : null
      const workPromise = Promise.resolve(this._handler(task.payload, task))
      const result = timeoutPromise ? await Promise.race([workPromise, timeoutPromise]) : await workPromise
      this.emit("complete", { task, result })
      this._settle()
    } catch (error) {
      if (task.attempts < this._options.maxRetries) {
        this.emit("retry", { task, error, attempt: task.attempts })
        this._inFlight--
        await this._redis.lPush("queue:tasks", JSON.stringify(task))
      } else {
        this.emit("failed", { task, error })
        this._settle()
      }
    }
  }

  async _processGroupTask(task) {
    task.attempts++
    try {
      if (!this._handler) {
        this.emit("complete", { task, result: undefined })
        this._settle()
        return
      }
      const timeoutPromise = this._options.groups.timeout > 0
        ? new Promise((_, reject) => setTimeout(() => reject(new Error("Task timeout")), this._options.groups.timeout))
        : null
      const workPromise = Promise.resolve(this._handler(task.payload, task))
      const result = timeoutPromise ? await Promise.race([workPromise, timeoutPromise]) : await workPromise
      this.emit("complete", { task, result })
      this._settle()
    } catch (error) {
      if (task.attempts < this._options.groups.maxRetries) {
        this.emit("retry", { task, error, attempt: task.attempts })
        this._inFlight--
        await this._redis.lPush(`queue:groups:${task.groupKey}`, JSON.stringify(task))
      } else {
        this.emit("failed", { task, error })
        this._settle()
      }
    }
  }

  _settle() {
    this._inFlight--
    this._totalSettled++
    if (this._inFlight === 0 && this._totalSettled > 0) this.emit("drain")
  }

  async _periodicCleanup() {
    try {
      if (!this._redis.isOpen) return
      const groupKeys = Array.from(this._groupWorkers.keys())
      for (const groupKey of groupKeys) {
        const length = await this._redis.lLen(`queue:groups:${groupKey}`)
        if (length === 0) {
          const keyExists = await this._redis.exists(`queue:groups:${groupKey}`)
          if (keyExists) await this._redis.del(`queue:groups:${groupKey}`)
          const groupWorkers = this._groupWorkers.get(groupKey)
          if (groupWorkers) {
            groupWorkers.clear()
            this._groupWorkers.delete(groupKey)
          }
        }
      }
    } catch {}
  }
}

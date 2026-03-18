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
local ttl = tonumber(ARGV[3])
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
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
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], now, ARGV[1])
  return 1
end
return 0
`

const LEASE_TTL = 60000
const HEARTBEAT_INTERVAL = 15000
const CLOSE_TIMEOUT = 5000

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
    } else if (this._current > 0) {
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
    this._groupInFlight = new Map()
    this._workerClients = []
    this._cleanupTimer = null
    this._inFlight = 0
    this._pushed = 0
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
   * @param {{ group?: string }} [options]
   * @returns {Promise<string>}
   */
  async push(payload, { group } = {}) {
    if (this._closed) throw new Error("Queue is closed")
    const task = group
      ? { uuid: randomUUID(), payload, createdAt: Date.now(), groupKey: group, attempts: 0 }
      : { uuid: randomUUID(), payload, createdAt: Date.now(), attempts: 0 }
    this._pushed++
    try {
      await this._enqueue(task, group)
    } catch (err) {
      this._pushed--
      throw err
    }
    this.emit("new", { task })
    return task.uuid
  }

  /**
   * @param {any} payload
   * @param {{ group?: string, timeout?: number|string }} [options]
   * @returns {Promise<any>}
   */
  pushAndWait(payload, { group, timeout = 0 } = {}) {
    if (this._closed) return Promise.reject(new Error("Queue is closed"))
    const task = group
      ? { uuid: randomUUID(), payload, createdAt: Date.now(), groupKey: group, attempts: 0 }
      : { uuid: randomUUID(), payload, createdAt: Date.now(), attempts: 0 }
    this._pushed++
    const result = this._awaitTask(task.uuid, timeout)
    result.catch(() => {})
    return this._enqueue(task, group).then(() => {
      this.emit("new", { task })
      return result
    }, (err) => {
      this._pushed--
      throw err
    })
  }

  /** @private */
  async _enqueue(task, group) {
    if (group) {
      await this._redis.lPush(`queue:groups:${group}`, JSON.stringify(task))
      if (!this._groupWorkers.has(group)) {
        this._groupWorkers.set(group, new Map())
        this._groupInFlight.set(group, 0)
        await this._startGroupWorkers(group)
      }
    } else {
      await this._redis.lPush("queue:tasks", JSON.stringify(task))
    }
  }

  /** @private */
  _awaitTask(uuid, timeout = 0) {
    const ms_ = ms(timeout)
    return new Promise((resolve, reject) => {
      let timer

      const onComplete = ({ task, result }) => {
        if (task.uuid !== uuid) return
        cleanup()
        resolve(result)
      }

      const onFailed = ({ task, error }) => {
        if (task.uuid !== uuid) return
        cleanup()
        reject(error)
      }

      const cleanup = () => {
        if (timer) clearTimeout(timer)
        this.off("complete", onComplete)
        this.off("failed", onFailed)
      }

      if (ms_ > 0) {
        timer = setTimeout(() => {
          cleanup()
          reject(new Error("pushAndWait timed out"))
        }, ms_)
        timer.unref?.()
      }

      this.on("complete", onComplete)
      this.on("failed", onFailed)
    })
  }

  /** @returns {Promise<void>} */
  async close() {
    this._closed = true
    await this._readyPromise.catch(() => {})

    if (this._cleanupTimer) clearInterval(this._cleanupTimer)
    clearTimeout(this._drainTimer)

    this._workers.clear()
    for (const groupWorkers of this._groupWorkers.values()) groupWorkers.clear()
    this._groupWorkers.clear()

    this._localSemaphore.releaseAll()

    if (this._inFlight > 0) {
      await Promise.race([
        new Promise((resolve) => {
          const check = () => { if (this._inFlight <= 0) resolve() }
          this.on("complete", check)
          this.on("failed", check)
        }),
        new Promise((resolve) => setTimeout(resolve, CLOSE_TIMEOUT)),
      ])
    }

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
    const opts = {
      timeout: this._options.timeout,
      maxRetries: this._options.maxRetries,
      retryKey: "queue:tasks",
    }
    this._runWorkerLoop(workerId, client, "queue:tasks", this._workers, opts)
  }

  async _startGroupWorker(workerId, groupKey) {
    const groupWorkers = this._groupWorkers.get(groupKey)
    const client = await this._createWorkerClient()
    const opts = {
      timeout: this._options.groups.timeout,
      maxRetries: this._options.groups.maxRetries,
      retryKey: `queue:groups:${groupKey}`,
      groupKey,
    }
    this._runWorkerLoop(workerId, client, `queue:groups:${groupKey}`, groupWorkers, opts)
  }

  async _runWorkerLoop(workerId, client, key, activeMap, opts) {
    const delay = opts.groupKey ? this._options.groups.delay : this._options.delay

    while (activeMap.get(workerId)) {
      try {
        if (!client.isOpen) break
        const taskData = await client.brPop(key, 1)
        if (!taskData) continue

        const task = JSON.parse(taskData.element)

        const localAcquired = await this._localSemaphore.acquire()
        if (!localAcquired) {
          await this._redis.lPush(key, taskData.element).catch(() => {})
          break
        }

        try {
          let leaseId = null
          if (this._options.globalConcurrency > 0) {
            leaseId = await this._acquireGlobal(workerId, activeMap)
            if (!leaseId) {
              await this._redis.lPush(key, taskData.element).catch(() => {})
              break
            }
          }

          this._inFlight++
          if (opts.groupKey) {
            this._groupInFlight.set(opts.groupKey, (this._groupInFlight.get(opts.groupKey) || 0) + 1)
          }

          try {
            await this._processTask(task, opts)
          } finally {
            if (opts.groupKey) {
              const count = (this._groupInFlight.get(opts.groupKey) || 1) - 1
              if (count <= 0) this._groupInFlight.delete(opts.groupKey)
              else this._groupInFlight.set(opts.groupKey, count)
            }
            if (leaseId) await this._releaseGlobal(leaseId).catch(() => {})
          }
        } finally {
          this._localSemaphore.release()
        }

        if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay))
      } catch {
        if (this._closed || !client.isOpen) break
      }
    }
    if (client.isOpen) await client.disconnect().catch(() => {})
  }

  async _acquireGlobal(workerId, activeMap) {
    const leaseId = randomUUID()
    while (activeMap.get(workerId) && !this._closed) {
      if (!this._redis.isOpen) return null
      const acquired = await this._redis.eval(ACQUIRE_SCRIPT, {
        keys: ["queue:active"],
        arguments: [String(this._options.globalConcurrency), leaseId, String(LEASE_TTL)],
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
        arguments: [leaseId],
      })
    }
  }

  async _processTask(task, opts) {
    task.attempts++
    let timer
    let result
    let handlerError
    let succeeded = false

    if (this._handler) {
      try {
        const timeoutPromise = opts.timeout > 0
          ? new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("Task timeout")), opts.timeout) })
          : null
        const workPromise = Promise.resolve(this._handler(task.payload, task))
        result = timeoutPromise ? await Promise.race([workPromise, timeoutPromise]) : await workPromise
        succeeded = true
      } catch (err) {
        handlerError = err
      } finally {
        if (timer) clearTimeout(timer)
      }
    } else {
      succeeded = true
    }

    if (succeeded) {
      this._settle()
      try { this.emit("complete", { task, result }) } finally { this._emitDrain() }
    } else if (task.attempts < opts.maxRetries && !this._closed) {
      let retried = false
      try {
        await this._redis.lPush(opts.retryKey, JSON.stringify(task))
        retried = true
      } catch {}
      if (retried) {
        this._inFlight--
        this.emit("retry", { task, error: handlerError, attempt: task.attempts })
      } else {
        this._settle()
        try { this.emit("failed", { task, error: handlerError }) } finally { this._emitDrain() }
      }
    } else {
      this._settle()
      try { this.emit("failed", { task, error: handlerError }) } finally { this._emitDrain() }
    }
  }

  _settle() {
    this._inFlight--
    this._totalSettled++
  }

  _emitDrain() {
    clearTimeout(this._drainTimer)
    this._drainTimer = setTimeout(() => {
      if (this._inFlight === 0 && this._totalSettled >= this._pushed) this.emit("drain")
    }, 0)
  }

  async _periodicCleanup() {
    try {
      if (!this._redis.isOpen) return
      for (const groupKey of Array.from(this._groupWorkers.keys())) {
        if ((this._groupInFlight.get(groupKey) || 0) > 0) continue
        const length = await this._redis.lLen(`queue:groups:${groupKey}`)
        if (length > 0) continue
        if ((this._groupInFlight.get(groupKey) || 0) > 0) continue
        const groupWorkers = this._groupWorkers.get(groupKey)
        if (groupWorkers) {
          groupWorkers.clear()
          this._groupWorkers.delete(groupKey)
        }
        this._groupInFlight.delete(groupKey)
      }
    } catch {}
  }
}

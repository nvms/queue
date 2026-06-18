import { createClient } from "redis"
import { EventEmitter } from "events"
import { randomUUID } from "crypto"
import ms from "@prsm/ms"
import { semaphore as createSemaphore } from "@prsm/lock"

/**
 * @typedef {Object} QueueOptions
 * @property {number} [concurrency] - maximum number of tasks this instance processes at once across both the default queue and all groups, enforced by an in-memory semaphore (default 1). This is also the number of worker loops created for the default queue. Set to 0 to make this instance push-only, enqueuing tasks without processing any.
 * @property {number} [globalConcurrency] - maximum number of tasks running concurrently across every instance sharing the same Redis, enforced by a Redis-backed semaphore with lease expiry (default 0, which disables the global limit). If an instance crashes mid-task its slots are reclaimed after the 60 second lease expires, so set a per-task timeout shorter than that to avoid holding slots longer than needed.
 * @property {number|string} [delay] - pause inserted after each task before the worker pulls the next one, as milliseconds or a duration string such as "100ms" (default 0, no pause). Useful as a coarse rate limit per worker.
 * @property {number|string} [timeout] - maximum time a single task may run before it is aborted and treated as a failed attempt, as milliseconds or a duration string such as "30s" (default 0, no limit). When it fires, the AbortSignal passed to the handler is aborted so cancellation-aware work stops instead of running detached during the retry.
 * @property {number} [maxRetries] - total number of attempts allowed before a task fails permanently, counting the first try (default 3). A handler that throws is retried until this count is reached, after which the "failed" event fires.
 * @property {{concurrency?: number, delay?: number|string, timeout?: number|string, maxRetries?: number}} [groups] - per-group overrides applied to grouped tasks. concurrency is the limit within a single group (default 1); delay, timeout, and maxRetries fall back to the top-level values when omitted. The top-level concurrency still caps total in-flight tasks on this instance, so groups partition that budget rather than adding to it.
 * @property {{url?: string, host?: string, port?: number, password?: string}} [redisOptions] - connection options passed to the node-redis client. Provide either a url or discrete host/port/password fields (defaults to redis on localhost:6379).
 * @property {number} [cleanupInterval] - interval in milliseconds at which idle, empty group workers are torn down and dropped default workers are revived (default 30000, 0 to disable). The timer is unref'd so it does not keep the process alive on its own.
 * @property {number|string} [connectTimeout] - maximum time to wait for the initial Redis connection before ready() rejects, as milliseconds or a duration string such as "10s" (default "10s", 0 to wait forever). This bounds startup: node-redis retries a refused connection indefinitely, so without a deadline ready() would hang when Redis is down. It applies only to the initial connect; once connected the client uses Redis's default reconnect behavior to ride out transient outages.
 * @property {object} [tracer] - optional @prsm/trace tracer; when provided, push, pushAndWait, and task processing emit spans (default none).
 */

/**
 * @typedef {Object} Task
 * @property {string} uuid - unique identifier generated when the task is pushed, returned by push() and used to correlate results.
 * @property {any} payload - the value passed to push(), handed to the handler as its first argument.
 * @property {number} createdAt - epoch milliseconds at which the task was pushed.
 * @property {string} [group] - the group key the task was pushed with, present only for grouped tasks.
 * @property {number} attempts - number of times the handler has been invoked for this task, starting at 1 on the first run and incrementing on each retry.
 * @property {AbortSignal} [signal] - aborts when the per-task timeout fires; pass it to cancellation-aware work so it stops instead of running detached during a retry. Non-enumerable, so it is not serialized when the task is re-queued.
 */

/**
 * @callback TaskHandler
 * @param {any} payload - the value the task was pushed with. Throw to trigger a retry; the return value is delivered as the result.
 * @param {Task} task - the full task object, including uuid, attempts, and group.
 * @param {AbortSignal} signal - aborts when the per-task timeout fires; also available as task.signal. Pass it to anything that supports cancellation so the work stops instead of running detached while a retry begins.
 * @returns {Promise<any>|any}
 */

const LEASE_TTL = 60000
const HEARTBEAT_INTERVAL = 15000
const CLOSE_TIMEOUT = 5000
const RESULT_TTL = 60000
const RESULT_POLL_INTERVAL = 1000

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
  /** @param {QueueOptions} [options] - configuration for concurrency, retries, timing, and the Redis connection. */
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
      connectTimeout: ms(options.connectTimeout ?? "10s"),
    }

    this._tracer = options.tracer ?? null
    this._handler = null
    this._workers = new Map()
    this._groupWorkers = new Map()
    this._groupInFlight = new Map()
    this._workerClients = []
    this._cleanupTimer = null
    this._inFlight = 0
    this._defaultInFlight = 0
    this._pushed = 0
    this._totalSettled = 0
    this._inFlightTasks = new Map()
    this._instanceId = randomUUID()
    this._closed = false
    this._localSemaphore = new LocalSemaphore(this._options.concurrency)
    this._activeLeases = new Set()
    this._heartbeats = new Map()

    this._redis = createClient(this._options.redisOptions)
    this._redis.on("error", () => {})
    this._semaphore = this._options.globalConcurrency > 0
      ? createSemaphore({
          max: this._options.globalConcurrency,
          ttl: LEASE_TTL,
          redis: this._options.redisOptions,
          prefix: "",
        })
      : null
    this._subClient = null
    this._groupNotifyClient = null
    this._readyPromise = this._initialize()
    // ready() callers see this rejection; the no-op keeps an unawaited queue
    // from emitting an unhandledRejection when the initial connect fails
    this._readyPromise.catch(() => {})
  }

  /**
   * Resolve once the queue is connected to Redis and workers are running. Rejects if the connection cannot be established within connectTimeout.
   * @returns {Promise<void>}
   */
  ready() {
    return this._readyPromise
  }

  /**
   * Number of tasks this instance is processing right now, across the default queue and all groups.
   * @returns {number}
   */
  get inFlight() {
    return this._inFlight
  }

  /**
   * Point-in-time snapshot of this instance's state and Redis-backed depth counts.
   * Safe to call from observability/devtools - hits Redis with LLEN per known group.
   * @returns {Promise<{
   *   options: object,
   *   inFlight: number,
   *   defaultInFlight: number,
   *   pushed: number,
   *   settled: number,
   *   defaultDepth: number,
   *   workers: { default: number, group: number },
   *   groups: Array<{ name: string, inFlight: number, depth: number, workers: number }>,
   *   inFlightTasks: Array<{ uuid: string, group: string|null, attempts: number, startedAt: number, workerId: string, payload: any }>,
   * }>}
   * @param {{ includePayload?: boolean }} [options] - includePayload attaches each in-flight task's payload to the result (default false, omitted to keep snapshots small and avoid leaking task data into observability output).
   */
  async snapshot({ includePayload = false } = {}) {
    const opts = this._options
    const groupNames = new Set([
      ...this._groupWorkers.keys(),
      ...this._groupInFlight.keys(),
    ])

    let defaultDepth = 0
    const groupDepths = new Map()
    if (this._redis?.isOpen) {
      try {
        defaultDepth = await this._redis.lLen('queue:tasks')
      } catch {}
      for (const name of groupNames) {
        try {
          groupDepths.set(name, await this._redis.lLen(`queue:groups:${name}`))
        } catch {
          groupDepths.set(name, 0)
        }
      }
    }

    const groups = Array.from(groupNames).map((name) => ({
      name,
      inFlight: this._groupInFlight.get(name) || 0,
      depth: groupDepths.get(name) || 0,
      workers: this._groupWorkers.get(name)?.size || 0,
    })).sort((a, b) => a.name.localeCompare(b.name))

    const inFlightTasks = []
    const seen = new Set()
    for (const [uuid, entry] of this._inFlightTasks) {
      seen.add(uuid)
      inFlightTasks.push({
        uuid,
        group: entry.group,
        attempts: entry.task.attempts,
        startedAt: entry.startedAt,
        workerId: entry.workerId,
        instanceId: this._instanceId,
        local: true,
        payload: includePayload ? entry.task.payload : undefined,
      })
    }
    if (this._redis?.isOpen) {
      try {
        const keys = []
        for await (const batch of this._redis.scanIterator({ MATCH: 'queue:inflight:*', COUNT: 100 })) {
          if (Array.isArray(batch)) keys.push(...batch)
          else keys.push(batch)
        }
        if (keys.length) {
          const values = await this._redis.mGet(keys)
          for (const raw of values) {
            if (!raw) continue
            try {
              const entry = JSON.parse(raw)
              if (seen.has(entry.uuid)) continue
              inFlightTasks.push({
                uuid: entry.uuid,
                group: entry.group ?? null,
                attempts: entry.attempts,
                startedAt: entry.startedAt,
                workerId: entry.workerId,
                instanceId: entry.instanceId,
                local: entry.instanceId === this._instanceId,
                payload: includePayload ? entry.payload : undefined,
              })
            } catch {}
          }
        }
      } catch {}
    }
    inFlightTasks.sort((a, b) => a.startedAt - b.startedAt)

    let groupWorkerTotal = 0
    for (const m of this._groupWorkers.values()) groupWorkerTotal += m.size

    return {
      options: {
        concurrency: opts.concurrency,
        globalConcurrency: opts.globalConcurrency,
        delay: opts.delay,
        timeout: opts.timeout,
        maxRetries: opts.maxRetries,
        groups: { ...opts.groups },
      },
      instanceId: this._instanceId,
      inFlight: this._inFlight,
      defaultInFlight: this._defaultInFlight,
      pushed: this._pushed,
      settled: this._totalSettled,
      defaultDepth,
      workers: { default: this._workers.size, group: groupWorkerTotal },
      groups,
      inFlightTasks,
    }
  }

  /**
   * Register the function that processes every task. Only one handler is active; calling this again replaces it.
   * @param {TaskHandler} handler - invoked once per attempt with the payload, task, and an AbortSignal.
   */
  process(handler) {
    this._handler = handler
  }

  /**
   * Enqueue a task for processing and return immediately with its uuid.
   * @param {any} payload - the value handed to the handler when the task runs.
   * @param {{ group?: string }} [options] - group is the key that isolates this task's concurrency from other groups (omit for the default queue).
   * @returns {Promise<string>} the task uuid, resolved once the task is durably enqueued in Redis.
   */
  async push(payload, { group } = {}) {
    if (this._closed) throw new Error("Queue is closed")
    const task = group
      ? { uuid: randomUUID(), payload, createdAt: Date.now(), group, attempts: 0 }
      : { uuid: randomUUID(), payload, createdAt: Date.now(), attempts: 0 }
    this._pushed++
    const span = this._tracer?.startSpan('queue.push', { 'queue.group': group ?? null, 'task.uuid': task.uuid }, { kind: 'producer' })
    if (span) task.traceparent = this._tracer.toTraceparent(span.context)
    try {
      await this._enqueue(task, group)
    } catch (err) {
      this._pushed--
      span?.setError(err)
      throw err
    } finally {
      span?.end()
    }
    this.emit("new", { task })
    return task.uuid
  }

  /**
   * Enqueue a task and resolve with its result once it completes, even if another instance processes it (delivered via Redis pub/sub with a durable key fallback, so delivery is at-least-once).
   * @param {any} payload - the value handed to the handler when the task runs.
   * @param {{ group?: string, timeout?: number|string }} [options] - group isolates this task's concurrency from other groups (omit for the default queue); timeout is how long to wait for a result before rejecting, as milliseconds or a duration string such as "30s" (default 0, wait forever). Retries are transparent, so a task that succeeds on a later attempt still resolves.
   * @returns {Promise<any>} the value the handler returned, or a rejection if the task fails after retries are exhausted or the timeout is reached.
   */
  async pushAndWait(payload, { group, timeout = 0 } = {}) {
    if (this._closed) throw new Error("Queue is closed")
    const task = group
      ? { uuid: randomUUID(), payload, createdAt: Date.now(), group, attempts: 0, awaitResult: true }
      : { uuid: randomUUID(), payload, createdAt: Date.now(), attempts: 0, awaitResult: true }
    const tpSpan = this._tracer?.startSpan('queue.pushAndWait', { 'queue.group': group ?? null, 'task.uuid': task.uuid }, { kind: 'producer' })
    if (tpSpan) {
      task.traceparent = this._tracer.toTraceparent(tpSpan.context)
      tpSpan.end()
    }
    this._pushed++
    const { promise, ready } = this._awaitTask(task.uuid, timeout)
    promise.catch(() => {})
    await ready
    try {
      await this._enqueue(task, group)
    } catch (err) {
      this._pushed--
      throw err
    }
    this.emit("new", { task })
    return promise
  }

  /** @private */
  async _enqueue(task, group) {
    if (group) {
      await this._redis.lPush(`queue:groups:${group}`, JSON.stringify(task))
      await this._ensureGroupWorkers(group)
      this._redis.publish("queue:group:notify", group).catch(() => {})
    } else {
      await this._redis.lPush("queue:tasks", JSON.stringify(task))
    }
  }

  /** @private */
  async _ensureGroupWorkers(group) {
    if (this._closed || this._options.concurrency === 0) return
    const existing = this._groupWorkers.get(group)
    if (existing && existing.size > 0) return
    if (!existing) {
      this._groupWorkers.set(group, new Map())
      this._groupInFlight.set(group, this._groupInFlight.get(group) ?? 0)
    }
    await this._startGroupWorkers(group)
  }

  /** @private */
  async _ensureSubClient() {
    if (this._subClient) return this._subClient
    this._subClient = this._redis.duplicate()
    this._subClient.on("error", () => {})
    await this._connectWithDeadline(this._subClient, "sub")
    return this._subClient
  }

  // node-redis keeps retrying a refused connection forever, so connect() never
  // settles when redis is down at startup. bound it so ready() rejects instead
  // of hanging, while leaving the default infinite reconnect for an already
  // connected client (survives transient outages and failovers)
  /** @private */
  async _connectWithDeadline(client, label) {
    const deadline = this._options.connectTimeout
    if (!deadline || deadline <= 0) return client.connect()
    const connectP = client.connect()
    connectP.catch(() => {})
    let timer
    try {
      await Promise.race([
        connectP,
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error(`Redis connection timed out after ${deadline}ms (${label})`)), deadline)
          timer.unref?.()
        }),
      ])
    } catch (err) {
      try { if (client.isOpen) await client.disconnect() } catch {}
      throw err
    } finally {
      clearTimeout(timer)
    }
  }

  /** @private */
  _awaitTask(uuid, timeout = 0) {
    const ms_ = ms(timeout)
    const channel = `queue:result:${uuid}`
    let resolveReady

    const ready = new Promise((r) => { resolveReady = r })

    const promise = new Promise((resolve, reject) => {
      let timer
      let pollTimer
      let settled = false

      const settle = (fn, value) => {
        if (settled) return
        settled = true
        cleanup()
        fn(value)
      }

      const settleFromPayload = ({ status, result, error }) => {
        if (status === "complete") settle(resolve, result)
        else settle(reject, error ? Object.assign(new Error(error.message), error) : new Error("Task failed"))
      }

      const onLocal = (event) => ({ task, result, error }) => {
        if (task.uuid !== uuid) return
        if (event === "complete") settle(resolve, result)
        else settle(reject, error)
      }

      const onComplete = onLocal("complete")
      const onFailed = onLocal("failed")

      // safety net for a dropped pub/sub message: read the durable result key
      const consumeDurable = async () => {
        if (settled || !this._redis?.isOpen) return
        let raw
        try { raw = await this._redis.get(channel) } catch { return }
        if (!raw || settled) return
        try { settleFromPayload(JSON.parse(raw)) } catch {}
      }

      const cleanup = () => {
        if (timer) clearTimeout(timer)
        if (pollTimer) clearInterval(pollTimer)
        this.off("complete", onComplete)
        this.off("failed", onFailed)
        this._subClient?.unsubscribe(channel).catch(() => {})
        this._redis?.del(channel).catch(() => {})
      }

      if (ms_ > 0) {
        timer = setTimeout(() => {
          consumeDurable().finally(() => settle(reject, new Error("pushAndWait timed out")))
        }, ms_)
        timer.unref?.()
      }

      pollTimer = setInterval(() => { consumeDurable() }, RESULT_POLL_INTERVAL)
      pollTimer.unref?.()

      this.on("complete", onComplete)
      this.on("failed", onFailed)

      this._ensureSubClient().then((sub) => {
        if (settled) { resolveReady(); return }
        sub.subscribe(channel, (message) => {
          try { settleFromPayload(JSON.parse(message)) } catch {}
        }).then(() => resolveReady()).catch(() => resolveReady())
      }).catch(() => resolveReady())
    })

    return { promise, ready }
  }

  /**
   * Stop accepting new tasks, wait briefly for in-flight tasks to settle, release any global concurrency leases, and disconnect every Redis client.
   * @returns {Promise<void>}
   */
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
    if (this._groupNotifyClient?.isOpen) await this._groupNotifyClient.unsubscribe().catch(() => {})
    if (this._groupNotifyClient?.isOpen) await this._groupNotifyClient.disconnect().catch(() => {})
    this._groupNotifyClient = null
    if (this._subClient?.isOpen) await this._subClient.disconnect().catch(() => {})
    this._subClient = null
    if (this._redis.isOpen) await this._redis.quit()
    if (this._semaphore) await this._semaphore.close().catch(() => {})
  }

  async _initialize() {
    await this._connectWithDeadline(this._redis, "main")
    if (this._semaphore) await this._semaphore.peek("queue:active").catch(() => {})
    await this._startWorkers()
    if (this._options.concurrency > 0) {
      await this._subscribeToGroupNotifications()
      await this._discoverExistingGroups()
    }
    if (this._options.cleanupInterval > 0) {
      this._cleanupTimer = setInterval(() => this._periodicCleanup(), this._options.cleanupInterval)
      this._cleanupTimer.unref()
    }
  }

  async _subscribeToGroupNotifications() {
    this._groupNotifyClient = this._redis.duplicate()
    this._groupNotifyClient.on("error", () => {})
    await this._connectWithDeadline(this._groupNotifyClient, "notify")
    await this._groupNotifyClient.subscribe("queue:group:notify", (group) => {
      this._ensureGroupWorkers(group)
    })
  }

  async _discoverExistingGroups() {
    const keys = await this._redis.keys("queue:groups:*")
    for (const key of keys) {
      const group = key.slice("queue:groups:".length)
      await this._ensureGroupWorkers(group)
    }
  }

  async _createWorkerClient() {
    const client = this._redis.duplicate()
    client.on("error", () => {})
    await this._connectWithDeadline(client, "worker")
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
    let client
    try {
      client = await this._createWorkerClient()
    } catch (err) {
      this._workers.delete(workerId)
      throw err
    }
    const opts = {
      timeout: this._options.timeout,
      maxRetries: this._options.maxRetries,
      retryKey: "queue:tasks",
    }
    this._runWorkerLoop(workerId, client, "queue:tasks", this._workers, opts)
  }

  async _startGroupWorker(workerId, groupKey) {
    const groupWorkers = this._groupWorkers.get(groupKey)
    let client
    try {
      client = await this._createWorkerClient()
    } catch (err) {
      groupWorkers?.delete(workerId)
      throw err
    }
    const opts = {
      timeout: this._options.groups.timeout,
      maxRetries: this._options.groups.maxRetries,
      retryKey: `queue:groups:${groupKey}`,
      group: groupKey,
    }
    this._runWorkerLoop(workerId, client, `queue:groups:${groupKey}`, groupWorkers, opts)
  }

  async _runWorkerLoop(workerId, client, key, activeMap, opts) {
    const delay = opts.group ? this._options.groups.delay : this._options.delay

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
          if (opts.group) {
            this._groupInFlight.set(opts.group, (this._groupInFlight.get(opts.group) || 0) + 1)
          } else {
            this._defaultInFlight++
          }
          this._inFlightTasks.set(task.uuid, { task, startedAt: Date.now(), workerId, group: opts.group ?? null })
          this._writeInflightRemote(task, workerId, opts.group).catch(() => {})

          try {
            await this._processTask(task, opts)
          } finally {
            this._inFlightTasks.delete(task.uuid)
            this._clearInflightRemote(task.uuid).catch(() => {})
            if (opts.group) {
              const count = (this._groupInFlight.get(opts.group) || 1) - 1
              if (count <= 0) this._groupInFlight.delete(opts.group)
              else this._groupInFlight.set(opts.group, count)
            } else {
              this._defaultInFlight = Math.max(0, this._defaultInFlight - 1)
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
    activeMap.delete(workerId)
    if (client.isOpen) await client.disconnect().catch(() => {})
  }

  async _acquireGlobal(workerId, activeMap) {
    while (activeMap.get(workerId) && !this._closed) {
      if (!this._redis.isOpen) return null
      const result = await this._semaphore.acquire("queue:active")
      if (result.acquired) {
        const leaseId = result.id
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
    await this._semaphore.release("queue:active", leaseId).catch(() => {})
  }

  async _renewGlobal(leaseId) {
    await this._semaphore.renew("queue:active", leaseId).catch(() => {})
  }

  async _processTask(task, opts) {
    task.attempts++
    let timer
    let result
    let handlerError
    let succeeded = false

    if (this._handler) {
      const parent = this._tracer && task.traceparent ? this._tracer.fromTraceparent(task.traceparent) : null
      const handle = this._tracer?.startSpan('queue.process', {
        'queue.group': task.group ?? null,
        'task.uuid': task.uuid,
        'task.attempt': task.attempts,
      }, {
        kind: 'consumer',
        parent: parent ? { traceId: parent.traceId, spanId: parent.parentSpanId, sampled: parent.sampled } : null,
      })
      const controller = new AbortController()
      // exposed as task.signal too, non-enumerable so it never gets serialized
      // onto the task when it is re-queued for a retry
      Object.defineProperty(task, "signal", { value: controller.signal, configurable: true, enumerable: false })
      const runHandler = async () => {
        const timeoutPromise = opts.timeout > 0
          ? new Promise((_, reject) => {
              timer = setTimeout(() => {
                const err = new Error("Task timeout")
                controller.abort(err)
                reject(err)
              }, opts.timeout)
            })
          : null
        const workPromise = Promise.resolve(this._handler(task.payload, task, controller.signal))
        result = timeoutPromise ? await Promise.race([workPromise, timeoutPromise]) : await workPromise
        succeeded = true
      }
      try {
        if (handle) {
          await this._tracer.run(handle.context, runHandler)
        } else {
          await runHandler()
        }
      } catch (err) {
        handlerError = err
        handle?.setError(err)
      } finally {
        if (timer) clearTimeout(timer)
        handle?.end()
      }
    } else {
      succeeded = true
    }

    if (succeeded) {
      this._settle()
      this._publishResult(task, { status: "complete", result })
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
        this._publishResult(task, { status: "failed", error: { message: handlerError?.message, code: handlerError?.code, name: handlerError?.name } })
        try { this.emit("failed", { task, error: handlerError }) } finally { this._emitDrain() }
      }
    } else {
      this._settle()
      this._publishResult(task, { status: "failed", error: { message: handlerError?.message, code: handlerError?.code, name: handlerError?.name } })
      try { this.emit("failed", { task, error: handlerError }) } finally { this._emitDrain() }
    }
  }

  async _writeInflightRemote(task, workerId, group) {
    if (!this._redis?.isOpen) return
    const timeoutMs = group ? this._options.groups.timeout : this._options.timeout
    const ttlMs = Math.max(60_000, (timeoutMs || 0) * 2)
    try {
      await this._redis.set(
        `queue:inflight:${task.uuid}`,
        JSON.stringify({
          uuid: task.uuid,
          payload: task.payload,
          group: task.group ?? null,
          attempts: task.attempts,
          createdAt: task.createdAt,
          startedAt: Date.now(),
          workerId,
          instanceId: this._instanceId,
        }),
        { PX: ttlMs }
      )
    } catch {}
  }

  async _clearInflightRemote(uuid) {
    if (!this._redis?.isOpen) return
    try { await this._redis.del(`queue:inflight:${uuid}`) } catch {}
  }

  // pub/sub is at-most-once: a waiter on another instance loses the result if
  // the message drops. for waited-on tasks we also persist it to a short-lived
  // key the waiter can read, making delivery at-least-once
  _publishResult(task, payload) {
    if (!this._redis.isOpen) return
    const key = `queue:result:${task.uuid}`
    const data = JSON.stringify(payload)
    this._redis.publish(key, data).catch(() => {})
    if (task.awaitResult) this._redis.set(key, data, { PX: RESULT_TTL }).catch(() => {})
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

  _reviveDefaultWorkers() {
    if (this._closed || this._options.concurrency === 0 || !this._redis.isOpen) return
    for (let i = 0; i < this._options.concurrency; i++) {
      const workerId = `worker-${i}`
      if (!this._workers.get(workerId)) this._startWorker(workerId).catch(() => {})
    }
  }

  async _periodicCleanup() {
    try {
      if (!this._redis.isOpen) return
      this._reviveDefaultWorkers()
      for (const groupKey of Array.from(this._groupWorkers.keys())) {
        if ((this._groupInFlight.get(groupKey) || 0) > 0) continue
        const length = await this._redis.lLen(`queue:groups:${groupKey}`)
        if (length > 0) {
          const groupWorkers = this._groupWorkers.get(groupKey)
          if (!groupWorkers || groupWorkers.size === 0) await this._ensureGroupWorkers(groupKey)
          continue
        }
        if ((this._groupInFlight.get(groupKey) || 0) > 0) continue
        const groupWorkers = this._groupWorkers.get(groupKey)
        if (groupWorkers) {
          groupWorkers.clear()
          this._groupWorkers.delete(groupKey)
        }
        this._groupInFlight.delete(groupKey)
      }
      this._workerClients = this._workerClients.filter((c) => c.isOpen)
    } catch {}
  }
}

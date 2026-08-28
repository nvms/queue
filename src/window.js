const TIME_PATTERN = /^([01]\d|2[0-3]):([0-5]\d)$/
const MINUTES_PER_DAY = 1440

function parseMinutes(label, value) {
  const match = typeof value === "string" && TIME_PATTERN.exec(value)
  if (!match) throw new Error(`Invalid window.${label}: ${JSON.stringify(value)} (expected "HH:mm")`)
  return Number(match[1]) * 60 + Number(match[2])
}

function minuteOfDayFormatter(tz) {
  try {
    return new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", hourCycle: "h23", timeZone: tz })
  } catch {
    throw new Error(`Invalid window.tz: ${JSON.stringify(tz)}`)
  }
}

function currentMinute(formatter, now) {
  let hour = 0
  let minute = 0
  for (const part of formatter.formatToParts(now)) {
    if (part.type === "hour") hour = Number(part.value) % 24
    if (part.type === "minute") minute = Number(part.value)
  }
  return hour * 60 + minute
}

/**
 * @typedef {Object} TimeWindow
 * @property {string} from - wall-clock start of the window as "HH:mm" (24 hour).
 * @property {string} to - wall-clock end of the window as "HH:mm" (24 hour), exclusive. A value earlier than from spans midnight.
 * @property {string} [tz] - IANA time zone the wall-clock times are read in (default: the process time zone).
 */

/**
 * @callback WindowPredicate
 * @returns {boolean|Promise<boolean>} true while workers may start tasks.
 */

export function createWindow(option, pollInterval) {
  if (option === undefined || option === null) return null
  if (typeof option === "function") {
    return { isOpen: () => option(), msUntilChange: () => pollInterval, describe: () => ({ type: "predicate" }) }
  }
  if (typeof option !== "object") throw new Error(`Invalid window: ${JSON.stringify(option)} (expected an object or a function)`)
  const from = parseMinutes("from", option.from)
  const to = parseMinutes("to", option.to)
  if (from === to) throw new Error("Invalid window: from and to must differ")
  const formatter = minuteOfDayFormatter(option.tz)
  const inside = (minute) => (from < to ? minute >= from && minute < to : minute >= from || minute < to)
  return {
    isOpen: (now = new Date()) => inside(currentMinute(formatter, now)),
    msUntilChange: (now = new Date()) => {
      const minute = currentMinute(formatter, now)
      const boundary = inside(minute) ? to : from
      const minutes = (boundary - minute + MINUTES_PER_DAY) % MINUTES_PER_DAY
      return minutes * 60000 - (now.getTime() % 60000)
    },
    describe: () => ({ type: "time", from: option.from, to: option.to, tz: option.tz ?? null }),
  }
}

const http = require('http')
const https = require('https')
const { URL } = require('url')

const logger = require('./logger')

const config = require('../data/config')
const { TimeTracker } = require('./time')

class Database {
  constructor (app) {
    this._app = app
    this._options = config.influx || {}
    this._validateConfig()

    this._token = this._resolveToken()
    this._baseUrl = new URL(this._options.url)
    this._org = this._options.org
    this._bucket = this._options.bucket
    this._writePrecision = this._options.writePrecision || 'ms'
    this._measurements = {
      pings: (this._options.measurements && this._options.measurements.pings) || 'server_pings',
      records: (this._options.measurements && this._options.measurements.records) || 'player_records'
    }

    this._writeBatchSize = Math.max(1, this._options.writeBatchSize || 250)
    this._flushInterval = this._options.flushIntervalMs || 1000
    this._pendingLines = []
    this._flushTimer = null
    this._isFlushing = false
  }

  ensureIndexes (callback) {
    this._verifyBucket()
      .then(() => callback())
      .catch(err => {
        logger.log('error', 'Cannot verify InfluxDB bucket access')
        throw err
      })
  }

  loadGraphPoints (graphDuration, callback) {
    const endTime = TimeTracker.getEpochMillis()
    const startTime = endTime - graphDuration

    this.getRecentPings(startTime, endTime, pingData => {
      const relativeGraphData = []

      for (const row of pingData) {
        let graphData = relativeGraphData[row.ip]
        if (!graphData) {
          relativeGraphData[row.ip] = graphData = [[], []]
        }

        graphData[0].push(row.timestamp)
        graphData[1].push(row.playerCount)
      }

      Object.keys(relativeGraphData).forEach(ip => {
        for (const serverRegistration of this._app.serverRegistrations) {
          if (serverRegistration.data.ip === ip) {
            const graphData = relativeGraphData[ip]
            serverRegistration.loadGraphPoints(startTime, graphData[0], graphData[1])
            break
          }
        }
      })

      if (Object.keys(relativeGraphData).length > 0) {
        const serverIp = Object.keys(relativeGraphData)[0]
        const timestamps = relativeGraphData[serverIp][0]
        this._app.timeTracker.loadGraphPoints(startTime, timestamps)
      }

      callback()
    })
  }

  loadRecords (callback) {
    let completedTasks = 0

    if (this._app.serverRegistrations.length === 0) {
      return callback()
    }

    this._app.serverRegistrations.forEach(serverRegistration => {
      serverRegistration.findNewGraphPeak()

      this.getRecord(serverRegistration.data.ip, (hasRecord, playerCount, timestamp) => {
        if (hasRecord) {
          serverRegistration.recordData = {
            playerCount,
            timestamp: TimeTracker.toSeconds(timestamp)
          }
        } else {
          this.getRecordLegacy(serverRegistration.data.ip, (hasRecordLegacy, playerCountLegacy, timestampLegacy) => {
            let newTimestamp = null
            let newPlayerCount = null

            if (hasRecordLegacy) {
              newTimestamp = timestampLegacy
              newPlayerCount = playerCountLegacy
            }

            serverRegistration.recordData = {
              playerCount: newPlayerCount,
              timestamp: TimeTracker.toSeconds(newTimestamp)
            }

            if (newTimestamp !== null) {
              this._queueLine(this._buildLineProtocol(
                this._measurements.records,
                { ip: serverRegistration.data.ip },
                { playerCount: newPlayerCount },
                newTimestamp
              ))
            }
          })
        }

        if (++completedTasks === this._app.serverRegistrations.length) {
          callback()
        }
      })
    })
  }

  getRecentPings (startTime, endTime, callback) {
    const fluxQuery = `from(bucket: "${this._escapeFluxString(this._bucket)}")
      |> range(start: ${this._toFluxTime(startTime)}, stop: ${this._toFluxTime(endTime)})
      |> filter(fn: (r) => r._measurement == "${this._escapeFluxString(this._measurements.pings)}")
      |> filter(fn: (r) => r._field == "playerCount")
      |> sort(columns: ["_time"])
    `

    this._queryFlux(fluxQuery)
      .then(rows => {
        const normalized = rows.map(row => {
          const status = row.status || 'success'
          const timestamp = new Date(row._time).getTime()
          return {
            ip: row.ip,
            timestamp,
            playerCount: status === 'success' ? Number(row._value) : null
          }
        })
        callback(normalized)
      })
      .catch(err => {
        logger.log('error', 'Cannot get recent pings')
        throw err
      })
  }

  getRecord (ip, callback) {
    const allTimeStart = this._toFluxTime(0)
    const fluxQuery = `from(bucket: "${this._escapeFluxString(this._bucket)}")
      |> range(start: ${allTimeStart})
      |> filter(fn: (r) => r._measurement == "${this._escapeFluxString(this._measurements.records)}")
      |> filter(fn: (r) => r.ip == "${this._escapeFluxString(ip)}")
      |> filter(fn: (r) => r._field == "playerCount")
      |> last()
    `

    this._queryFlux(fluxQuery)
      .then(rows => {
        if (rows.length === 0) {
          // eslint-disable-next-line node/no-callback-literal
          callback(false)
          return
        }

        const row = rows[0]
        const playerCount = Number(row._value)
        const timestamp = new Date(row._time).getTime()
        // eslint-disable-next-line node/no-callback-literal
        callback(true, playerCount, timestamp)
      })
      .catch(err => {
        logger.log('error', `Cannot get ping record for ${ip}`)
        throw err
      })
  }

  getRecordLegacy (ip, callback) {
    const allTimeStart = this._toFluxTime(0)
    const fluxQuery = `from(bucket: "${this._escapeFluxString(this._bucket)}")
      |> range(start: ${allTimeStart})
      |> filter(fn: (r) => r._measurement == "${this._escapeFluxString(this._measurements.pings)}")
      |> filter(fn: (r) => r.ip == "${this._escapeFluxString(ip)}")
      |> filter(fn: (r) => r._field == "playerCount")
      |> filter(fn: (r) => r.status == "success")
      |> max()
    `

    this._queryFlux(fluxQuery)
      .then(rows => {
        if (rows.length === 0 || rows[0]._value === '') {
          // eslint-disable-next-line node/no-callback-literal
          callback(false)
          return
        }

        const row = rows[0]
        const playerCount = Number(row._value)
        const timestamp = new Date(row._time).getTime()
        // eslint-disable-next-line node/no-callback-literal
        callback(true, playerCount, timestamp)
      })
      .catch(err => {
        logger.log('error', `Cannot get legacy ping record for ${ip}`)
        throw err
      })
  }

  insertPing (ip, timestamp, unsafePlayerCount) {
    const isSuccess = typeof unsafePlayerCount === 'number' && !Number.isNaN(unsafePlayerCount)
    const playerCount = isSuccess ? unsafePlayerCount : 0
    const status = isSuccess ? 'success' : 'failed'

    const line = this._buildLineProtocol(
      this._measurements.pings,
      { ip, status },
      { playerCount: playerCount },
      timestamp
    )

    this._queueLine(line)
  }

  updatePlayerCountRecord (ip, playerCount, timestamp) {
    const line = this._buildLineProtocol(
      this._measurements.records,
      { ip },
      { playerCount },
      timestamp
    )

    this._queueLine(line)
  }

  initOldPingsDelete (callback) {
    logger.info('InfluxDB bucket retention rules manage old ping cleanup.')
    callback()
  }

  deleteOldPings (callback) {
    if (callback) {
      callback()
    }
  }

  _queueLine (line) {
    this._pendingLines.push(line)

    if (this._pendingLines.length >= this._writeBatchSize) {
      this._flushLines()
      return
    }

    if (!this._flushTimer) {
      this._flushTimer = setTimeout(() => {
        this._flushTimer = null
        this._flushLines()
      }, this._flushInterval)

      if (typeof this._flushTimer.unref === 'function') {
        this._flushTimer.unref()
      }
    }
  }

  _flushLines () {
    if (this._isFlushing || this._pendingLines.length === 0) {
      return
    }

    if (this._flushTimer) {
      clearTimeout(this._flushTimer)
      this._flushTimer = null
    }

    const payload = this._pendingLines.join('\n')
    this._pendingLines = []
    this._isFlushing = true

    this._writeLines(payload)
      .catch(err => {
        logger.log('error', `Failed to write data to InfluxDB: ${err.message}`)
      })
      .finally(() => {
        this._isFlushing = false
        if (this._pendingLines.length > 0) {
          this._flushLines()
        }
      })
  }

  _writeLines (payload) {
    const path = `/api/v2/write?org=${encodeURIComponent(this._org)}&bucket=${encodeURIComponent(this._bucket)}&precision=${encodeURIComponent(this._writePrecision)}`

    return this._request({
      method: 'POST',
      path,
      headers: {
        'Content-Type': 'text/plain; charset=utf-8'
      },
      body: payload,
      expectedStatuses: [204]
    })
  }

  _queryFlux (fluxQuery) {
    const path = `/api/v2/query?org=${encodeURIComponent(this._org)}`
    const body = JSON.stringify({
      query: fluxQuery,
      type: 'flux',
      dialect: {
        annotations: [],
        delimiter: ',',
        header: true
      }
    })

    return this._request({
      method: 'POST',
      path,
      headers: {
        'Content-Type': 'application/json',
        Accept: 'text/csv'
      },
      body,
      expectedStatuses: [200]
    }).then(res => this._parseFluxCsv(res.body))
  }

  _verifyBucket () {
    const path = `/api/v2/buckets?name=${encodeURIComponent(this._bucket)}&org=${encodeURIComponent(this._org)}`

    return this._request({
      method: 'GET',
      path,
      headers: {
        Accept: 'application/json'
      },
      expectedStatuses: [200]
    }).then(res => {
      let payload
      try {
        payload = JSON.parse(res.body || '{}')
      } catch (err) {
        throw new Error('Unable to parse bucket response')
      }

      if (!payload.buckets || payload.buckets.length === 0) {
        throw new Error(`Bucket "${this._bucket}" not found or inaccessible`)
      }
    })
  }

  _request ({ method, path, headers = {}, body = '', expectedStatuses = [200, 204] }) {
    const targetUrl = new URL(path, this._baseUrl)
    const isHttps = targetUrl.protocol === 'https:'
    const transport = isHttps ? https : http

    const options = {
      method,
      hostname: targetUrl.hostname,
      port: targetUrl.port || (isHttps ? 443 : 80),
      path: `${targetUrl.pathname}${targetUrl.search}`,
      headers: {
        Authorization: `Token ${this._token}`,
        Accept: 'application/json',
        ...headers
      }
    }

    return new Promise((resolve, reject) => {
      const req = transport.request(options, res => {
        const chunks = []
        res.on('data', chunk => chunks.push(chunk))
        res.on('end', () => {
          const responseBody = Buffer.concat(chunks).toString()

          if (!expectedStatuses.includes(res.statusCode)) {
            return reject(new Error(`InfluxDB responded with status ${res.statusCode}: ${responseBody}`))
          }

          resolve({ statusCode: res.statusCode, body: responseBody })
        })
      })

      req.on('error', reject)

      if (body) {
        req.write(body)
      }

      req.end()
    })
  }

  _parseFluxCsv (csvText) {
    if (!csvText) {
      return []
    }

    const lines = csvText.split(/\r?\n/).filter(Boolean)
    const rows = []
    let headers = null

    for (const line of lines) {
      if (line.startsWith('#')) {
        continue
      }

      const values = this._splitCsvLine(line)
      if (!headers) {
        headers = values
        continue
      }

      const row = {}
      headers.forEach((key, index) => {
        row[key] = values[index]
      })
      rows.push(row)
    }

    return rows
  }

  _splitCsvLine (line) {
    const result = []
    let current = ''
    let inQuotes = false

    for (let i = 0; i < line.length; i++) {
      const char = line[i]

      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"'
          i++
        } else {
          inQuotes = !inQuotes
        }
      } else if (char === ',' && !inQuotes) {
        result.push(current)
        current = ''
      } else {
        current += char
      }
    }

    result.push(current)
    return result
  }

  _buildLineProtocol (measurement, tags, fields, timestamp) {
    const measurementName = this._escapeMeasurement(measurement)
    const tagString = Object.entries(tags)
      .filter(([, value]) => value !== undefined && value !== null)
      .map(([key, value]) => `${this._escapeTag(key)}=${this._escapeTag(String(value))}`)
      .join(',')

    const fieldString = Object.entries(fields)
      .filter(([, value]) => value !== undefined && value !== null)
      .map(([key, value]) => `${this._escapeTag(key)}=${this._formatFieldValue(value)}`)
      .join(',')

    const line = tagString ? `${measurementName},${tagString} ${fieldString} ${Math.floor(timestamp)}` : `${measurementName} ${fieldString} ${Math.floor(timestamp)}`
    return line
  }

  _formatFieldValue (value) {
    if (typeof value === 'number' && Number.isInteger(value)) {
      return `${value}i`
    }

    if (typeof value === 'number') {
      return value.toString()
    }

    if (typeof value === 'boolean') {
      return value ? 'true' : 'false'
    }

    return `"${String(value).replace(/"/g, '\\"')}"`
  }

  _escapeTag (value) {
    return value.replace(/,/g, '\\,').replace(/ /g, '\\ ').replace(/=/g, '\\=')
  }

  _escapeMeasurement (value) {
    return this._escapeTag(value)
  }

  _escapeFluxString (value) {
    return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"')
  }

  _toFluxTime (timestamp) {
    return `time(v: ${new Date(timestamp).toISOString()})`
  }

  _validateConfig () {
    if (!this._options.url || !this._options.bucket || !this._options.org) {
      throw new Error('Missing InfluxDB configuration. Please set config.influx.url, org, and bucket.')
    }
  }

  _resolveToken () {
    if (this._options.token) {
      return this._options.token
    }

    if (this._options.tokenEnvVar && process.env[this._options.tokenEnvVar]) {
      return process.env[this._options.tokenEnvVar]
    }

    throw new Error('Missing InfluxDB API token. Provide config.influx.token or config.influx.tokenEnvVar environment variable.')
  }
}

module.exports = Database

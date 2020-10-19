const moment = require('moment')
const systemDB = require('./common/index.js')
const fs = require('fs')
const xml2js = require('xml2js')
const parser = new xml2js.Parser()
const log4js = require('./server/logger/logger')
const logger = log4js.getLogger('countOffset')
const Kafka = require('no-kafka')
const { forceFromBegin } = require('./config.js')

function getHeader(d) {
  let tableHeader = Object.keys(d)
  return tableHeader
}

function getChangeFlag(data, flag) {
  if (data[flag] == 'Y') return true
  else return false
}

function isInvalidDate(date) {

  return !isNaN(Date.parse(date))
}

function isInvalidTime(time) {
  let isInvalid = /^([0-1]?[0-9]|2[0-4])([0-5][0-9])([0-5][0-9])?$/.test(time)

  return !isInvalid
}

function replaceQuotes(value) {
  if (isNumber(value)) {
    return `'${value}'`
  } else if (value == null || value == '') {
    return `null`
  }

  return (/\'/).test(value) ? `'${value.replace(/\'/g, '"')}'` : `'${value}'`
}

function getCol(obj, colList) {
  return colList.reduce((pre, col) => {
    let newObj = {}
    newObj[col] = obj[col] ? obj[col] : ['']
    return Object.assign(pre, newObj)
  }, {})
}
const isNumber = function (num) {
  if (num == null || num.toString().replace(/\s/g, '') == '') {
    return false
  }
  // check Number
  num = Number(num)
  if(isNaN(num)){
    return false
  } else {
    return true
  }
}

function getColToObj(obj, colList) {
  return colList.reduce((pre, col) => {
    let newObj = {}
    newObj[col] = isNumber(obj[col]) ? obj[col] : obj[col] ? obj[col] : null
    return Object.assign(pre, newObj)
  }, {})
}

const deleteDataBySeq = async (tableName, key, value, fileName, client = null) => {
  let flag = false
  fileName = fileName.replace('.XML', '').substr(0, 30)
  // 如果 pramary key 沒有值 則跳出
  key.map((k) => {
    if (value[k] == '') {
      flag = true
      return
    }
  })

  if (!flag) {
    let expression = key.reduce((k, i) => {
      if (k != '') k += ' and '
      return k + i + '=' + `'${value[i]}'`
    }, '')

    try {
      let sql = `WITH deleted AS (
        delete from ${tableName} where ${expression} and ('${fileName}' > update_by or update_by ='init' or update_by is null)
        RETURNING *) SELECT count(*) FROM deleted;
        `

      if (client) {
        return await client.query(sql)
      } else {
        return await systemDB.Query(sql)
      }
    } catch (e) {
      logger.error('can not delete data', e, tableName, key, value)
      throw Error('delete data error')
    }
  }
}

async function deleteKeyValue(tableName, key, value, client = null) {
  let flag = false

  // 如果 pramary key 沒有值 則跳出
  key.map((k) => {
    if (value[k] == '') {
      flag = true
      return
    }
  })

  if (!flag) {
    let expression = key.reduce((k, i) => {
      if (k != '') k += ' and '
      return k + i + '=' + `'${value[i]}'`
    }, '')

    let res = await systemDB.Query(`select ${key} from ${tableName} where ${expression}`)
    try {
      if (res.rows.length >= 1) {
        if (client) {
          await client.query(`delete from ${tableName} where ${expression}`)
        } else {
          await systemDB.Query(`delete from ${tableName} where ${expression}`)
        }

      }
    } catch (e) {
      throw Error('delete data error')
    }
  }
}

async function insertLogs(log) {
  let sql = `INSERT INTO wiprocurement.logs_kafka (logtype, logname, fetch_count, update_time, dura_sec, status, rsv1, rsv2, rsv3) 
  VALUES ('${log.logtype}', '${log.logname}', '${log.fetch_count}', '${log.update_time}', '${log.dura_sec}', '${log.status}', '${log.rsv1}', '${log.rsv2}', '${log.rsv3}') ON CONFLICT DO NOTHING RETURNING uuid;`
  let result = await systemDB.Query(sql)
  return result.rows[0]
}

async function insertFinishedLog(topic, partition, offset, filename, updatetime, updateby) {
  let sql = `INSERT INTO wiprocurement.logs_kafka_finished (topic, partition, "offset", filename, update_time, update_by) 
  VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (topic, partition, "offset", filename) DO UPDATE SET filename=excluded.filename, update_time=excluded.update_time, update_by=excluded.update_by;`
  let result = await systemDB.Query(sql, [topic, partition, offset, filename, updatetime, updateby])
  return result
}


async function checkFinishedLog(topic, partition, offset, filename) {
  let sql = `SELECT filename, update_time, update_by FROM wiprocurement.logs_kafka_finished 
  WHERE topic = $1 AND partition = $2 AND "offset" = $3 AND filename = $4;`
  let result = await systemDB.Query(sql, [topic, partition, offset, filename])
  return result
}

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array)
  }
}


const batchUpsertOnSeq = async (tableName, key, values, fileName, client = null) => {
  if (values.length > 0) {
    let sqls = []
    for (let i = 0; i < values.length; i++) {
      let value = values[i]
      let flag = false
      key.map((k) => {
        if (value[k] == '') {
          flag = true
          return
        }
      })
      if (!flag) {
        try {
          let tableHeader = getHeader(value)
          let insertValue = Object.values(value).map(v => {
            let checkValue = Array.isArray(v) ? v[0] : v
            return replaceQuotes(checkValue)
          }).join()

          let updateTime = moment().format('YYYY-MM-DD HH:mm:ssZZ')
          fileName = fileName.replace('.XML', '').substr(0, 100)

          let sql = qerenalInsertSQL(tableName, tableHeader, insertValue, updateTime, fileName, key, value)
          
          // plm kafka data 不需要 where update by 條件去判斷更新
          if (tableName != 'wiprocurement.sapalt' && tableName != 'wiprocurement.plm_pcbform') {
            sql = whereKafkaSQL(sql, tableName)
          }

          // plm_pcbform 需要去判斷 sync_ts 時間是否較大
          if (tableName == 'wiprocurement.plm_pcbform') {
            sql = whereKafkaSQLBySyncTs(sql, tableName)
          }
          
          sqls.push(sql)
        } catch (e) {
          throw Error('insert bulk data error')
        }
      } else {
        throw Error('insert data key is null, error')
      }
    }
    if (client) {
      await client.query(sqls.join(''))
    } else {
      await systemDB.Query(sqls.join(''))
    }
  }
}

const qerenalInsertSQL = (tableName, tableHeader, insertValue, updateTime, fileName, key, value) => {
  return `Insert into ${tableName} (${tableHeader}, update_time, update_by) VALUES (${insertValue}, '${updateTime}', '${fileName}') 
          ON CONFLICT (${key.join()}) DO 
          UPDATE SET ${updataValue(value)}, update_time='${updateTime}', update_by='${fileName}'`
}

const whereKafkaSQL = (sql, tableName) => {
  return sql + ` where EXCLUDED.update_by > ${tableName}.update_by or ${tableName}.update_by is null or ${tableName}.update_by = 'init';`
}

const whereKafkaSQLBySyncTs = (sql, tableName) => {
  return sql + ` where EXCLUDED.sync_ts > ${tableName}.sync_ts;`
}

async function selectKeyValueBulk(tableName, key, values, fileName, client = null) {
  if (values.length > 0) {
    let sqls = []
    for (let i = 0; i < values.length; i++) {
      const value = values[i]
      let flag = false
      key.map((k) => {
        if (value[k] == '') {
          flag = true
          return
        }
      })
      if (!flag) {
        try {
          let tableHeader = getHeader(value)
          let insertValue = Object.values(value).map(v => replaceQuotes(v[0])).join()
          let updateTime = moment().format('YYYY-MM-DD HH:mm:ssZZ')
          fileName = fileName.replace('.XML', '').substr(0, 30)
          let sql = `Insert into ${tableName} (${tableHeader}, update_time, update_by) VALUES (${insertValue}, '${updateTime}', '${fileName}') 
          ON CONFLICT (${key.join()}) DO 
            UPDATE SET ${updataValue(value)}, update_time='${updateTime}', update_by='${fileName}';`
          sqls.push(sql)
          // console.log(sql)
        } catch (e) {
          throw Error('insert bulk data error')
        }
      }
    }
    if (client) {
      await client.query(sqls.join(''))
    } else {
      await systemDB.Query(sqls.join(''))
    }

  }

}

function updataValue(value) {
  return Object.keys(value).reduce((k, i) => {
    if (k != '') k += ','
    return k + i + '=EXCLUDED.' + i
  }, '')
}

class Counter {
  constructor() {
    this.topic = null
    this.partion = null
    this.count = 0
    this.offset = null
    this.Finished = 0
    this.Error = 0
    this.Ignore = 0
    this.Pass = 0
  }
  show() {
    // console.log()
    return `Counter: ${this.count} (Finished: ${this.Finished} /Error: ${this.Error} /Pass: ${this.Pass} /Ignore: ${this.Ignore}) CurrentOffset: ${this.offset}`
  }
}

class Log {
  constructor(type, name, status, rsv1, rsv2, rsv3) {
    this.logtype = type
    this.logname = name
    this.fetch_count = 0
    this.update_time = moment().format('YYYY-MM-DD HH:mm:ssZZ')
    this.dura_sec = 0
    this.status = status || null
    this.rsv1 = rsv1 || null
    this.rsv2 = rsv2 || null
    this.rsv3 = rsv3 || null
  }
}

/**
 * Parsing message
 */
const parsingMes = async (message) => {
  let result = await new Promise((resolve, reject) => parser.parseString(message.toString(), (err, result) => {
    if (err) reject(err)
    else resolve(result)
  }))
  return result
}

module.exports = {
  insertLogs,
  insertFinishedLog,
  checkFinishedLog,
  isInvalidDate,
  getChangeFlag,
  getCol,
  getColToObj,
  deleteKeyValue,
  deleteDataBySeq,
  asyncForEach,
  selectKeyValueBulk,
  batchUpsertOnSeq,
  Counter,
  Log,
  parsingMes,
  isInvalidTime,
}


const xml2js = require('xml2js')
const parser = new xml2js.Parser()
const systemDB = require('../../common/index.js')
const moment = require('moment')
const fs = require('fs')
const fse = require('fs-extra')
const path = require('path')
const unzipper = require('unzipper')
const _ = require('lodash')
const { DATAPROCESS_ZIP_DIR, DATAPROCESS_MESSAGE_DIR, pg: { pgSchema } } = require('../../config.js')
const { promisify } = require('util')
const readFile_promise = promisify(fs.readFile)
const { getValue: getExchangeRate } = require('../../kafka_modules/exchange_rate.js')
const { getValue: getImg } = require('../../kafka_modules/img.js')
const { getValue: getInfoRecord } = require('../../kafka_modules/info_record.js')
const { getPo } = require('../../kafka_modules/po.js')
// const { getMaterialDoc } = require('../../kafka_modules/sap_outbound_mm.js')
const { getMaterial } = require('../../kafka_modules/material_master.js')
const { insertLogs, Log, asyncForEach, insertFinishedLog, parsingMes } = require('../../common.js')
let squel = require('squel').useFlavour('postgres')
const UNZIP_OUTPUT_DIR = 'output'
const log4js = require('../logger/logger')
const logger = log4js.getLogger('import')

class rawdata {
  async importAll(ctx){
    try {
      let { date, topic } = ctx.params
      let tableName = null

      if (!date || !topic) {
        logger.error('arguments error')
        throw Error('arguments error')
      }

      if (topic == 'SAP_Outbound_MM') {
        tableName = getSapOutboundMMParam(ctx.query)
      }
      logger.debug('get params from api:', date, topic, tableName)
      let { endDate } = ctx.query
      if (!endDate) {
        endDate = moment().add(-1, 'days').format('YYYY-MM-DD')
      } else {
        endDate = moment(endDate, 'YYYY-MM-DD').format('YYYY-MM-DD')
      }
      
      // get log, 拿到error offset之後 要將 起始點改為 get到的 message 往下繼續
      // 這筆成功之後 要將 error log status 改回 Success
      let resultFromLog = await getErrorLog(topic, tableName, moment(date, 'YYYY-MM-DD').format('YYYY-MM-DD'))
      if (!_.isEmpty(resultFromLog)) {
        logger.info('have uncompleted record, continue from', date) // 未完成紀錄, 從上次錯誤繼續
        date = resultFromLog['start_date']
      }
      let formatDate = moment(date, 'YYYY-MM-DD').format('YYYY-MM-DD')

      let dates = getDates(new Date(formatDate), new Date(endDate))
      logger.info(`start to import from ${formatDate} to ${endDate}, total ${dates.length} days`)

      for (let i = 0; i < dates.length; i++) {
        await getMessageByTopic(dates[i], 'zip', topic, tableName, resultFromLog)
        await insertImportLogs(topic, topic, 'Success', null, dates[i], null, null, tableName, `${dates[i]}_DataProcess.zip`)
      }
  
      if (!ctx.query.endDate) {
        // 處理今天的Message
        let today = moment().format('YYYY-MM-DD')
        await getMessageByTopic(today, 'message', topic, tableName, null)
        await insertImportLogs(topic, topic, 'Success', null, today, null, null, tableName, `${today}_DataProcess`)
      }

      logger.info(`done. import from ${formatDate} to ${endDate}, total ${dates.length} days`)
      ctx.body = 'success'
      ctx.status = 200
    } catch(err) {
      logger.error('importSingle function have something wrong,', err)
      ctx.body = err.message
      ctx.status = 400
    }
  }

  async importSingle(ctx){
    let { date, topic, partition, offset, filename, type } = ctx.params
    let tableName = null, formatDate = null
    
    try {
      if (!date || !topic || !partition || !offset || !filename || !type) {
        logger.error('arguments error')
        throw Error('arguments error')
      }
      logger.debug('get params from api:', date, topic, partition, offset, filename, type)

      if (topic == 'SAP_Outbound_MM') {
        tableName = getSapOutboundMMParam(ctx.query)
      }
      
      let formatDate = moment(date, 'YYYY-MM-DD').format('YYYY-MM-DD')
      if (type == 'zip') {
        await unzipByDateTopic(formatDate, topic)
      }

      // check filename
      await checkFileExist(type, formatDate, topic, partition, filename)

      await findandImportRawData(type, formatDate, topic, topic, partition, filename, offset, tableName)
      logger.info(`done. import ${topic} files from ${formatDate} dir`)
      if (type == 'zip') {
        await removeUnZipMessage(formatDate)
      }
      ctx.body = 'success'
      ctx.status = 200
    } catch (err) {
      logger.error('importSingle function have something wrong, ', err)
      if (type == 'zip') {
        await removeUnZipMessage(formatDate)
      }
      ctx.body = 'Fail'
      ctx.status = 400
    }
  }
}

const getSapOutboundMMParam = (query) => {
  if (query && query.tableName) {
    return query.tableName
  } else {
    logger.error('arguments error')
    throw Error('arguments error')
  }
}

const memCheck = function() {
  let mem = process.memoryUsage()
  let format = function(bytes) {
    return (bytes / 1024 / 1024).toFixed(2) + 'MB'
  }
  console.log('Process: heapTotal ' + format(mem.heapTotal) + ' heapUsed ' + format(mem.heapUsed) + ' rss ' + format(mem.rss))
}

/**
 *
 * @param {string} type zip or message
 * @param {string} date 2019-06-10
 * @param {string} topic PO
 *
 * @returns {array} topic list [ 'wzs.sap.mm.po', 'wks.sap.mm.po', ...]
 */
const getTopicInDir = async (type, date, topic) => {
  let filePath = null
  if (type == 'zip') {
    filePath = `${DATAPROCESS_ZIP_DIR}/${date}`
  } else {
    filePath = `${DATAPROCESS_MESSAGE_DIR}/${date}`
  }
  
  if (!fs.existsSync(`${filePath}`)) {
    logger.info(`${filePath} is not exist`)
    return []
  }
  
  logger.debug('getMessageByTopic', filePath, type, filePath, topic)

  let filesInDir = await getDirFiles(path.resolve(filePath))
  let topicList = _.chain(filesInDir)
    .filter((m) => m.toLowerCase().includes(topic.toLowerCase()))
    .sort()
    .map(v => {
      if (type == 'zip') return v.split('_DataProcess.zip')[0]
      else return v
    })
    .value()

  logger.info(filePath, `${topicList.length} target to be process:`, topicList)

  return topicList
}

const getMessageByTopic = async function(date, type, topicName, tableName, logResult = null) {

  // get topioc name from date dir
  let topicList = await getTopicInDir(type, date, topicName)

  if (topicList.length > 0) {
    let topicFromLog, partitionFromLog, filenameFromLog
    if (type == 'zip' && logResult && !_.isEmpty(logResult)) {
      topicFromLog = logResult['topic']
      let idx = _.indexOf(topicList, topicFromLog)
      topicList = _.slice(topicList, idx)
    }

    await asyncForEach(topicList, async (topic) => {
      global.gc()

      let topicPath = null
      // unzip topic 資料夾
      if (type == 'zip') {
        await unzipByDateTopic(date, topic)
        topicPath = path.resolve(UNZIP_OUTPUT_DIR, date, topic)
      } else {
        topicPath = path.resolve(DATAPROCESS_MESSAGE_DIR, date, topic)
      }

      // 拿資料夾中的 有哪些partitions
      let partitions = await getDirFiles(topicPath)

      if (type == 'zip' && logResult && !_.isEmpty(logResult) && topic == topicFromLog) {
        partitionFromLog = logResult['partition']
        let idx = _.indexOf(partitions, partitionFromLog)
        partitions = _.slice(partitions, idx)
      }

      // partitions: [0, 1, 2]
      await asyncForEach(partitions, async (partition) => {
        let dirPath = path.resolve(topicPath, partition.toString())

        // 從partition資料夾中的 有哪些檔案
        let files = await getDirFiles(dirPath)
        let message = _.sortBy(files)

        message = message.map(mes => {
          let s = mes.split('-')
          if (s.length >= 3) {
            // filname : timestamp-offset-XMLfilename.XML
            return {
              offset: s[1],
              filename: s[2],
            }
          } else {
            logger.error(`filename format not right, ${topic}, ${partition}`, mes)
            throw Error(`filename format not right, ${topic}, ${partition}, ${mes}`)
          }
        })

        if (type == 'zip' && logResult && topic == topicFromLog && partition == partitionFromLog) {
          filenameFromLog = logResult['rsv2']
          let idx = _.indexOf(message.map(o => o.filename), filenameFromLog)
          message = _.slice(message, idx)
        }

        await asyncForEach(message, async (mes) => {
          try {
            await findandImportRawData(type, date, topicName, topic, partition, mes.filename, mes.offset, tableName)
            // update for Error log
            if (type == 'zip' && logResult && topic == topicFromLog && partition == partitionFromLog && mes.filename == filenameFromLog) {
              updateImportLog(logResult['uuid'])
            }
          } catch(err) {
            if (type == 'zip') {
              await removeUnZipMessage(date)
            }
            await insertImportLogs(topicName, topic, 'Error', err.message, date, partition, mes.offset, null, mes.filename)
            logger.error('get Message By Topic', type, date, topicName, topic, partition, mes.filename, mes.offset, tableName)
            throw Error(err)
          }
        })
      })
    })
    if (type == 'zip') {
      await removeUnZipMessage(date)
    }
  }
  return
}

const getDirFiles = async function(path) {
  return new Promise(function (resolve, reject) {
    fs.readdir(path, function (error, result) {
      if (error) {
        reject(error)
      } else {
        resolve(result)
      }
    })
  })
}

const getDirFilesByFiliename = async function(path, filename) {
  let files = await getDirFiles(path)
  return files.find(o => o.includes(filename))
}

const checkFileExist = async (type, date, topic, partition, filename) => {
  let targetPath = null
  if (type == 'zip') {
    targetPath = `${UNZIP_OUTPUT_DIR}/${date}/${topic}/${partition}`
  } else {
    targetPath = `${DATAPROCESS_MESSAGE_DIR}/${date}/${topic}/${partition}`
  }

  let fullfilename = await getDirFilesByFiliename(targetPath, filename)
  if (!fullfilename) {
    logger.error(`can not fiound filename, ${targetPath}, ${date}, ${topic}, ${partition}, ${filename}`)
    throw Error(`can not fiound filename,  ${targetPath}, ${date}, ${topic}, ${partition}, ${filename}`)
  }
  logger.debug('file exist, and parsing file from', targetPath, fullfilename)
  return {
    fullfilename,
    targetPath,
  }
}

// 處理file & 根據topic 去執行 kafka module
const findandImportRawData = async function(type, date, topicName, topic, partition, filename, offset, assign = null) {
 
  let { fullfilename, targetPath } = await checkFileExist(type, date, topic, partition, filename)
  let fPath = `${targetPath}/${fullfilename}`
  
  logger.debug(`find message ${fPath}`, `| ${date} ${topicName} `)
  let XMLfile = path.resolve(fPath)
  let origString = await readFile_promise(XMLfile, 'utf-8')
  
  // Parsing message
  let result = await parsingMes(origString)
  // let result = await new Promise((resolve, reject) => parser.parseString(origString.toString(), (err, result) => {
  //   if (err) reject(err)
  //   else resolve(result)
  // }))
  
  let xmlFileName = result.SAP_XML.CONTROL[0].FILE_NAME[0]
  let messageName = result.SAP_XML.CONTROL[0].MESSAGE_NAME
  let messageKey = result.SAP_XML.CONTROL[0].MESSAGE_KEY[0]
  let item = result.SAP_XML
  let logname = null

  logger.debug(`Parsing message ${xmlFileName}, ${messageName}, ${messageKey}`)
  if ((!assign) || ((assign != null) && (messageName == assign))) {
    try {
      if (messageName == 'EXCHRATE') {
        logname = 'exchange_rate'
        let value = item.EXCHRATE[0].EXCHRATE_ITEM

        logger.debug('exec getExchangeRate function', `topic: ${topicName} and messageName ${messageName} == 'EXCHRATE'`)
        await getExchangeRate(value, xmlFileName)
      
      } else if (messageName == 'INFORECORD') {
        logname = 'info_record'
        let value = item.EINA[0].EINA_ITEM

        logger.debug('exec getInfoRecord function', `topic: ${topicName} and messageName ${messageName} == 'INFORECORD'`)
        await getInfoRecord(value, xmlFileName)
      
      } else if (messageName == 'MatDoc') {
        logname = 'SAP_Outbound_MM_MatDoc'
        let value = item.MATDOC[0].MATDOC_ITEM

        logger.debug('exec getMaterialDoc function', `topic: ${topicName} and messageName ${messageName} == 'MatDoc'`)
        // await getMaterialDoc(value, xmlFileName)
      
      } else if (messageName == 'MatMaster') {
        logname = 'MatMaster'
        let value = item.MARA[0].MARA_ITEM

        logger.debug('exec getMaterial function', `topic: ${topicName} and messageName ${messageName} == 'MatMaster'`)
        await getMaterial(value, xmlFileName)
      
      } else if (messageName == 'Vendor_General') {
        logname = 'SAP_Outbound_MM_Vendor_General'
        let value = item.BASEDATA[0].BASEDATA_ITEM[0].TABLE[0].TABLE_ITEM

        logger.debug('exec getVendor function', `topic: ${topicName} and messageName ${messageName} == 'Vendor_General'`)
        // await getVendor(value, xmlFileName)
      
      } else if (messageName == 'PO') {
        logname = 'PO'
        let value = item.EKKO[0].EKKO_ITEM

        logger.debug('exec getPo function', `topic: ${topicName} and messageName ${messageName} == 'PO'`)
        await getPo(value, xmlFileName)
      } else if (['IMG_T001W', 'IMG_T024', 'IMG_T024W'].includes(messageKey)) {
        logname = 'img'
        let value = item.IMG[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM

        logger.debug('exec getImg function', `topic: ${topicName} and messageKey ${messageKey} include IMG_T001W, IMG_T024, IMG_T024W`)
        await getImg(value, messageKey, xmlFileName)
      } else {
        logger.debug('Pass: can not match topicName function')
        console.log('Pass: can not match topicName function')
        return
        // throw Error('Error: can not match topicName function')
      }
    } catch (err) {

      await insertImportLogs(topicName, topic, 'Error', err.message, date, partition, offset, assign, xmlFileName)
      logger.error('insertImportLogs: ', topicName, topic, date, partition, offset, assign, xmlFileName)
      throw Error('insertImportLogs: ', err)
    } finally {
      global.gc()
    }
    logger.debug('[Success]: insert message', topicName, topic, date, partition, offset, assign, xmlFileName)
    // await insertFinishedLog(key, partition, offset, xmlFileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
  }
  return
}

const unzipByDateTopic = async function(date, topic) {
  let zipFile = `${DATAPROCESS_ZIP_DIR}/${date}/${topic}_DataProcess.zip`
  let destPath = path.resolve(`${UNZIP_OUTPUT_DIR}//${date}`)
  logger.debug(`want to unzip ${topic} file in ${date} dir, and unzip from ${zipFile} to ${destPath}`)
  logger.info(`unzip file ${zipFile} to ${destPath}`)
  return new Promise(function(resolve, reject){
    fs.createReadStream(zipFile)
      .pipe(unzipper.Extract({ path: destPath }))
      .promise()
      .then(async () => {
        return resolve()
      }, e => reject(e))
  })
}

const removeUnZipMessage = async function(date) {
  logger.info(`Remove ${UNZIP_OUTPUT_DIR}/${date} directory When the import file is completed.`)
  
  return new Promise(function(resolve, reject){
    fse.remove(path.resolve(UNZIP_OUTPUT_DIR, date), (err) => {
      if (err) reject(err)
      else resolve()
    })
  })
}

const getDates = function(startDate, endDate) {
  let dates = [],
    currentDate = startDate,
    addDays = function(days) {
      let date = new Date(this.valueOf())
      date.setDate(date.getDate() + days)
      return date
    }

  while (currentDate <= endDate) {
    dates.push(moment(currentDate).format('YYYY-MM-DD'))
    currentDate = addDays.call(currentDate, 1)
  }
  return dates
}

const insertImportLogs = async function(logname, topic, status, errMessage, date, partition, offset, assign, xmlFileName) {
  let sql = squel.insert()
    .into(`${pgSchema}.logs_kafka_import`)
    
  sql.set('logtype', 'importRawdata')
    .set('logname', logname)
    .set('create_time', moment().utc().format())
    .set('update_time', moment().utc().format())
    .set('status', status)
    .set('topic', topic)
    .set('partition', partition)
    .set('"offset"', offset)
    .set('start_date', date)
    .set('rsv1', errMessage)
    .set('rsv2', xmlFileName)

  if (assign) {
    sql.set('rsv3', assign)
  }

  let res = await systemDB.Query(sql.toParam())
  return res
}

const updateImportLog = async function(uuid) {
  let sql = squel.update().table(`${pgSchema}.logs_kafka_import`)

  sql.set('status', 'Reimport Success')
    .set('update_time', moment().utc().format())
    .where('uuid = ?', uuid)

  let res = await systemDB.Query(sql.toParam()).then((res) => { 
    return true
  }).catch((err) => {
    logger.error('updateimportlog function have error', err)
    return false
  })
  return res
}

const getErrorLog = async function(topic, tableName, date) {
  
  let sql = squel.select()
    .distinct()
    .field('uuid')
    .field('topic')
    .field('partition')
    .field('"offset"')
    .field('rsv2')
    .field('start_date')
    .field('update_time')
    .from(`${pgSchema}.logs_kafka_import`, 'logs_kafka_import')
    .where('logtype = ? and logname = ? and status = ? and start_date >= ?', 'importRawdata', topic, 'Error', date)
    .order('"update_time"')
    .limit(1)

  if (tableName) {
    sql.where('rsv1 = ?', tableName)
  }

  const result = await systemDB.Query(sql.toParam())

  if (result.rowCount > 0) {
    return result.rows[0]
  } else {
    return {}
  }
}

module.exports = rawdata

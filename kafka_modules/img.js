/**
 * update mode
 */
const moment = require('moment')
const fs = require('fs')
const { kafkaHost, forceProcess, msgInterval, saveMessage, env } = require('../config.js')
const { fileSaver, mkdirSyncRecursive } = require('../utils/filesaver/filesave.js')
// const { sendNoti } = require('../utils/slack/notification')
const {
  getCol,
  deleteKeyValue,
  deleteDataBySeq,
  insertLogs,
  insertFinishedLog,
  checkFinishedLog,
  asyncForEach,
  Counter,
  Log,
  batchUpsertOnSeq,
  parsingMes,
} = require('../common.js')
const log4js = require('../server/logger/logger')
const logger = log4js.getLogger('img')
const kafkaconn = require('./kafka_connecter')
let consumer = null

// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0
const dbHelper = require('../common/db_wrapper_help')

const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

const sendEmailForNotice = async (errorMes, topic, partition, offset, xml) => {
  try {
    let info = {
      topic: `${topic}-${partition}-${xml}`,
      subject: 'fetch img failed',
      kafkaIp: `${kafkaHost}`,
      message: errorMes,
      offset,
    }

    await mail.sendmail(msg.sendTopicErrorMsg(info))
    logger.info('sent notice email success', moment().format('YYYY-MM-DD HH:mm:ss'))
  
  } catch (err) {
    logger.error('sendEmailForNotice: ', err)
  }
}

const dataHandler = async function (messageSet, topic, partition) {
  await asyncForEach(messageSet, async (m) => {
    // let result = await new Promise((resolve, reject) => parser.parseString(element.message.value.toString(), (err, result) => {
    //   if (err) reject(err)
    //   else resolve(result)
    // }))
    // await get_img(result)
    totalCounter++
    counterData[topic][partition].count++
    // counterData[topic][partition].offset = m.offset

    let result = await parsingMes(m.message.value)
    let xmlFileName = result.SAP_XML.CONTROL[0].FILE_NAME[0]
    let key = result.SAP_XML.CONTROL[0].MESSAGE_KEY[0]
    let messageName = result.SAP_XML.CONTROL[0].MESSAGE_NAME

    // 將收到的訊息存入fs
    if (saveMessage) {
      fileSaver(topic, partition, m.offset, xmlFileName, m.message.value)
    }

    let checkResult = await checkFinishedLog(topic, partition, m.offset, xmlFileName)
    if (checkResult.rowCount == 1 && !forceProcess) {
      logger.debug(`the file is already exist in DB, t:${topic}, P:${partition}, o:${m.offset}, f:${xmlFileName}`)
      // 檔案已經被處理過
      counterData[topic][partition].Pass++
    } else {

      let logname = 'img'
      if (['IMG_T001W', 'IMG_T024', 'IMG_T024W'].includes(key)) {
        try {
          dataLength = result.SAP_XML.IMG[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM.length
          await getValue(result.SAP_XML.IMG[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM, key, xmlFileName)
        } catch (e) {
          logger.error(`failed to parse xml: ${xmlFileName}`, e)
          counterData[topic][partition].Error++
          // await sendNoti(`(${env})failed to process xml: ${xmlFileName}, kafka: t:${topic}, p:${partition}, o:${m.offset}`)          
          await sendEmailForNotice(e, topic, partition, m.offset, xmlFileName)

          let { uuid: logID } = await insertLogs(new Log('kafka', logname, 'Error', null, e.message, `${topic}|${partition}|${m.offset}`))
          await consumer.unsubscribe(topic, partition)
          await consumer.end()
          throw Error(`can not parse xml and update database, ${topic}, ${partition}, ${m.offset}`, e)
        }
        // 完成處理, 寫入FinishedLogs
        counterData[topic][partition].Finished++
        await insertFinishedLog(topic, partition, m.offset, xmlFileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
      }
      else {
        // 不是我們要的資料, 寫入KafaLogs標記忽略
        counterData[topic][partition].Ignore++
        await insertLogs(new Log('kafka', logname, 'Ignore', xmlFileName, messageName, `${topic}|${partition}|${m.offset}`))
      }
    }
    let commitRes = await consumer.commitOffset(topic, partition, m.offset)
    logger.debug(`done with commit, t:${topic}, p:${partition}, o:${m.offset}, result:`, commitRes)
    counterData[topic][partition].offset = JSON.stringify(commitRes)
    return commitRes
  })
}

const img = async function () {
  let targets = [
    { topic: 'SAP_Outbound_Others', partition: [0, 1, 2] },
  ]

  logger.info('consumer init')
  consumer = await new kafkaconn(env)

  logger.info('get offset from kafka')
  let topic = await consumer.getStartOffset(targets)
  if (topic.length <= 0) {
    logger.error('getStartOffset somthing wrong')
    throw Error('getStartOffset somthing wrong')
  }

  // return consumer.init().then(async function () {
  topic.forEach(async function (element) {
    // start to subscribe kafka
    try {
      await consumer.subscribe(element.topic, element.partition, dataHandler, element.option)
      // init counter
      if (!counterData[element.topic]) counterData[element.topic] = {}
      counterData[element.topic][element.partition] = new Counter()
      // init fs directory
      if (saveMessage && !fs.existsSync('Message/')) {
        mkdirSyncRecursive('Message/')
      }
    } catch(err) {
      logger.error('img subscribe error', err, element.topic, element.partition, element.option)
      await consumer.unsubscribe(element.topic, element.partition)
      await consumer.end()
      sendEmailForNotice(err)
    }
  })
  if (msgInterval > 0) {
    setInterval(function () {
      console.log(new Date(), 'img')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, msgInterval);
  }
}

let table = {
  'IMG_T001W': {
    'tableName': 'wiprocurement.t001w',
    'key': ['MANDT', 'WERKS'],
    'col': ['MANDT', 'WERKS', 'NAME1', 'BWKEY', 'KUNNR', 'LIFNR', 'FABKL', 'NAME2', 'STRAS', 'PFACH',
      'PSTLZ', 'ORT01', 'EKORG', 'VKORG', 'CHAZV', 'KKOWK', 'KORDB', 'BEDPL', 'LAND1', 'REGIO',
      'COUNC', 'CITYC', 'ADRNR', 'IWERK', 'TXJCD', 'VTWEG', 'SPART', 'SPRAS', 'WKSOP', 'AWSLS',
      'CHAZV_OLD', 'VLFKZ', 'BZIRK', 'ZONE1', 'TAXIW', 'BZQHL', 'LET01', 'LET02', 'LET03', 'TXNAM_MA1',
      'TXNAM_MA2', 'TXNAM_MA3', 'BETOL', 'J_1BBRANCH', 'VTBFI', 'FPRFW', 'ACHVM', 'DVSART', 'NODETYPE', 'NSCHEMA',
      'PKOSA', 'MISCH', 'MGVUPD', 'VSTEL', 'MGVLAUPD', 'MGVLAREVAL', 'SOURCING', 'OILIVAL', 'OIHVTYPE', 'OIHCREDIPI',
      'STORETYPE', 'DEP_STORE'],
  },
  'IMG_T024': {
    'tableName': 'wiprocurement.t024',
    'key': ['MANDT', 'EKGRP'],
    'col': ['MANDT', 'EKGRP', 'EKNAM', 'EKTEL', 'LDEST', 'TELFX', 'TEL_NUMBER', 'TEL_EXTENS', 'SMTP_ADDR'],
  },
  'IMG_T024W': {
    'tableName': 'wiprocurement.t024w',
    'key': ['MANDT', 'WERKS', 'EKORG'],
    'col': ['MANDT', 'WERKS', 'EKORG'],
  },
}

const getValue = dbHelper.atomic(async (client, data, message_key, fileName)=> {
  if (Array.isArray(data) && data.length > 0) {
    let img_items = []

    await asyncForEach(data, async (d, idx) => {
      let { TIMESTAMP, TRKORR, ZMODE, ...newObject } = d
      // IMG
      if (newObject['MANDT'] == '888') {

        let parseObj = getCol(newObject, table[message_key].col)
        if (ZMODE == 'U') {
          img_items.push(parseObj)

        } else if (ZMODE == 'D') {
          if (img_items.length > 0) {
            // 在delete 之前 先把 array 中的資料 commit
            await batchUpsertOnSeq(table[message_key].tableName, table[message_key].key, img_items, fileName, client)
            img_items = []
          }
          // await deleteKeyValue(table[message_key].tableName, table[message_key].key, parseObj, client)
          await deleteDataBySeq(table[message_key].tableName, table[message_key].key, parseObj, fileName, client)
          
        } else {
          logger.warn(`Unknow mode(ZMODE): ${ZMODE}`)
        }
      } else {
        logger.info(`ignore client (MANDT): ${newObject['MANDT']}`)
      }
    })

    if (img_items.length > 0) {
      await batchUpsertOnSeq(table[message_key].tableName, table[message_key].key, img_items, fileName, client)
    }
  }
})

module.exports = {
  img,
  getValue,
}

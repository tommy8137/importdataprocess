const moment = require('moment')
const fs = require('fs')
const { kafkaHost, forceProcess, msgInterval, saveMessage, env } = require('../config.js')
const { fileSaver, mkdirSyncRecursive } = require('../utils/filesaver/filesave.js')
const systemDB = require('../common/index.js')
const {
  isInvalidDate,
  getCol,
  insertLogs,
  asyncForEach,
  batchUpsertOnSeq,
  deleteKeyValue,
  deleteDataBySeq,
  insertFinishedLog,
  checkFinishedLog,
  Counter,
  Log,
  parsingMes,
} = require('../common.js')
// const { sendNoti } = require('../utils/slack/notification')
const log4js = require('../server/logger/logger')
const logger = log4js.getLogger('info_record')
const kafkaconn = require('./kafka_connecter')
let consumer = null

const dbHelper = require('../common/db_wrapper_help')
// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0

const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

const sendEmailForNotice = async (errorMes, topic, partition, offset, xml) => {
  try {
    let info = {
      topic: `${topic}-${partition}-${xml}`,
      subject: 'fetch info_record failed',
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

// data handler function can return a Promise
const dataHandler = async function (messageSet, topic, partition) {

  await asyncForEach(messageSet, async (m) => {
    // ----
    // let result = await new Promise((resolve, reject) => parser.parseString(m.message.value.toString(), (err, result) => {
    //   if (err) reject(err)
    //   else resolve(result)
    // }))
    // await get_info_record(result)
    // ----

    totalCounter++
    counterData[topic][partition].count++
    // counterData[topic][partition].offset = m.offset
    let result = await parsingMes(m.message.value)
    let xmlFileName = result.SAP_XML.CONTROL[0].FILE_NAME[0]

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

      if (result.SAP_XML.CONTROL[0].TIMESTAMP[0] >= '201811190000000000') {
        let messageName = result.SAP_XML.CONTROL[0].MESSAGE_NAME
        let logname = 'info_record'
        if (result.SAP_XML.CONTROL[0].MESSAGE_NAME == 'INFORECORD') {

          try {
            dataLength = result.SAP_XML.EINA[0].EINA_ITEM.length
            logger.debug(`start parsing file: ${xmlFileName}`)
            await getValue(result.SAP_XML.EINA[0].EINA_ITEM, xmlFileName)
            logger.debug(`done parsing file: ${xmlFileName}`)
          } catch (e) {
            logger.error(`can not parse xml and update database`, e)
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
          logger.debug(`write the record into db, ${topic},${partition},${m.offset},${xmlFileName}`)

        } else {
          // 不是我們要的資料, 寫入KafaLogs標記忽略
          counterData[topic][partition].Ignore++
          await insertLogs(new Log('kafka', logname, 'Ignore', xmlFileName, messageName, `${topic}|${partition}|${m.offset}`))
        }

        // logger.debug(`the record write into ${topic},${partition},${m.offset},${xmlFileName}`)

      } else {
        logger.warn(`data timestamp is too old, ${result.SAP_XML.CONTROL[0].TIMESTAMP[0]}, skip`)
      }
    }
    let commitRes = await consumer.commitOffset(topic, partition, m.offset)
    logger.debug(`done with commit, t:${topic}, p:${partition}, o:${m.offset}, result:`, commitRes)
    counterData[topic][partition].offset = JSON.stringify(commitRes)
    return commitRes
  })
}

const info_record = async function () {

  let targets = [
    { topic: 'wzs.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wks.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wtz.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wcz.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wmx.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wih.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wmt.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wcq.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wmcq.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'wcd.sap.mm.info_record', partition: [0, 1, 2] },
    // add at 2019/06/04
    { topic: 'whq.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'aiih.sap.mm.info_record', partition: [0, 1, 2] },
    { topic: 'witx.sap.mm.info_record', partition: [0, 1, 2] },
  ]

  logger.info('consumer init')
  consumer = await new kafkaconn(env)

  logger.info('get offset from kafka')
  let topic = await consumer.getStartOffset(targets)
  if (topic.length <= 0) {
    logger.error('getStartOffset somthing wrong')
    throw Error('getStartOffset somthing wrong')
  }


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
    } catch (err) {
      logger.error('info_record subscribe error', err, element.topic, element.partition, element.option)
      await consumer.unsubscribe(element.topic, element.partition)
      await consumer.end()
      sendEmailForNotice(err)
    }
  })

  if (msgInterval > 0) {
    setInterval(function () {
      console.log(new Date(), 'info_record')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, msgInterval);
  }
}


const getValue = dbHelper.atomic(async (client, data, fileName) => {

  logger.debug(`processing fileName: ${fileName}`)
  let colList = {
    EINA: ['ZCRTDAT', 'ZCRTTIM', 'MANDT', 'INFNR', 'MATNR', 'MATKL', 'LIFNR', 'LOEKZ', 'ERDAT', 'ERNAM',
      'TXZ01', 'SORTL', 'MEINS', 'UMREZ', 'UMREN', 'IDNLF', 'VERKF', 'TELF1', 'MAHN1', 'MAHN2',
      'MAHN3', 'URZNR', 'URZDT', 'URZLA', 'URZTP', 'URZZT', 'LMEIN', 'REGIO', 'VABME', 'LTSNR',
      'LTSSF', 'WGLIF', 'RUECK', 'LIFAB', 'LIFBI', 'KOLIF', 'ANZPU', 'PUNEI', 'RELIF', 'MFRNR',
      'BMATN', 'MFRPN'],
    EINE: ['MANDT', 'INFNR', 'EKORG', 'ESOKZ', 'WERKS', 'LOEKZ', 'ERDAT', 'ERNAM', 'EKGRP', 'WAERS',
      'BONUS', 'MGBON', 'MINBM', 'NORBM', 'APLFZ', 'UEBTO', 'UEBTK', 'UNTTO', 'ANGNR', 'ANGDT',
      'ANFNR', 'ANFPS', 'ABSKZ', 'AMODV', 'AMODB', 'AMOBM', 'AMOBW', 'AMOAM', 'AMOAW', 'AMORS',
      'BSTYP', 'EBELN', 'EBELP', 'DATLB', 'NETPR', 'PEINH', 'BPRME', 'PRDAT', 'BPUMZ', 'BPUMN',
      'MTXNO', 'WEBRE', 'EFFPR', 'EKKOL', 'SKTOF', 'KZABS', 'MWSKZ', 'BWTAR', 'EBONU', 'EVERS',
      'EXPRF', 'BSTAE', 'MEPRF', 'INCO1', 'INCO2', 'XERSN', 'EBON2', 'EBON3', 'EBONF', 'MHDRZ',
      'VERID', 'BSTMA', 'RDPRF', 'MEGRU', 'J_1BNBM', 'SPE_CRE_REF_DOC', 'IPRKZ', 'CO_ORDER', 'VENDOR_RMA_REQ', 'DIFF_INVOICE',
      'MRPIND', 'SGT_SSREL', 'TRANSPORT_CHAIN', 'STAGING_TIME'],
    A018: ['MANDT', 'KAPPL', 'KSCHL', 'LIFNR', 'MATNR', 'EKORG', 'ESOKZ', 'DATBI', 'DATAB', 'KNUMH', 'KOPOS',
      'KBETR', 'KONWA', 'KPEIN', 'KMEIN', 'KUMZA', 'KUMNE', 'MEINS', 'LOEVM_KO'],
  }

  if (Array.isArray(data) && data.length > 0) {
    let eina_items = [], eine_items = [], a018_items = []
    await asyncForEach(data, async (obj) => {

      if (obj['MANDT'] == '888') {
        let { EINA_EINE, ...newObject } = obj
        let parseEinaObj = getCol(newObject, colList.EINA)

        // EINA_EINE
        if (isInvalidDate(parseEinaObj['ZCRTDAT'])) parseEinaObj['ZCRTDAT'] = ['']
        if (isInvalidDate(parseEinaObj['ERDAT'])) parseEinaObj['ERDAT'] = ['']
        if (isInvalidDate(parseEinaObj['URZDT'])) parseEinaObj['URZDT'] = ['']
        if (isInvalidDate(parseEinaObj['LIFBI'])) parseEinaObj['LIFBI'] = ['']
        if (isInvalidDate(parseEinaObj['LIFAB'])) parseEinaObj['LIFAB'] = ['']

        if (parseEinaObj['LOEKZ'] == 'X') {
          // 在update 之前 先把 array 中的資料 commit
          if (eina_items.length > 0) {
            await batchUpsertOnSeq('wiprocurement.eina', ['MANDT', 'INFNR'], eina_items, fileName, client)
            eina_items = []
          }
          await updateFlaggedForDel('wiprocurement.eina', ['MANDT', 'INFNR'], parseEinaObj, 'LOEKZ', fileName, client)
        } else {
          eina_items.push(parseEinaObj)
          // 改用Bulk commit
          // // await selectKeyValue('wiprocurement.eina', ['MANDT', 'INFNR'], parseEinaObj, fileName)
        }

        // EINA_EINE_ITEM
        let EINA_EINE_ITEM = EINA_EINE[0].EINA_EINE_ITEM

        if (Array.isArray(EINA_EINE_ITEM) && EINA_EINE_ITEM.length > 0 && EINA_EINE_ITEM[0] != '') {
          await asyncForEach(EINA_EINE_ITEM, async function (eine_item) {
            let { A018_KONP, ...eine } = eine_item
            let parseEineObj = getCol(eine, colList.EINE)

            if (parseEineObj['ESOKZ'] == '0' || parseEineObj['ESOKZ'] == '3') {
              if (isInvalidDate(parseEineObj['ERDAT'])) parseEineObj['ERDAT'] = ['']
              if (isInvalidDate(parseEineObj['ANGDT'])) parseEineObj['ANGDT'] = ['']
              if (isInvalidDate(parseEineObj['AMODV'])) parseEineObj['AMODV'] = ['']
              if (isInvalidDate(parseEineObj['AMODB'])) parseEineObj['AMODB'] = ['']
              if (isInvalidDate(parseEineObj['DATLB'])) parseEineObj['DATLB'] = ['']
              if (isInvalidDate(parseEineObj['PRDAT'])) parseEineObj['PRDAT'] = ['']

              if (parseEineObj['LOEKZ'] == 'X') {
                // 在update 之前 先把 array 中的資料 commit
                if (eine_items.length > 0) {
                  await batchUpsertOnSeq('wiprocurement.eine', ['MANDT', 'INFNR', 'EKORG', 'ESOKZ'], eine_items, fileName, client)
                  eine_items = []
                }
                await updateFlaggedForDel('wiprocurement.eine', ['MANDT', 'INFNR', 'EKORG', 'ESOKZ'], parseEineObj, 'LOEKZ', fileName, client)
              } else {
                eine_items.push(parseEineObj)
                // 改用Bulk commit
                // await selectKeyValue('wiprocurement.eine', ['MANDT', 'INFNR', 'EKORG', 'ESOKZ'], parseEineObj, fileName)
              }
            }
            // A018_KONP
            if (Array.isArray(A018_KONP) && A018_KONP.length > 0 && A018_KONP[0] != '') {
              let a018_item = A018_KONP[0].A018_KONP_ITEM
              // 先就第一筆刪除整批資料 (LIFNR, MATNR, EKORG, ESOKZ)
              // await deleteKeyValue('wiprocurement.a018_konp', ['LIFNR', 'MATNR', 'EKORG', 'ESOKZ'], a018_item[0], client)
              let deletedRows = await deleteDataBySeq('wiprocurement.a018_konp', ['LIFNR', 'MATNR', 'EKORG', 'ESOKZ'], a018_item[0], fileName, client)
              let shouldInsert = false

              // 在 delection 會做update_by 與filename的判斷, 判斷是否為新的一筆資料
              // 沒有刪除資料的原因是 1. 沒有此筆資料 2. update_by 的檔案 如果比 這次要insert的檔案較大 所以不應該再被舊的資料做更改
              if (deletedRows.rows[0].count == 0) {
                let findRes = await findDataBySeq('wiprocurement.a018_konp', ['LIFNR', 'MATNR', 'EKORG', 'ESOKZ'], a018_item[0], client)
                // findRes == false, 資料庫沒有這筆資料, 需要做insert
                if (!findRes) {
                  shouldInsert = true
                }
              } else {
                shouldInsert = true
              }

              // let items = []
              if (shouldInsert) {
                a018_item.forEach(item => {
                  let parseA018Obj = getCol(item, colList.A018)
                  if (item['ESOKZ'] == '0' || item['ESOKZ'] == '3') {  // 0:Standard 一般採購, 3:Subcontracting 外包, 這兩種分類才需要
                    // 改用Bulk commit
                    a018_items.push(parseA018Obj)
                    // items.push(parseA018Obj)
                  }
                })
              }
              // 然後整批資料寫入(同一批不同時間區間的採購價)
              // await batchUpsertOnSeq('wiprocurement.a018_konp', ['MANDT', 'KAPPL', 'KSCHL', 'LIFNR', 'MATNR', 'EKORG', 'ESOKZ', 'DATBI', 'DATAB', 'KNUMH', 'KOPOS'], items, fileName)
            }
          })
        }
      }
    })
    // 將items的資料送到 db 去insert
    if (eina_items.length > 0) {
      await batchUpsertOnSeq('wiprocurement.eina', ['MANDT', 'INFNR'], eina_items, fileName, client)
    }
    if (eine_items.length > 0) {
      await batchUpsertOnSeq('wiprocurement.eine', ['MANDT', 'INFNR', 'EKORG', 'ESOKZ'], eine_items, fileName, client)
    }
    if (a018_items.length > 0) {
      await batchUpsertOnSeq('wiprocurement.a018_konp', ['MANDT', 'KAPPL', 'KSCHL', 'LIFNR', 'MATNR', 'EKORG', 'ESOKZ', 'DATBI', 'DATAB', 'KNUMH', 'KOPOS'], a018_items, fileName, client)
    }
  }
})

/**
 * 檢查資料庫是否有 這筆資料, 如果有就 代表資料庫的資料是新的 如果沒有就 必須要insert
 * @param {*} tableName wiprocurement.a018_konp
 * @param {*} key a018 檢查是否有能被刪除的key
 * @param {*} value 資料
 * @param {*} client client
 *
 * @returns {boolean} true: 有找到資料
 */
async function findDataBySeq(tableName, key, value, client = null) {
  let flag = false
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
      let result = null
      let sql = `select count(*) from ${tableName} where ${expression}`

      if (client) {
        result = await client.query(sql)
      } else {
        result = await systemDB.Query(sql)
      }

      if(result.rows[0].count > 0) {
        return true
      } else {
        return false
      }
    } catch (e) {
      throw Error(e, 'update data error')
    }
  }
}

/**
 * 
 * @param {String} tableName 修改的目標Table
 * @param {Array} key 目標Table的PK
 * @param {Object} value 更新的整份data
 * @param {String} flagKey 目標的key值
 * @param {Object} client connection物件
 */
async function updateFlaggedForDel(tableName, key, value, flagKey, filename, client = null) {
  let flag = false
  filename = filename.replace('.XML', '').substr(0, 30)
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
      if (client) {
        await client.query(`update ${tableName} set ${flagKey}='O' where ${expression} and ('${filename}' > update_by or update_by ='init' or update_by is null)`)
      } else {
        await systemDB.Query(`update ${tableName} set ${flagKey}='O' where ${expression} and ('${filename}' > update_by or update_by ='init' or update_by is null)`)
      }
    } catch (e) {
      throw Error(e, 'update data error')
    }
  }
}

module.exports = {
  info_record,
  getValue,
}

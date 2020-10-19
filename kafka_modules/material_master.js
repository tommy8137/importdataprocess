const moment = require('moment')
// const _ = require('lodash')
const fs = require('fs')

const config = require('../config.js')
const { fileSaver } = require('../utils/filesaver/filesave.js')
// const { sendNoti } = require('../utils/slack/notification')
const commonFunc = require('../common.js')
const dbHelper = require('../common/db_wrapper_help')
const kafkaconn = require('./kafkaJS_connecter')

const log4js = require('../server/logger/logger')
const logger = log4js.getLogger('MatMaster')
const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

let kafkajs = null
const topic = config.kafka.topic.mat_master

// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0

const sendEmailForNotice = async (errorMes, offset) => {
  try {
    let info = {
      topic: topic,
      subject: 'fetch mat_master failed',
      kafkaIp: `${config.kafka.kafkaHost}:${config.kafka.kafkaPort}`,
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
const dataHandler = async (topic, partition, offset, messageSet) => {
  
  totalCounter++
  counterData[topic][partition].count++

  let logname = 'mat_master'

  let fileName = messageSet.CONTROL.FILE_NAME
  // console.log('fileName', fileName)

  // 將收到的訊息存入fs
  if (config.saveMessage) {
    fileSaver(topic, partition, offset, fileName, messageSet)
  }

  let checkResult = await commonFunc.checkFinishedLog(topic, partition, offset, fileName)
  if (checkResult.rowCount == 1 && !config.forceProcess) {
    logger.debug(`the file is already exist in DB, t:${topic}, P:${partition}, o:${offset}, f:${fileName}`)
    // 檔案已經被處理過
    counterData[topic][partition].Pass++
  } else {
    try {
      await getMaterial(messageSet.MARA.MARA_ITEM, fileName)
          
    } catch(e) {
      counterData[topic][partition].Error++
      // await sendNoti(`(${config.env})failed to process xml: ${fileName}, kafka: t:${topic}, p:${partition}, o:${offset}`)
      await sendEmailForNotice(e, offset)
      let { uuid: logID } = await commonFunc.insertLogs(new commonFunc.Log('kafka', logname, 'Error', null, e.message, `${topic}|${partition}|${offset}`))

      logger.error('mat_master dataHandler function error', offset, e)
      throw Error(`can not parse data and update to database, ${offset}`, e)
    }
    counterData[topic][partition].Finished++
    await commonFunc.insertFinishedLog(topic, partition, offset, fileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
    logger.debug(`write the record into db, ${topic},${partition},${offset},${fileName}`)
  }
  logger.debug(`done with commit, t:${topic}, p:${partition}, o:${offset}`)
}

const MatMaster = async function(){
  kafkajs = await new kafkaconn(`mat_master-${config.env}`)

  const run = async () => {
    let fromBeginning = true
    let autoCommit = false

    if (config.saveMessage && !fs.existsSync('Message/')) {
      commonFunc.mkdirSyncRecursive('Message/')
    }

    await kafkajs.connect()
    await kafkajs.subscribe(topic, fromBeginning)

    await kafkajs.kconsumer.run({
      autoCommit: autoCommit,
      eachMessage: async ({ topic, partition, message }) => {
        // init counter
        if (!counterData[topic]) {
          counterData[topic] = {}
          counterData[topic][partition] = new commonFunc.Counter()
        }

        try {
          const decodedValue = await kafkajs.registry.decode(message.value)
          // console.log(topic, partition, message.offset, decodedValue)
          await dataHandler(topic, partition, message.offset, decodedValue)

        } catch(error) {
          logger.error('consumer run topic error, offset: ', message.offset, error)
          kafkajs.stop()
          kafkajs.disconnect()
          throw Error(error)
        }
      },
    })
  }
  
  run().catch(e => {
    console.error(`!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!`)
    console.error(`[consumer] ${e.message}`, e)
    sendEmailForNotice(e)
  })

  if (config.msgInterval > 0) {
    setInterval(function () {
      console.log(new Date(), 'mat_master')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, config.msgInterval);
  }
}

const getMaterial = dbHelper.atomic(async (client, dataSet, fileName)=> {

  if (Array.isArray(dataSet) && dataSet.length > 0) {
    let colList = {
      mara: ['ZCRTDAT', 'ZCRTTIM', 'DATACFLG', 'MARCCFLG', 'MVKECFLG', 'MATNR', 'MAKTX', 'MTART', 'MATKL', 'ZZCATE',
        'ZZMOD', 'ZZGFTO', 'ZZCPJN', 'ZZCGRP', 'SPART', 'LVORM', 'ZZGLDE', 'NORMT', 'ZZCMDN', 'ZZLPNO',
        'ZZMNCO', 'ZZMJCO', 'ZZPDID', 'MEINS', 'ZZAGIT', 'ZZEQPN', 'ZZCBDN', 'ZZPDCA', 'ZZGLISE', 'ZZSERI',
        'ZZAPNO', 'ZZSPRD', 'MPROF', 'ZZCONFIG1', 'ZZCONFIG2', 'ZZCOLOR1', 'ZZCOLOR2', 'ZZATYPE', 'ZZAREGION', 'ZZPACKTYPE',
        'ZZREVI', 'ZZCRIS', 'ZZEEME', 'ZZCONTYP1', 'ZZCONTYP2', 'ZZUNIQUE', 'DISST'],
      marc: ['DATACFLG', 'MDMACFLG', 'MARDCFLG', 'MATNR', 'WERKS', 'RGEKZ', 'ZZCIFK', 'BESKZ', 'SOBSL', 'STRGR',
        'ZZBSAR', 'DZEIT', 'PLIFZ', 'DISPO', 'EKGRP', 'PRCTR', 'MTVFP', 'ZZACSN', 'LVORM', 'ZZRMA',
        'ZZCTOG', 'ZZREVL', 'ZZMFG', 'ZZFCC', 'ZZLIFS', 'ZZCKD', 'ZZALC', 'ZZPPID', 'ZZTECO', 'ZZORCO',
        'NCOST', 'ZZHTC', 'SCHGT', 'ZZMTFK', 'LGFSB', 'MMSTA', 'ZZBDFLG', 'ZZCTBA', 'ZZDISP', 'ZZSERMODE',
        'LGPRO', 'ZZCCNO', 'DISLS', 'BSTMI', 'BSTMA', 'AUSSS', 'WEBAZ', 'EISBE', 'SHFLG', 'SHZET',
        'VRMOD', 'VINT1', 'VINT2', 'STDPD', 'KAUSF', 'AHDIS'],
    }
    let mara_items = [], marc_items = []
    await commonFunc.asyncForEach(dataSet, async (data) => {
      let { MARA_MARC, MARA_MVKE, ...newObject } = data
      // MARA
      let parseMaraObj = commonFunc.getCol(newObject, colList.mara)
      if (commonFunc.getChangeFlag(parseMaraObj, 'DATACFLG')) {
        
        mara_items.push(parseMaraObj)
        // 改用Bulk commit
        // // await selectKeyValue('wiprocurement.mara', ['MATNR'], parseMaraObj, fileName)
      }

      if (MARA_MARC) {
        let MARA_MARC_ITEM = MARA_MARC.MARA_MARC_ITEM

        // MARC
        if (Array.isArray(MARA_MARC_ITEM) && MARA_MARC_ITEM.length > 0) {
          await commonFunc.asyncForEach(MARA_MARC_ITEM, async function (item) {
            let { MARA_MARC_MDMA, MARA_MARC_MARD, ...newMARC } = item
            let parseMarcObj = commonFunc.getCol(newMARC, colList.marc)
  
            if (commonFunc.getChangeFlag(parseMarcObj, 'DATACFLG')) {
              marc_items.push(parseMarcObj)
              // 改用Bulk commit
              // await selectKeyValue('wiprocurement.marc', ['MATNR', 'WERKS'], parseMarcObj, fileName)
            }
          })
        }
      }
    })

    if (mara_items.length > 0) {
      await commonFunc.batchUpsertOnSeq('wiprocurement.mara', ['MATNR'], mara_items, fileName, client)
    }
    if (marc_items.length > 0) {
      await commonFunc.batchUpsertOnSeq('wiprocurement.marc', ['MATNR', 'WERKS'], marc_items, fileName, client)
    }
  }
})

module.exports = {
  MatMaster,
  getMaterial,
}

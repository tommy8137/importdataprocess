const moment = require('moment')
const _ = require('lodash')
const fs = require('fs')

const config = require('../config.js')
const { fileSaver } = require('../utils/filesaver/filesave.js')
// const { sendNoti } = require('../utils/slack/notification')
const commonFunc = require('../common.js')
const dbHelper = require('../common/db_wrapper_help')
const kafkaconn = require('./kafkaJS_connecter')

const log4js = require('../server/logger/logger')
const logger = log4js.getLogger('plm')
const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

let kafkajs = null
const topic = config.kafka.topic.alt

// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0

const sendEmailForNotice = async (errorMes, offset) => {
  try {
    let info = {
      topic: topic,
      subject: 'fetch sapalt failed',
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

const dataHandler = async (topic, partition, offset, messageSet) => {
  
  totalCounter++
  counterData[topic][partition].count++

  let logname = 'plm_sapalt'

  // key: RUNSERIAL, MFGNUM, ItemNum, AltNum
  let fileName = `${offset}-${messageSet.RUNSERIAL}_${messageSet.MFGNUM}_${messageSet.ITEMNUM}_${messageSet.ALTNUM}`

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
      await getPlm(messageSet, fileName)
    } catch(e) {
      counterData[topic][partition].Error++
      // await sendNoti(`(${config.env})failed to process xml: ${fileName}, kafka: t:${topic}, p:${partition}, o:${offset}`)
      await sendEmailForNotice(e, offset)
      let { uuid: logID } = await commonFunc.insertLogs(new commonFunc.Log('kafka', logname, 'Error', null, e.message, `${topic}|${partition}|${offset}`))

      logger.error('plm alt dataHandler functino error', offset, e)
      throw Error(`can not parse data and update to database, ${offset}`, e)
    }
    counterData[topic][partition].Finished++
    await commonFunc.insertFinishedLog(topic, partition, offset, fileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
    logger.debug(`write the record into db, ${topic},${partition},${offset},${fileName}`)
  }
  logger.debug(`done with commit, t:${topic}, p:${partition}, o:${offset}`)
}

const plm = async function () {
  kafkajs = await new kafkaconn(`plm-${config.env}`)

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
          await dataHandler(topic, partition, message.offset, decodedValue)

        } catch(error) {
          logger.error('consumer run topic error, offset: ', message.offset, error)
          kafkajs.stop()
          kafkajs.disconnect()
          throw error
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
      console.log(new Date(), 'plm_sapalt')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, config.msgInterval);
  }
}

const getPlm = dbHelper.atomic(async (client, data, fileName)=> {
  try {
  // key: MfgNum, ItemNum, AltNum
    let alt_item = sortOutValue(data)
    // insetData
    if (alt_item.length > 0) {
      await commonFunc.batchUpsertOnSeq('wiprocurement.sapalt', ['runserial', 'mfgnum', 'itemnum', 'altnum'], alt_item, fileName, client)
    }
  } catch(error) {
    logger.error('getPlm function insert to db error', error)
    throw error
  }
})


const sortOutValue = (data) => {
  let obj = {}
  if (!_.isEmpty(data) && !_.isNull(data['RUNSERIAL']) && !_.isNull(data['ITEMNUM']) && !_.isNull(data['MFGNUM']) && !_.isNull(data['ALTNUM'])) {
    obj.sync_id = data['SYNC_ID']
    obj.sync_op = data['SYNC_OP']
    obj.sync_ts = data['SYNC_TS']
    obj.runserial = data['RUNSERIAL']
    obj.ecnno = data['ECNNO']
    obj.bomchangetype = data['BOMCHANGETYPE']
    obj.mfgnum = data['MFGNUM']
    obj.itemnum = data['ITEMNUM']
    obj.position = data['POSITION']
    obj.altnum = data['ALTNUM']
    obj.expirydate = data['EXPIRYDATE'] ? moment(data['EXPIRYDATE']).format() : null
    obj.saptranstatus = data['SAPTRANSTATUS']
    obj.transtatus = data['TRANSTATUS']
    obj.fgpart = data['FGPART'] ? data['FGPART'] : null
    obj.fgrev = data['FGREV']
    obj.fgseq = data['FGSEQ']
    obj.creator = data['CREATOR']
    obj.lastupdate = data['LASTUPDATE'] ? moment(data['LASTUPDATE']).format() : null
    obj.lastmodifier = data['LASTMODIFIER']
    obj.changeno = data['CHANGENO']
    return [obj]
  } else {
    logger.error('get alt data, key: RUNSERIAL, ITEMNUM, MFGNUM or ALTNUM is null')
    throw Error('get alt data, key: RUNSERIAL, ITEMNUM, MFGNUM or ALTNUM is null')
  }
}

module.exports = {
  plm,
}
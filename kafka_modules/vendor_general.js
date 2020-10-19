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
const logger = log4js.getLogger('vendor_general')
const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

let kafkajs = null
const topic = config.kafka.topic.vendor

// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0

const sendEmailForNotice = async (errorMes, offset) => {
  try {
    let info = {
      topic: topic,
      subject: 'fetch vendor_general failed',
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

  let logname = 'vendor_general'

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
      await getVendor(messageSet.BASEDATA.BASEDATA_ITEM.TABLE.TABLE_ITEM, fileName)

    } catch(e) {
      counterData[topic][partition].Error++
      // await sendNoti(`(${config.env})failed to process xml: ${fileName}, kafka: t:${topic}, p:${partition}, o:${offset}`)
      await sendEmailForNotice(e, offset)
      let { uuid: logID } = await commonFunc.insertLogs(new commonFunc.Log('kafka', logname, 'Error', null, e.message, `${topic}|${partition}|${offset}`))

      logger.error('vendor general dataHandler function error', offset, e)
      throw Error(`can not parse data and update to database, ${offset}`, e)
    }
    counterData[topic][partition].Finished++
    await commonFunc.insertFinishedLog(topic, partition, offset, fileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
    logger.debug(`write the record into db, ${topic},${partition},${offset},${fileName}`)
  }
  logger.debug(`done with commit, t:${topic}, p:${partition}, o:${offset}`)
}

const vendor = async function () {
  kafkajs = await new kafkaconn(`vendor-${config.env}`)

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
      console.log(new Date(), 'vendor_general')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, config.msgInterval);
  }
}

const getVendor = dbHelper.atomic(async (client, dataSet, fileName)=> {
  try {
    if (Array.isArray(dataSet) && dataSet.length > 0)  {
      let colList = ['TIMESTAMP', 'ZMODE', 'MANDT', 'LIFNR', 'TELF1', 'REMARK', 'TELFX', 'STCEG', 'KTOKK', 'LOEVM',
        'LAND1', 'ORT01', 'NAME1', 'NAME2', 'NAME3', 'NAME4', 'POST_CODE1', 'HOUSE_NUM1', 'STREET', 'STR_SUPPL1', 'STR_SUPPL2', 'STR_SUPPL3',
        'ERDAT', 'ERNAM', 'KUNNR', 'SPERR', 'SPERM', 'UPDAT', 'UPTIM', 'NODEL', 'SORT1', 'SORT2',
        'VBUND', 'SMTP_ADDR', 'ADRNR']
      
      let vendor_items = []
      await commonFunc.asyncForEach(dataSet, async (obj) => {
        let parseObj = commonFunc.getColToObj(obj, colList)

        // LFA1
        if (parseObj['MANDT'] && parseObj['LIFNR']) {
          if (parseObj['MANDT'] == '888' && checkGroup(parseObj['KTOKK'])) {
            let flag = getVendorChangeFlag(parseObj, 'ZMODE')
            if (commonFunc.isInvalidDate(parseObj['UPDAT'])) parseObj['UPDAT'] = null
            if (commonFunc.isInvalidDate(parseObj['UPTIM'])) parseObj['UPTIM'] = null
            parseObj['TIMESTAMP'] = [moment(parseObj['TIMESTAMP'], 'YYYYMMDDHHmmss').format()]
    
            if (flag == 2) {
              vendor_items.push(parseObj)
    
            } else if(flag == 1) {
              // 在update 之前 先把 array 中的資料 commit
              if (vendor_items.length > 0) {
                await commonFunc.batchUpsertOnSeq('wiprocurement.lfa1', ['MANDT', 'LIFNR'], vendor_items, fileName, client)
                vendor_items = []
              }
              // await deleteKeyValue('wiprocurement.lfa1', ['MANDT', 'LIFNR'], parseObj, client)
              await commonFunc.deleteDataBySeq('wiprocurement.lfa1', ['MANDT', 'LIFNR'], parseObj, fileName, client)
            }
          }
        } else {
          logger.error('get vendor gerenal data, key: MANDT or LIFNR is null')
          throw Error('get vendor gerenal data, key: MANDT or LIFNR is null')
        }
      })
      if (vendor_items.length > 0) {
        await commonFunc.batchUpsertOnSeq('wiprocurement.lfa1', ['MANDT', 'LIFNR'], vendor_items, fileName, client)
      }
    } else {
      logger.error('get vendor gerenal data, data format ERROR')
      throw Error('get vendor gerenal data, data format ERROR')
    }
  } catch(error) {
    logger.error('getVendor function insert to db error', error)
    throw error
  }
})

function getVendorChangeFlag(data, flag){
  if (data[flag]) {
    if ((data[flag] == 'C') || (data[flag] == 'U')) return 2
    else if (data[flag] == 'D') return 1
  }
  return 0
}

function checkGroup(account) {
  return ['Z001', 'Z009', 'Z007', 'Z002', 'Z003', 'Z011', 'Z012'].includes(account)
}

module.exports = {
  vendor,
}
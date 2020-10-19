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
const logger = log4js.getLogger('exchange_rate')

const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

let kafkajs = null
const topic = config.kafka.topic.exchange_rate

// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0

const sendEmailForNotice = async (errorMes, offset) => {
  try {
    let info = {
      topic: topic,
      subject: 'fetch exchange_rate failed',
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

  let logname = 'exchange_rate'
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
      await getExchRate(messageSet.EXCHRATE.EXCHRATE_ITEM, fileName)

    } catch(e) {

      counterData[topic][partition].Error++
      // await sendNoti(`(${config.env})failed to process xml: ${fileName}, kafka: t:${topic}, p:${partition}, o:${offset}`)
      await sendEmailForNotice(e, offset)
      let { uuid: logID } = await commonFunc.insertLogs(new commonFunc.Log('kafka', logname, 'Error', null, e.message, `${topic}|${partition}|${offset}`))

      logger.error('exchange_rate dataHandler function error', offset, e)
      throw Error(`can not parse data and update to database, ${offset}`, e)
    }
    counterData[topic][partition].Finished++
    await commonFunc.insertFinishedLog(topic, partition, offset, fileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
    logger.debug(`write the record into db, ${topic},${partition},${offset},${fileName}`)
  }
  logger.debug(`done with commit, t:${topic}, p:${partition}, o:${offset}`)
}

const exchange_rate = async function () {
  kafkajs = await new kafkaconn(`exchange_rate-${config.env}`)

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
      console.log(new Date(), 'exchange_rate')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, config.msgInterval);
  }
}

const getExchRate = dbHelper.atomic(async (client, dataSet, fileName)=> {
  let colList = ['KURST', 'FCURR', 'TCURR', 'GDATU', 'KURSM', 'UKURS', 'FFACT', 'TFACT', 'OPMOD', 'CRTDT', 'CRTTM']
  try {
    if (Array.isArray(dataSet) && dataSet.length > 0)  {      
      let exchRate_items = []

      await commonFunc.asyncForEach(dataSet, async (obj) => {
        let parseObj = commonFunc.getColToObj(obj, colList)

        if (parseObj['KURST'] && parseObj['FCURR'] && parseObj['TCURR'] && parseObj['GDATU'] ) {
          if (parseObj['KURST'] == 'M') {
            if (parseObj['OPMOD'] == 'C' || parseObj['OPMOD'] == 'U') {
              exchRate_items.push(parseObj)
              
            } else if (parseObj['OPMOD'] == 'D') {
              if (exchRate_items.length > 0) {
                // 在delete 之前 先把 array 中的資料 commit
                await commonFunc.batchUpsertOnSeq('wiprocurement.exchange_rate', ['KURST', 'FCURR', 'TCURR', 'GDATU'], exchRate_items, fileName, client)
                exchRate_items = []
              }
              // await deleteKeyValue('wiprocurement.exchange_rate', ['KURST', 'FCURR', 'TCURR', 'GDATU'], parseObj, client)
              await commonFunc.deleteDataBySeq('wiprocurement.exchange_rate', ['KURST', 'FCURR', 'TCURR', 'GDATU'], parseObj, fileName, client)
            } else {
              logger.warn(`Unknow mode(OPMOD): ${parseObj['OPMOD']}`)
            }
          } else {
            logger.info(`ignore exchange type (KURST): ${parseObj['KURST']}`)
          }
        } else {
          logger.error('get exchange_rate data, key: KURST or FCURR or TCURR or GDATU == null')
          throw Error('get exchange_rate data, key: KURST or FCURR or TCURR or GDATU == null')
        }
      })
      if (exchRate_items.length > 0) {
        await commonFunc.batchUpsertOnSeq('wiprocurement.exchange_rate', ['KURST', 'FCURR', 'TCURR', 'GDATU'], exchRate_items, fileName, client)
      }
    } else {
      logger.error('get exchange_rate data, data format ERROR')
      throw Error('get exchange_rate data, data format ERROR')
    }
  } catch(error) {
    logger.error('getExchRate function insert to db error', error)
    throw error
  }
})

module.exports = {
  exchange_rate,
  // getValue,
}

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
const logger = log4js.getLogger('mat_doc')
const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

let kafkajs = null
const topic = config.kafka.topic.mat_doc

// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0

const sendEmailForNotice = async (errorMes, offset) => {
  try {
    let info = {
      topic: topic,
      subject: 'fetch mat_doc failed',
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

  let logname = 'mat_doc'

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
      await getMaterialDoc(messageSet.MATDOC.MATDOC_ITEM, fileName)

    } catch(e) {
      counterData[topic][partition].Error++
      // await sendNoti(`(${config.env})failed to process xml: ${fileName}, kafka: t:${topic}, p:${partition}, o:${offset}`)
      await sendEmailForNotice(e, offset)
      let { uuid: logID } = await commonFunc.insertLogs(new commonFunc.Log('kafka', logname, 'Error', null, e.message, `${topic}|${partition}|${offset}`))

      logger.error('mat_doc dataHandler function error', offset, e)
      throw Error(`can not parse data and update to database, ${offset}`, e)
    }
    counterData[topic][partition].Finished++
    await commonFunc.insertFinishedLog(topic, partition, offset, fileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
    logger.debug(`write the record into db, ${topic},${partition},${offset},${fileName}`)
  }
  logger.debug(`done with commit, t:${topic}, p:${partition}, o:${offset}`)
}

const mat_doc = async function () {
  kafkajs = await new kafkaconn(`mat_doc-${config.env}`)

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
      console.log(new Date(), 'mat_doc')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, config.msgInterval);
  }
}

const getMaterialDoc = dbHelper.atomic(async (client, dataSet, fileName) => {
  let colList = {
    matdoc: ['ZCRTDAT', 'ZCRTTIM', 'MKPFCFLG', 'MSEGCFLG', 'MBLNR', 'MJAHR', 'VGART', 'BLART', 'BLAUM', 'BLDAT',
      'BUDAT', 'CPUDT', 'CPUTM', 'AEDAT', 'USNAM', 'XBLNR', 'BKTXT', 'XABLN', 'AWSYS', 'TCODE2',
      'SPE_BUDAT_UHR', 'SPE_BUDAT_ZONE', 'LE_VBELN'],
    matdoc_ln: ['MBLNR', 'MJAHR', 'ZEILE', 'LINE_ID', 'PARENT_ID', 'BWART', 'XAUTO', 'MATNR', 'WERKS', 'LGORT',
      'CHARG', 'INSMK', 'SOBKZ', 'LIFNR', 'KUNNR', 'KDAUF', 'KDPOS', 'KDEIN', 'SHKZG', 'WAERS',
      'DMBTR', 'BUALT', 'MENGE', 'MEINS', 'ERFMG', 'ERFME', 'BPMNG', 'BPRME', 'EBELN', 'EBELP',
      'LFBJA', 'LFBNR', 'LFPOS', 'SJAHR', 'SMBLN', 'SMBLP', 'ELIKZ', 'SGTXT', 'WEMPF', 'ABLAD',
      'KOKRS', 'KOSTL', 'AUFNR', 'GJAHR', 'BUKRS', 'RSNUM', 'RSPOS', 'UMMAT', 'UMWRK', 'UMLGO',
      'KZBEW', 'KZVBR', 'KZZUG', 'GRUND', 'EVERS', 'PRCTR', 'AUFPS', 'SAKTO', 'PPRCTR', 'QINSPST',
      'EMATN', 'MAT_KDAUF', 'MAT_KDPOS', 'LLIEF', 'ZUSTD_T156M', 'VGART_MKPF', 'BUDAT_MKPF', 'CPUDT_MKPF', 'CPUTM_MKPF', 'USNAM_MKPF',
      'XBLNR_MKPF', 'TCODE2_MKPF', 'VBELN_IM', 'VBELP_IM', 'PRUEFLOS', 'ZMSEG_TLGORT'],
  }

  try {
    if (Array.isArray(dataSet) && dataSet.length > 0)  {   

      let matdoc_items = [], matdoc_ln_itmes = []
      await commonFunc.asyncForEach(dataSet, async (obj) => {
        let { MATDOC_LN, ...newObject } = obj
        // MARADOC
        let parseMatdocObj = commonFunc.getColToObj(newObject, colList.matdoc)

        if (parseMatdocObj['MBLNR'] && parseMatdocObj['MJAHR']) {
          if (commonFunc.getChangeFlag(parseMatdocObj, 'MKPFCFLG')) {
            if (commonFunc.isInvalidDate(parseMatdocObj['AEDAT'])) parseMatdocObj['AEDAT'] = null
            if (commonFunc.isInvalidDate(parseMatdocObj['ZCRTDAT'])) parseMatdocObj['ZCRTDAT'] = null
            if (commonFunc.isInvalidDate(parseMatdocObj['BLDAT'])) parseMatdocObj['BLDAT'] = null
            if (commonFunc.isInvalidDate(parseMatdocObj['BUDAT'])) parseMatdocObj['BUDAT'] = null
            if (commonFunc.isInvalidDate(parseMatdocObj['CPUDT'])) parseMatdocObj['CPUDT'] = null
            if (parseMatdocObj['CPUTM'] == '' || commonFunc.isInvalidTime(parseMatdocObj['CPUTM'])) {
              parseMatdocObj['CPUTM'] = null
            }
            if (parseMatdocObj['SPE_BUDAT_UHR'] == '' || commonFunc.isInvalidTime(parseMatdocObj['SPE_BUDAT_UHR'])) {
              parseMatdocObj['SPE_BUDAT_UHR'] = null
            }
            matdoc_items.push(parseMatdocObj)
          }

          // MATDOC_LN
          if (commonFunc.getChangeFlag(parseMatdocObj, 'MSEGCFLG')) {
            let MATDOC_LN_ITEM = MATDOC_LN.MATDOC_LN_ITEM

            if (Array.isArray(MATDOC_LN_ITEM) && MATDOC_LN_ITEM.length > 0) {

              await commonFunc.asyncForEach(MATDOC_LN_ITEM, async function (item) {
                let parseMatdocLnObj = commonFunc.getColToObj(item, colList.matdoc_ln)

                if (parseMatdocLnObj['MBLNR'] && parseMatdocLnObj['MJAHR'] && parseMatdocLnObj['ZEILE']) {
                  if (commonFunc.isInvalidDate(parseMatdocLnObj['BUDAT_MKPF'])) parseMatdocLnObj['BUDAT_MKPF'] = null
                  if (commonFunc.isInvalidDate(parseMatdocLnObj['CPUDT_MKPF'])) parseMatdocLnObj['CPUDT_MKPF'] = null
                  if (parseMatdocLnObj['CPUTM_MKPF'] == '' || commonFunc.isInvalidTime(parseMatdocLnObj['CPUTM_MKPF'])) {
                    parseMatdocLnObj['CPUTM_MKPF'] = null
                  }

                  if (filterType(parseMatdocLnObj['BWART'])) {
                    matdoc_ln_itmes.push(parseMatdocLnObj)
                  }
                } else {
                  logger.error('get MATDOC_LN data, key: MBLNR or MJAHR or ZEILE == null')
                  throw Error('get MATDOC_LN data, key: MBLNR or MJAHR or ZEILE == null')
                }
              })
            }
          }
        } else {
          logger.error('get MARADOC data, key: MBLNR or MJAHR == null')
          throw Error('get MARADOC data, key: MBLNR or MJAHR == null')
        }
      })

      if (matdoc_items.length > 0) {
        await commonFunc.batchUpsertOnSeq('wiprocurement.matdoc', ['MBLNR', 'MJAHR'], matdoc_items, fileName, client)
      }
      if (matdoc_ln_itmes.length > 0) {
        await commonFunc.batchUpsertOnSeq('wiprocurement.matdoc_ln', ['MBLNR', 'MJAHR', 'ZEILE'], matdoc_ln_itmes, fileName, client)
      }
    } else {
      logger.error('get mat_doc data, data format ERROR')
      throw Error('get mat_doc data, data format ERROR')
    }
  } catch(error) {
    logger.error('getMaterialDoc function insert to db error', error)
    throw error
  }
})


function filterType(type) {
  return ['101', '102', '122', '123', '161', '162'].includes(type)
}

module.exports = {
  mat_doc,
}

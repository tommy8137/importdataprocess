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
const logger = log4js.getLogger('po')
const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')

let kafkajs = null
const topic = config.kafka.topic.po

// counterData 記錄收訊息的狀態並呈現, 若msgInterval設為0則不顯示
let counterData = {}
let totalCounter = 0

const sendEmailForNotice = async (errorMes, offset) => {
  try {
    let info = {
      topic: topic,
      subject: 'fetch po failed',
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

  let logname = 'po'
  let fileName = messageSet.CONTROL.FILE_NAME

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
      await getPo(messageSet.EKKO.EKKO_ITEM, fileName)
    } catch(e) {
      counterData[topic][partition].Error++
      // await sendNoti(`(${config.env})failed to process xml: ${fileName}, kafka: t:${topic}, p:${partition}, o:${offset}`)
      await sendEmailForNotice(e, offset)

      let { uuid: logID } = await commonFunc.insertLogs(new commonFunc.Log('kafka', logname, 'Error', null, e.message, `${topic}|${partition}|${offset}`))

      logger.error('po dataHandler function error', offset, e)
      throw Error(`can not parse data and update to database, ${offset}`, e)
    }
    counterData[topic][partition].Finished++
    await commonFunc.insertFinishedLog(topic, partition, offset, fileName, moment().format('YYYY-MM-DD HH:mm:ssZZ'), logname)
    logger.debug(`write the record into db, ${topic},${partition},${offset},${fileName}`)
  }
  logger.debug(`done with commit, t:${topic}, p:${partition}, o:${offset}`)
}

const Po = async function(){
  kafkajs = await new kafkaconn(`po-${config.env}`)

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
      console.log(new Date(), 'po')
      Object.keys(counterData).forEach(function (topic) {
        Object.keys(counterData[topic]).forEach(function (partition) {
          console.log(topic, partition, counterData[topic][partition].show())
        })
      })
    }, config.msgInterval);
  }
}

const getPo = dbHelper.atomic(async (client, dataSet, fileName)=> {
  try {
    let colList = {
      ekko: ['ZCRTDAT', 'ZCRTTIM', 'DATACFLG', 'EKPACFLG', 'EKPOCFLG', 'EBELN', 'MANDT', 'BUKRS', 'BSTYP', 'BSART',
        'BSAKZ', 'LOEKZ', 'STATU', 'AEDAT', 'ERNAM', 'PINCR', 'LPONR', 'LIFNR', 'SPRAS', 'ZTERM',
        'ZBD1T', 'ZBD2T', 'ZBD3T', 'ZBD1P', 'ZBD2P', 'EKORG', 'EKGRP', 'WAERS', 'WKURS', 'KUFIX',
        'BEDAT', 'KDATB', 'KDATE', 'IHREZ', 'LLIEF', 'KUNNR', 'ZZSUPWERKS', 'INCO1', 'INCO2', 'KTWRT',
        'KNUMV', 'KALSM', 'UNSEZ', 'ABSGR', 'ZZCSVD', 'ZZCSVN', 'ZZRENC', 'ZZREND', 'ZZHUBID', 'ZZKUNRE',
        'ZZVSBED', 'ZZINCO1', 'ZZINCO2', 'ZZZTERM', 'ZZTERDC', 'ZZFORD', 'ZZSHIY', 'ZZLGORT', 'ZPRLK'],
      ekpo: ['DATACFLG', 'EKETCFLG', 'EKESCFLG', 'EBELN', 'EBELP', 'LOEKZ', 'STATU', 'AEDAT', 'TXZ01', 'MATNR',
        'EMATN', 'BUKRS', 'ZZRECWERKS', 'LGORT', 'MATKL', 'INFNR', 'KTMNG', 'MENGE', 'MEINS', 'BPRME',
        'BPUMZ', 'BPUMN', 'UMREZ', 'UMREN', 'NETPR', 'PEINH', 'WEBAZ', 'MWSKZ', 'INSMK', 'SPINF',
        'ELIKZ', 'PSTYP', 'KNTTP', 'WEPOS', 'REPOS', 'WEBRE', 'LMEIN', 'EVERS', 'PRDAT', 'BSTYP',
        'PLIFZ', 'NTGEW', 'GEWEI', 'SOBKZ', 'SSQSS', 'BSTAE', 'KO_PRCTR', 'INCO1', 'INCO2', 'BANFN',
        'BNFPO', 'MTART', 'RETPO', 'LFRET', 'MPROF', 'MFRPN', 'MFRNR', 'BERID', 'ZZREFB', 'ZZETD',
        'ZZMRO_MAT', 'ZZMRO_QTY', 'ZZMRO_UNIT', 'ZZREPRI', 'ZZWAERS', 'ZZCASH', 'ZMEPRF', 'ZZTERM', 'ZREGION', 'LPRIO',
        'ZZRMANO', 'ZZPOSEX', 'ZZRTVAG'],
    }

    if (Array.isArray(dataSet) && dataSet.length > 0)  {
      let ekko_items = [], ekpo_items = []
      await commonFunc.asyncForEach(dataSet, async (d) => {
        let { EKKO_EKPA, EKKO_EKPO, ...newObject } = d

        // EKKO
        let parseEkkoObj = commonFunc.getColToObj(newObject, colList.ekko)
        if (parseEkkoObj['EBELN'] && parseEkkoObj['MANDT']) {
          let ekko_flag = getPoChangeFlag(parseEkkoObj, 'DATACFLG')
          if (commonFunc.isInvalidDate(parseEkkoObj['KDATE'])) parseEkkoObj['KDATE'] = null
          if (commonFunc.isInvalidDate(parseEkkoObj['KDATB'])) parseEkkoObj['KDATB'] = null
          if (commonFunc.isInvalidDate(parseEkkoObj['AEDAT'])) parseEkkoObj['AEDAT'] = null
      

          if (ekko_flag == 2) {
            ekko_items.push(parseEkkoObj)
    
          } else if (ekko_flag == 1) {
            // 在update 之前 先把 array 中的資料 commit
            if (ekko_items.length > 0 ) {
              await commonFunc.batchUpsertOnSeq('wiprocurement.ekko', ['EBELN', 'MANDT'], ekko_items, fileName, client)
              ekko_items = []
            }
            // await deleteKeyValue('wiprocurement.ekko', ['EBELN', 'MANDT'], parseEkkoObj, client)
            await commonFunc.deleteDataBySeq('wiprocurement.ekko', ['EBELN', 'MANDT'], parseEkkoObj, fileName, client)
          }

          // EKKO_EKPO
          // console.log(EKKO_EKPO)
          if (!_.isNil(EKKO_EKPO)) {
            let EKKO_EKPO_ITEM = EKKO_EKPO.EKKO_EKPO_ITEM

            if (Array.isArray(EKKO_EKPO_ITEM) && EKKO_EKPO_ITEM.length > 0) {
              await commonFunc.asyncForEach(EKKO_EKPO_ITEM, async function (items) {
                let { EKKO_EKPO_EKET, EKKO_EKPO_EKES, ...newEKPO } = items
                let parseEkpoObj = commonFunc.getColToObj(newEKPO, colList.ekpo)
                if (parseEkpoObj['EBELN'] && parseEkpoObj['EBELP']) {
  
                  if (commonFunc.isInvalidDate(parseEkpoObj['PRDAT'])) parseEkpoObj['PRDAT'] = null
                  if (commonFunc.isInvalidDate(parseEkpoObj['ZZETD'])) parseEkpoObj['ZZETD'] = null
                  if (commonFunc.isInvalidDate(parseEkpoObj['AEDAT'])) parseEkpoObj['AEDAT'] = null
                  
                  let ekpo_flag = getPoChangeFlag(parseEkpoObj, 'DATACFLG')
                  if (ekpo_flag == 2) {
                    ekpo_items.push(parseEkpoObj)
                  }
                } else {
                  logger.error('get EKPO data, key: EBELN or EBELP == null')
                  throw Error('get EKPO data, key: EBELN or EBELP == null')
                }
              })
            }
            
          } else {
            logger.error('EKKO data is not a Array, format ERROR')
            throw Error('EKKO data is not a Array, format ERROR')
          }
        } else {
          logger.error('get EKKO data, key: EBELN or MANDT == null')
          throw Error('get EKKO data, key: EBELN or MANDT == null')
        }
      })

      if (ekko_items.length > 0) {
        await commonFunc.batchUpsertOnSeq('wiprocurement.ekko', ['EBELN', 'MANDT'], ekko_items, fileName, client)
      }
      if (ekpo_items.length > 0) {
        await commonFunc.batchUpsertOnSeq('wiprocurement.ekpo', ['EBELN', 'EBELP'], ekpo_items, fileName, client)
      }
    } else {
      logger.error('get po data, data format ERROR')
      throw Error('get po data, data format ERROR')
    }
  } catch(error) {
    logger.error('getPo function insert to db error', error)
    throw error
  }
})

function getPoChangeFlag(data, flag){
  if (data[flag] == 'Y') return 2
  else if (data[flag] == 'D') return 1
  else return 0
}

module.exports = {
  Po,
  getPo,
}

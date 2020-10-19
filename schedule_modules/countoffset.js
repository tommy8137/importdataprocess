const config = require('../config.js')
const systemDB = require('../common/index.js')
const { topics, kafkaHost, env, forceFromBegin } = require('../config')
const moment = require('moment')
let squel = require('squel').useFlavour('postgres')
const Kafka = require('no-kafka')
const { Kafka: kafkaJS } = require('kafkajs')
const { asyncForEach } = require('../common.js')
const mail = require('../utils/mail/mail.js')
const msg = require('../utils/mail/message.js')
const log4js = require('../server/logger/logger')
const logger = log4js.getLogger('countOffset')
const _ = require('lodash')
const kafkaconn = require('../kafka_modules/kafka_connecter')
let consumer = null

/**
 * ex: [ { topic: 'SAP_Outbound_Others', partition: 0, offset: 17580 },
  { topic: 'SAP_Outbound_Others', partition: 1, offset: 17500 },
  { topic: 'SAP_Outbound_Others', partition: 2, offset: 46951 },
  { topic: 'whq.plm.com.sapalt', partition: 0, offset: '180' } ]
 */
let pastLatestOffset = []

/**
 *
 * @param {Array} topicList [ 'wtz.sap.mm.info_record', 'wcz.sap.mm.info_record' ]
 *
 * @returns {Array} [{ topic: 'wtz.sap.mm.info_record', partition: 0, offset: 800 }]
 */
const getLatestOffsetEachTopic = async (topicList) => {
  let kaflaLatestOffset = []
  try {
    await asyncForEach(topicList, async (topic) => {
      let result = await consumer.subscribe(topic, [], () => {}, { time: Kafka.LATEST_OFFSET })
      for (let i = 0; i < result.length; i++) {
        kaflaLatestOffset.push({ 'topic': topic, 'partition': parseInt(i), 'offset': result[i] })
        await consumer.unsubscribe(topic, parseInt(i))
      }
    })
  } catch (err) {
    logger.error('getLatestOffset: get latest offset error', err)
    logger.error('error param are ', topicList)
  }
  return kaflaLatestOffset
}

/**
 *
 * @param {*} topic 辨識的topic名稱
 * @param {*} item 要整理的資料
 *
 * @returns {Array} ex: {
  offset: "180",
  partition: 0,
  topic: "alt"
}
 */
const collateKafkaJSLatestRes = (topic, item) => {
  return _.map(item, (r) => {
    return { topic, partition: r.partition, offset: r.offset }
  })
}

const getPlmLatestOffset = async () =>{

  const kafka = new kafkaJS({
    clientId: `countoffset-${config.env}`,
    brokers: [`${config.kafka.kafkaHost}:${config.kafka.kafkaPort}`],
    sasl: {
      mechanism: 'plain', // scram-sha-256 or scram-sha-512
      username: `${config.kafka.saslName}`,
      password: `${config.kafka.saslPw}`,
    },
  })
  let topicKey = config.kafka.topic
  
  const admin = kafka.admin()
  await admin.connect()

  let plmLatestOffset = []

  // Object.keys(topicList) = ['plm', 'alt', 'vendor', 'mat_doc']
  for(let i = 0; i < Object.keys(topicKey).length; i++) {
    let topicList = topicKey[Object.keys(topicKey)[i]]

    for(let j = 0; j < topicList.length; j++) {
      // res: [ { partition: 0, offset: '180', high: '180', low: '180' } ]
      let fetchRes = await admin.fetchTopicOffsets(topicList[j])
      let res = collateKafkaJSLatestRes(topicList[j], fetchRes)
      
      plmLatestOffset.push(...res)
    }
  }

  await admin.disconnect()
  return plmLatestOffset
}

/**
 * 將 offset 與 db 的數量做計算比較是否有相同
 *
 * @param {Array} list topic list ex: [{ topic: 'wtz.sap.mm.info_record', partition: 0 }]
 * @param {Array} dbOffset ex: [{ topic: 'wtz.sap.mm.info_record', partition: 0, count: 50, errorCount: 2 }]
 * @param {Array} latestOffset ex: [{ topic: 'wtz.sap.mm.info_record', partition: 0, offset: 800 }]
 *
 */
const getCompareResult = (topiclist, dbOffset, latestOffset) => {
  logger.debug('start to compare offset count with', topiclist, dbOffset, latestOffset)
  // compare
  let result = {}, compareOffset = 0, errorOffset = 0
  topiclist.forEach(i => {
    let dbOffsetRes = dbOffset.find(x => x.topic == i.topic && x.partition == i.partition)
    let dbCount = dbOffsetRes ? parseInt(dbOffsetRes.count) : 0
    let dbErrorCount = dbOffsetRes ? parseInt(dbOffsetRes.errorCount) : 0

    let findPastOffset = pastLatestOffset.find(x => x.topic == i.topic && x.partition == i.partition)
    let pastOffset = findPastOffset ? findPastOffset.offset : 0

    let findLatestOffset = latestOffset.find(x => x.topic == i.topic && x.partition == i.partition)
    let lastestOffset = findLatestOffset ? findLatestOffset.offset : 0

    let needCount = lastestOffset - pastOffset
    if (!result[i.topic]) result[i.topic] = {}

    result[i.topic][i.partition] = {
      'YesterdayLatestOffset': pastOffset,
      'TodayLatestOffset': lastestOffset,
      'DBShouldReceive': needCount,
      'DBHave': dbCount,
      'DBErrorCount': dbErrorCount,
      'QuantityVariance': dbCount - needCount,
    }
    if (needCount != dbCount) compareOffset += 1
    if (dbErrorCount > 0) errorOffset += 1
  })
  return {
    result,
    compareOffset,
    errorOffset,
  }
}

/**
 * 如果資料量有不一致, 將會寄Email 通知
 *
 * @param {object} result {
  'wtz.sap.mm.info_record': {
    '0': {
      YesterdayLatestOffset: '180',
      TodayLatestOffset: '180',
      DBReceive: 0,
      DBHave: 1,
      DBErrorCount: 1,
      QuantityVariance: 1
    }
  }
}
 *
 * @param {number} compareOffset 紀錄是否有數量不一致, > 0的話才需要寄信
 */
const sendEmailForNotice = async (result, compareOffset, errorOffset) => {
  try {
    let info = {
      typeName: 'countoffset',
      updateBy: 'cronjob',
    }
    info.content = jsonToTable(result)
    if (compareOffset > 0 || errorOffset > 0) {
      info.compareResult = 'db count is not equal to count from kafka'
      await mail.sendmail(msg.sendMsg(info))
      logger.info('db count is not equal to count from kafka, and sent email success,', moment().format('YYYY-MM-DD HH:mm:ss'))
    }

  } catch (err) {
    logger.error('sendEmailForNotice: ', err)
    logger.error('error param are ', result)
  }
}

/**
 * 將資料庫 status 為 Finished or Error or Ignore的數量做計算 by topic & partition
 *
 * @param {Array} list topic list ex: [{ topic: 'wtz.sap.mm.info_record', partition: 0 }]
 * @param {Array} finised topic & partition 被寫入db 的數量 ex: [ { topic: 'wtz.sap.mm.info_record', partition: '0', count: 47 }]
 * @param {Array} others topic & partition 在db中被Ignore or Error的資料 ex: [ 'wtz.sap.mm.info_record|0|123' ]
 *
 * @returns {Array} ex: [{ topic: 'wtz.sap.mm.info_record', partition: 0, count: 50 }]
 */
const handleDBCount = (list, finised, ignoreData, errorData) => {
  return list.map(res => {
    // find ignoreData count by res.topic & partition
    let countByLogIgnore = _.countBy(ignoreData, o => {
      let s = o.split('|')
      let topic = s[0]
      let partition = s[1]
      return topic == res.topic && partition == res.partition
    })

    // find ignoreData count by res.topic & partition
    let countByLogError = _.countBy(errorData, o => {
      let s = o.split('|')
      let topic = s[0]
      let partition = s[1]
      return topic == res.topic && partition == res.partition
    })
    let ErrorCount = countByLogError.true ? countByLogError.true : 0
    let IgnoreCount = countByLogIgnore.true ? countByLogIgnore.true : 0
    let index = _.findIndex(finised, x => x.topic == res.topic && x.partition == res.partition)
    
    res.errorCount = ErrorCount
    res.count = IgnoreCount + ErrorCount
    if (index >= 0) {
      res.count += finised[index].count
    }
    return res
  })
}

// main function
const countoffset = async function () {
  logger.info('start count Offset, ', moment().format('YYYY-MM-DD HH:mm:ss'))
  console.log('----start count Offset----')
  logger.info('start to init kafka connect to id:', `countoffset-${env}`)
  consumer = await new kafkaconn(`countoffset-${env}`)

  
  let topicList = [] // topicList會用kafka library的方法找 最新的topic
  let list = [] // 將所有的topic 與partition 列下來 [{ topic: '', partition: 0 }]
  
  // 整理kafka 2.0 topic list
  let plmTopic = config.kafka.topic
  Object.keys(plmTopic).forEach(t => {
    let topic = plmTopic[t]
    topic.forEach(t => {
      list.push({ topic: t, partition: 0 })
    })
  })

  
  // 整理kafka 0.9 topic list
  Object.keys(topics).forEach(f => {
    Object.keys(topics[f]).forEach(t => {
      topicList.push(t)
      topics[f][t].forEach(p => {
        list.push({ 'topic': t, 'partition': p })
      })
    })
  })

  // // get 最新的offset by topic, partition
  // console.log('get 最新的offset by topic, partition')
  let kaflaLatestOffset = await getLatestOffsetEachTopic(topicList)
  let plmLatestOffset = await getPlmLatestOffset()

  let latestOffset = []
  if (pastLatestOffset.length <= 0) {
    logger.info('Service does not have yesterday\'s offset')
  } else {

    // get Offset from db
    let yesterday = moment().subtract(1, 'day').format('YYYY-MM-DD 00:00:00')
    let today = moment().format('YYYY-MM-DD 00:00:00')

    logger.debug(`get db count from ${yesterday} to ${today}`)
    let dbOffset = await getDBfinishedLogTotalOffset(yesterday, today)

    let logsKafkaIgnore = await getDBLogsKafkaTotalOffset(yesterday, today, 'ignore')
    let logsKafkaError = await getDBLogsKafkaTotalOffset(yesterday, today, 'error')

    let dbAllCount = null
    if (logsKafkaIgnore.length > 0 || logsKafkaError.length > 0) {
      dbAllCount = handleDBCount(list, dbOffset, logsKafkaIgnore.map(l => l.rsv3), logsKafkaError.map(l => l.rsv3))
    } else {
      dbAllCount = dbOffset
    }
    latestOffset = [...kaflaLatestOffset, ...plmLatestOffset]
    // get result
    let { result, compareOffset, errorOffset } = getCompareResult(list, dbAllCount, latestOffset)
    logger.debug('get compare result:', result, 'and get different offset count', compareOffset)
    // sent email to someone
    await sendEmailForNotice(result, compareOffset, errorOffset)
  }
  
  // ex: kaflaLatestOffset = [{ topic: 'wtz.sap.mm.info_record', partition: 0, offset: 800 }]
  // copy today latest offset to past param for tomorrow
  pastLatestOffset = latestOffset
  kaflaLatestOffset = []
  plmLatestOffset = []
  latestOffset = []

  await consumer.end()
  logger.info('end count Offset,', moment().format('YYYY-MM-DD HH:mm:ss'))
}

/**
 * 昨天被寫入資料庫的資料量 by topic & partition
 * @param {String} yesterday ex: 2019-06-10 00:00:00
 * @param {String} today ex: 2019-06-11 00:00:00
 *
 * @returns {Array} ex: [ { topic: 'wtz.sap.mm.info_record', partition: '0', count: 47 },
                      { topic: 'wtz.sap.mm.info_record', partition: '1', count: 45 } ]
 */
const getDBfinishedLogTotalOffset = async function (yesterday, today) {
  let sql = squel.select()
    .field('topic')
    .field('partition')
    .field('count(1)', 'count')
    .from('wiprocurement.logs_kafka_finished')
    .group('topic')
    .group('partition')
    .where('update_time >= ? and update_time < ?', yesterday, today)

  const result = await systemDB.Query(sql.toParam())
  return result.rows
}

/**
 * 昨天從Kafka收進來 但非我們需要資料(Ignore or Error) by topic & partition
 * @param {String} yesterday ex: 2019-06-10 00:00:00
 * @param {String} today ex: 2019-06-11 00:00:00
 *
 * @returns {Array} ex: [ { rsv3: "wtz.sap.mm.info_record|0|<offset>" }, { rsv3: "wtz.sap.mm.info_record|1|<offset>" } ]
 */
const getDBLogsKafkaTotalOffset = async function (yesterday, today, status) {
  let sql = squel.select()
    .distinct()
    .field('rsv3')
    .from('wiprocurement.logs_kafka')
    .where('update_time >= ? and update_time < ? and upper(status) = upper(?)', yesterday, today, status)

  const result = await systemDB.Query(sql.toParam())
  return result.rows
}

function jsonToTable(json) {
  let table = ''
  for (var k in json) {
    table = table + '<tr><th scope="row" style="border:1px solid black;">' + k + '</th>'
    for (var i in json[k]) {
      table = table + '<td style="border:1px solid black;"><table>'
      for (var j in json[k][i]) {
        table = table + '<tr><td>' + j + '</td><td>' + json[k][i][j] + '</tr></td>'
      }
      table = table + '</table></td>'
    }
    table = table + '</tr>'
  }
  return table
}
module.exports = {
  countoffset,
}

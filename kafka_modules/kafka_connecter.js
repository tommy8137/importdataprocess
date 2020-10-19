/**
 *  @example (async () => {
  kf = await new kafkaconn(env)
  await kf.subscribe('www.sap.mm.material_master', 1, () => { }, {})
  await kf.unsubscribe('www.sap.mm.material_master', 1)
  await kf.end()
  await kf.subscribe('www.sap.mm.material_master', 1, () => { }, {})
})()

 */


const _ = require('lodash')
const Kafka = require('no-kafka')
const { kafkaHost } = require('../config.js')
const log4js = require('../server/logger/logger')
const logger = log4js.getLogger('kafka_connector')
const { forceFromBegin } = require('../config.js')

class KafkaConnector {
  constructor(env = 'test') {
    return (async () => {
      let kafkaGroupId = `kafka-group-wiprocure-${env}`
      let kafkaClientId = `kafka-group-wiprocure-${env}`
      this.kconsumer = new Kafka.SimpleConsumer({
        connectionString: kafkaHost,
        maxBytes: 10485760,
        groupId: kafkaGroupId,
        clientId: kafkaClientId,
        recoveryOffset: Kafka.EARLIEST_OFFSET,
        handlerConcurrency: 1,
      })
      try {
        let initRes = await this.kconsumer.init()
        logger.debug(`kafka connection config, group_id:${kafkaGroupId}, client_id:${kafkaClientId}`, initRes)
      } catch (err) {
        logger.error('failed to connect kafka connector', kafkaHost)
        throw Error(`failed to connect kafka connector: ${kafkaHost}`)
      }
      return this
    })()
  }

  /**
   * 
   * @param {String} topic kafka topic, ex: 'www.sap.mm.material_master'
   * @param {Integer} partition kafka partition, ex: 1
   * @param {Integer} offset kafka offset, ex: 8518
   */
  async commitOffset(topic, partition, offset) {
    try {
      let commitRes = await this.kconsumer.commitOffset({ topic: topic, partition: partition, offset: offset })
      logger.debug(`kafka commitOffset, topic:${topic}, partition:${partition}, offset:${offset}`, commitRes)
      return { topic, partition, offset }
    } catch (err) {
      logger.error(`failed commitOffset, topic:${topic}, partition:${partition}`, err)
      return []
    }
  }

  /**
   *
   * @param {Array} list kafka topic list, ex: [{ topic: 'www.sap.mm.material_master', partition: 1 }]
   */
  async fetchOffset(list) {
    try {
      let fetchRes = await this.kconsumer.fetchOffset(list)
      logger.debug('kafka fetchOffset, topic list:', list, fetchRes)
      return fetchRes
    } catch (err) {
      logger.error('failed fetchOffset, topic list:', list, err)
      return []
    }
  }

  /**
  * 
  * @param {String} topic kafka topic, ex: 'www.sap.mm.material_master'
  * @param {Integer} partition  kafka topic partition, ex: 1
  * @param {Function} handler 當收到資料時所執行的處理function
  * @param {Object} option kafka連線的參數
  * @returns {Array} 連線的offset參數
  */
  async subscribe(topic, partition, handler, option) {
    try {
      let subscribeRes = await this.kconsumer.subscribe(topic, partition, option, handler)
      logger.debug(`kafka subscribe, topic:${topic}, partition:${partition}`, subscribeRes)
      return subscribeRes
    } catch (err) {
      logger.error(`failed subscribe, topic:${topic}, partition:${partition}`, err)
      throw Error(`failed subscribe, topic:${topic}, partition:${partition}`)
    }
  }

  /**
  * 
  * @param {String} topic kafka topic, ex: 'www.sap.mm.material_master'
  * @param {Integer} partition  kafka topic partition, ex: 1
  */
  async unsubscribe(topic, partition) {
    try {
      let unsubscribeRes = await this.kconsumer.unsubscribe(topic, partition)
      logger.debug(`kafka unsubscribe, topic:${topic}, partition:${partition}`, unsubscribeRes)
      return unsubscribeRes
    } catch (err) {
      logger.error(`failed unsubscribe, topic:${topic}, partition:${partition}`, err)
      return []
    }
  }

  /**
   * 結束kafka clinet的連線
   */
  async end() {
    try {
      let endRes = await this.kconsumer.end()
      console.log('endRes', endRes)
    } catch (err) {
      logger.warn('failed to end the kafka connection')
    }
  }

  async getStartOffset (subscriptions) {
    let list = [], result = []
    subscriptions.forEach(async function (element) {
      element.partition.forEach((p) => {
        list.push({ 'topic': element.topic, 'partition': p })
      })
    })
  
    try {
      logger.debug(`config forceFromBegin: ${forceFromBegin}`)
      let commitOffsetList = (!forceFromBegin) ? await this.fetchOffset(list) : []
  
      subscriptions.forEach(element => {
        for (let i = 0; i < element.partition.length; i++) {
          if (forceFromBegin) {
            logger.info(`force offset from earliest offset, subscribe topic: ${element.topic}, partition: ${element.partition[i]}, offset: Kafka.EARLIEST_OFFSET`)
            result.push({
              topic: element.topic,
              partition: element.partition[i],
              option: { time: Kafka.EARLIEST_OFFSET },
            })
          } else {
            // get offset by topic & partition
            let commitOffset = commitOffsetList.find(c => c.topic == element.topic && c.partition == element.partition[i])
            let kafkaOption = (commitOffset && commitOffset.offset != -1) ? { offset: commitOffset.offset } : { time: Kafka.EARLIEST_OFFSET }
            result.push({
              topic: element.topic,
              partition: element.partition[i],
              option: kafkaOption,
            })
            logger.info(`subscribe topic: ${element.topic}, partition: ${element.partition[i]}, offset:`, kafkaOption)
          }
        }
      })
    } catch (err) {
      logger.error(`get commit offset or early offset error`, err)
      logger.error('error param are ', subscriptions)
    }
  
    return result
  }
}
module.exports = KafkaConnector
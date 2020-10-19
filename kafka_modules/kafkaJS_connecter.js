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
const { Kafka } = require('kafkajs')
const config = require('../config.js')
const log4js = require('../server/logger/logger')
const logger = log4js.getLogger('kafka_connector by kafkajs')
const { forceFromBegin } = require('../config.js')

const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')

class KafkaConnector {
  constructor(env = 'test') {
    return (async () => {

      const kafkaJs = new Kafka({
        clientId: `wiprocurement-client-${env}`,
        brokers: [`${config.kafka.kafkaHost}:${config.kafka.kafkaPort}`],
        sasl: {
          mechanism: 'plain', // scram-sha-256 or scram-sha-512
          username: `${config.kafka.saslName}`,
          password: `${config.kafka.saslPw}`,
        },
      })
      this.registry = new SchemaRegistry({ host: `${config.kafka.RegistryUrl}` })
      this.kconsumer = kafkaJs.consumer({ groupId: `wiprocurement-group-${env}` })
      return this
    })()
  }

/**
  * connect kafka topic by kafkaJS
  * @returns {Array} 連線的offset參數
  */
  async connect() {
    try {
      await this.kconsumer.connect()
      logger.debug('start to kafka connect')
    } catch (err) {
      logger.error('failed kafka connect', err)
      throw Error(err)
      // return []
    }
  }

/**
  * subscribe kafka topic by kafkaJS
  * @param {String} topic kafka topic, ex: 'www.sap.mm.material_master'
  * @param {Boolean} fromBeginning 是否從起始開始抓取
  * @returns {Array} 連線的offset參數
  */
  async subscribe(topicList, fromBeginning) {
    try {
      topicList.forEach(async function (topic) {
        let subscribeRes = await this.kconsumer.subscribe({ topic: `${topic}`, fromBeginning: `${fromBeginning}` })
        logger.debug(`start to kafka subscribe, topic:${topic}`, subscribeRes)
      }, this)
      return
      // return subscribeRes
    } catch (err) {
      logger.error(`failed subscribe, topic:${topicList}`, err)
      throw Error(err)
      // return []
    }
  }

/**
  * disconnect kafka topic by kafkaJS
  * @returns {Array} 連線的offset參數
  */
  async disconnect() {
    try {
      let connectRes = await this.kconsumer.disconnect()
      logger.debug('start to kafka disconnect', connectRes)
      console.log(connectRes)
      return connectRes
    } catch (err) {
      logger.error('failed kafka connect', err)
      return []
    }
  }

  async stop() {
    try {
      let connectRes = await this.kconsumer.stop()
      logger.debug('stop kafka', connectRes)
      console.log(connectRes)
      return connectRes
    } catch (err) {
      logger.error('failed kafka stop', err)
      return []
    }
  }

}
module.exports = KafkaConnector
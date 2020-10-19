const systemDB = require('../common/index.js')
const log4js = require('log4js')
const logger = log4js.getLogger('postgres')

function makeid(length) {
  var result           = '';
  var characters       = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  var charactersLength = characters.length;
  for ( var i = 0; i < length; i++ ) {
     result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

module.exports = {
  atomic: function (fn) {
    const self = this
    async function wrapper(...args) {
      let client = await systemDB.pool.connect()
      let rid = makeid(5)
      logger.debug(`--- NEW CONNECTION FROM POOL --- idleCount:${systemDB.pool.idleCount}, totalCount:${systemDB.pool.totalCount}`, rid)
      // self.client = client
      // fn.bind(self);
      args.unshift(client)
      try {
        await client.query('BEGIN')
        logger.debug('--- BEGIN ---',rid)
        let res = await fn.apply(this, args)
        await client.query('COMMIT')
        logger.debug('--- COMMIT ---',rid)
        client.release()
        return res
      } catch (err) {
        logger.warn('--- ROLLBACK ---', rid, err)
        await client.query('ROLLBACK')
        client.release()
        throw err
      }
    }
    return wrapper
  },
}

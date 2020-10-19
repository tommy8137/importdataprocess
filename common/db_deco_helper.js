const systemDB = require('../common/index.js')
const log4js = require('log4js')
const logger = log4js.getLogger('postgres')

class DBDecoHelper {
    constructor() {

    }

    func_transaction() {
        const self = this;
        return function (target, name, descriptor) {
            const method = descriptor.value;
            let pgPool = systemDB.pool
            let ret;
            descriptor.value = async (...args) => {
                try {
                    console.log('test!!!!!!')
                    this.client = await pgPool.connect()
                    await this.client.query('BEGIN')
                    method.bind(self)
                    ret = await method.apply(target, args)
                    await this.client.query('COMMIT')
                } catch (error) {
                    console.log('eeeeeeeee',error)
                    if (this.client) {
                        await this.client.query('ROLLBACK')
                        logger.warn("--- ROLLBACK ---", error)
                    }
                    throw error;
                }
                return ret;
            };
        }
    }
}

export default new DBDecoHelper();
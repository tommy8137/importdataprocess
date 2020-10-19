const router = require('koa-router')
const rawDataRouter = require('./api/importRawData.js')
const apiRouter = new router()

apiRouter.use('/importRawData', rawDataRouter.routes())


module.exports = apiRouter

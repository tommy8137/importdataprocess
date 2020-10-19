const router = require('koa-router')
const rawdata = require('../../service/rawdata.js')

const apiRouter = new router()
const importRawData = new rawdata()

apiRouter.get('/all/:date/:topic', importRawData.importAll)
apiRouter.get('/single/:type/:date/:topic/:partition/:offset/:filename', importRawData.importSingle)

module.exports = apiRouter

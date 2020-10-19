const { fork } = require('child_process')
const { plm } = require('./kafka_modules/plm.js')
const { vendor } = require('./kafka_modules/vendor_general.js')
const { mat_doc } = require('./kafka_modules/mat_doc.js')

const { exchange_rate } = require('./kafka_modules/exchange_rate.js')
const { img } = require('./kafka_modules/img.js')
const { info_record } = require('./kafka_modules/info_record.js')
// const { SAP_Outbound_MM } = require('./kafka_modules/sap_outbound_mm.js')
const { Po } = require('./kafka_modules/po.js')
const { MatMaster } = require('./kafka_modules/material_master.js')
const { pcb } = require('./kafka_modules/pcb.js')
// const {importAll, importSingle} = require('./service/rawdata.js')
const schedule = require('./schedule_modules/schedule.js')
const Koa = require('koa')
const bodyParser = require('koa-bodyparser')
const koaLogger = require('koa-logger')
const router = require('koa-router')
const app = new Koa()
const apiRouter = require('./server/router/index.js')
const { port, env, pg, forceProcess } = require('./config.js')
const isDocker = require('is-docker');
const EventEmitter = require('events')
const log4js = require('./server/logger/logger')
const logger = log4js.getLogger('server')
// const { countoffset } = require('./schedule_modules/countoffset')

EventEmitter.defaultMaxListeners = 100

logger.info(`the system is now on env: ${env}`)
if (!isDocker()) {
  if (env == 'dev' || env == 'prod' || pg.pgIp == '10.34.3.107' || pg.pgIp == '192.168.100.210'|| pg.pgIp == '192.168.100.208') {
    process.exit(1);
  }
}


// // compression once
// const compression = require('./schedule_modules/compression.js')
// // compression with schedule


switch (process.env.TOPIC_NAME) {
  case 'exchange_rate':
    exchange_rate()
    plm()
    pcb()
    break
  case 'img':
    img()
    break
  case 'info_record':
    info_record()
    break
  case 'sap_outbound_mm':
    mat_doc()
    vendor()
    break
  case 'po':
    Po()
    break
  case 'checker_rawdata': {
    app.use(koaLogger())
    app.use(bodyParser())
    const appRouter = new router()
    appRouter.use(apiRouter.routes())
    app.use(appRouter.routes())
    console.log('>> available API list: \n', apiRouter.stack.map(i => `[${i.methods}] ${i.path}`))
    app.listen(port)
    console.log(`>> the server is start at port ${port}`)

    // kafka_checker
    // countoffset()
    schedule()
    break
  }
  case 'MatMaster':
    MatMaster()
    break
  default:
    console.log('without env variable: TOPIC_NAME')
    break
}

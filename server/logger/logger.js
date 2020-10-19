const log4js = require('log4js')
log4js.configure({
  appenders: {
    out: {
      type: 'stdout',
    },
    file: {
      type: 'file', filename: 'logs/data-process.log',
      maxLogSize: 10485760,
      numBackups: 7,
      compress: true,
    },
  },
  categories: {
    default: { appenders: ['out', 'file'], level: 'debug' },
  },
})
module.exports = log4js

const compression = require('./compression')
const { countoffset } = require('./countoffset')
const schedule = require('node-schedule')
const config = require('../config.js')

const scheduleFun = () => {
  console.log('----start schedule job----')
  schedule.scheduleJob(config.scheduleTime, () => {
    countoffset()
  })
}

module.exports = scheduleFun

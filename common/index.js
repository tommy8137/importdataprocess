const Postgres = require('./postgres.js')
const config = require('../config.js')

const systemDB = Postgres(config.pg, { max: 20, application_name: `wieprocure-data-processing-${process.pid}` })

module.exports = systemDB
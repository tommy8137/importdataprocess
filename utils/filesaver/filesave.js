const moment = require('moment-timezone')
const fs = require('fs')
const log4js = require('../../server/logger/logger')
const logger = log4js.getLogger('filesaver')

function fileSaver(topic, partition, offset, xmlFileName, data) {
    let today = moment().format('YYYY-MM-DD')
    let pathByDate = `Message/${today}/${topic}/${partition}`
    if (!fs.existsSync(pathByDate)) mkdirSyncRecursive(pathByDate)
    let timestamp = moment().format('x')
    
    logger.debug(`create file ${timestamp}-${offset}-${xmlFileName}`)
    let logStream = fs.createWriteStream(`Message/${today}/${topic}/${partition}/${timestamp}-${offset}-${xmlFileName}`)
    logStream.write(data.toString())
    logStream.end()
}

const mkdirSyncRecursive = (directory) => {
    var path = directory.replace(/\/$/, '').split('/');
    for (var i = 1; i <= path.length; i++) {
        var segment = path.slice(0, i).join('/');
        !fs.existsSync(segment) ? fs.mkdirSync(segment) : null;
    }
}

module.exports = {
    fileSaver,
    mkdirSyncRecursive,
}

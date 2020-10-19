const moment = require('moment-timezone')
const config = require('../../config.js')

function sendMsg(info) {
  return {
    from: config.sender,
    to: config.recevier,
    subject: `${info.typeName} result: ${info.compareResult}`,
    html: `<div>
            <div> Dear sirs, </div>
            <div> <b>Time:</b> ${moment.tz('Asia/Taipei')} </div>
            <div> <b>DB IP:</b> ${config.pg.pgIp} </div>
            <div> <b>Kafka IP:</b> ${config.kafkaHost}, ${config.kafka.kafkaHost}:${config.kafka.kafkaPort} </div>
            <table class="table" style="border:1px solid black;">
              <thead>
                <tr>
                  <th scope="col" style="border:1px solid black;">topic</th>
                  <th scope="col" style="border:1px solid black;">partition:0</th>
                  <th scope="col" style="border:1px solid black;">partition:1</th>
                  <th scope="col" style="border:1px solid black;">partition:2</th>
                  <th scope="col" style="border:1px solid black;">partition:3</th>
                  <th scope="col" style="border:1px solid black;">partition:4</th>
                  <th scope="col" style="border:1px solid black;">partition:5</th>
                </tr>
              </thead>
              <tbody>
              `+ info.content + `
              </tbody>
            </table>
          </div>`
  }
}
function sendTopicErrorMsg(info) {
  
  return {
    from: config.sender,
    to: config.recevier,
    subject: `${info.topic} result: ${info.subject}`,
    html: `<div>
            <div> Dear sirs, </div>
            <div> <b>Time:</b> ${moment.tz('Asia/Taipei')} </div>
            <div> <b>DB IP:</b> ${config.pg.pgIp} </div>
            <div> <b>Kafka IP:</b> ${info.kafkaIp} </div>
            <div> <b>Topic:</b> ${info.topic} </div>
            <div> <b>offset:</b> ${info.offset} </div>
            <dix> <b>Error:</b> ${info.message} </div>
          </div>`
  }
}
module.exports = {
  sendMsg,
  sendTopicErrorMsg,
}

const nodemailer = require('nodemailer')
const { mailConfig, sender } = require('../../config.js')
async function sendmail(message) {
  let transporter

  if (mailConfig.host == 'email-smtp.us-east-1.amazonaws.com') {
    transporter = nodemailer.createTransport(
      {
        host: mailConfig.host,
        port: 25,
        auth: {
          user: mailConfig.smtp_user,
          pass: mailConfig.smtp_password,
        },
        logger: false,
        debug: false, // include SMTP traffic in the logs
      },
      {
        from: sender,
      }
    )
  } else {
    transporter = nodemailer.createTransport(
      {
        host: mailConfig.host,
        port: mailConfig.port,
        logger: false,
        debug: false, // include SMTP traffic in the logs
      },
      {
        from: sender,
      }
    )
  }
  await transporter.sendMail(message)
}
module.exports = {
  sendmail,
}

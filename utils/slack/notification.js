const request = require('request');
const SLACK_URL = 'https://hooks.slack.com/services/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'

const postBody = async (url, payload) => {
    const options = {
        url: url,
        headers: { "content-type": "application/json", },
        json: true,
        body: payload
    };
    // Return new promise
    return new Promise(function (resolve, reject) {
        // Do async job
        request.post(options, function (err, resp, body) {
            if (err) {
                reject(err);
            } else {
                resolve(body);
            }
        })
    })
}

const sendNoti = async (msg = "This is posted to #general and comes from a bot named webhookbot.") => {
    let data = { "channel": "#wieprocurement", "username": "webhookbot", "text": msg, "icon_emoji": ":ghost:" }
    await postBody(SLACK_URL, data)
}
const sendTaskStatus = async(status)=>{
    let data = {"icon_emoji": ":ghost:",
    "text":"hhhhhh",
       "attachments": [
           {
               "fallback": "Required plain-text summary of the attachment.",
               "color": "#36a64f",
               "pretext": "Optional text that appears above the attachment block",
               "author_name": "Bobby Tables",
               "author_link": "http://flickr.com/bobby/",
               "author_icon": "http://flickr.com/icons/bobby.jpg",
               "title": "Slack API Documentation",
               "title_link": "https://api.slack.com/",
               "text": "Optional text that appears within the attachment",
               "fields": status,
               "image_url": "http://my-website.com/path/to/image.jpg",
               "thumb_url": "http://example.com/path/to/thumb.png",
               "footer": "Slack API",
               "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png"
           }
       ]
   }
   await postBody(SLACK_URL, data)
}

module.exports = {
    sendNoti,
    sendTaskStatus,
}

// sendNoti('GAGAGAGA!!!!')
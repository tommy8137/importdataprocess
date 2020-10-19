var topic = 'SAP_Outbound_FICO';
 //var topic = 'SAP_Outbound_FICO';
var parsing = require('xml2js').parseString; 
var fs = require('fs')
var pg = require('pg')
var kafka = require('kafka-node')
// Consumer = kafka.Consumer, 
const config = {
	user:'postgres',
	host:'172.17.0.2',
	database:'info',
	port:'5432',
};
const clientPg = new pg.Client(config);
clientPg.connect()
client = new kafka.KafkaClient({kafkaHost: '10.37.36.96:9092'}); 


var topics = [ 
// { topic: topic, partition: 1 }, 
{ topic: topic, partition: 1},
]; 
var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 }; 

var consumer = new kafka.Consumer(client, topics, options); 
var offset = new kafka.Offset(client);
	
consumer.on('message', function (message) { 

	parsing(message.value, async function(err, result) {
		const exchane = result .SAP_XML.EXCHRATE

		if(exchane != null) {
			for(var i = 0; i< exchane[0].EXCHRATE_ITEM.length; i++) {
				const tmp = exchane[0].EXCHRATE_ITEM[i]
				const time = tmp.GDATU
				const key = Object.keys(tmp)
				const v = Object.values(tmp)
				const test = v.toString().split(",")
				const conclict = key.toString().toLowerCase()
				var string = "'" + test.join("','") + "'"
				if(tmp.OPMOD == 'C') {
					const query = `INSERT INTO rate VALUES(NEXTVAL('rate_id_seq'),${string});`
					const res = await clientPg.query(query)
				}else if(tmp.OPMOD == 'U') {
					const query = `UPDATE rate SET (${conclict})=(${string});`
					const res = await clientPg.query(query)
				}else if(tmp.OPMOD == 'D') {
					const query = `DELETE FROM rate WHERE gdatu = '${time}';`
					const res = await clientPg.query(query)
				}

			}
		}
	})

}); 

consumer.on('error', function (err) { 
console.log('error', err); 
}); 

/* 
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset 
*/ 
consumer.on('offsetOutOfRange', function (topic) { 
topic.maxNum = 2; 
offset.fetch([topic], function (err, offsets) { 
if (err) { 
return console.error(err); 
} 
var min = Math.min.apply(null, offsets[topic.topic][topic.partition]); 
consumer.setOffset(topic.topic, topic.partition, min); 
}); 
});


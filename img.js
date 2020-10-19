
var topic = 'SAP_Outbound_Others';
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
{ topic: topic, partition: 2},
]; 
var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 }; 

var consumer = new kafka.Consumer(client, topics, options); 
var offset = new kafka.Offset(client); 

consumer.on('message', function (message) { 

	parsing(message.value, async function(err, result) {

		const img = result.SAP_XML.IMG
		if(img != null) {
			
			if(img[0].IMG_ITEM[0].OBJNAME == 'T001W') {
				
				for(var i = 0; i < img[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM.length; i ++) {
					const tmp = img[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM[i]
					const plant = tmp.WERKS

					if(tmp.ZMODE == 'U') {
						const key = Object.keys(tmp)
						const v = Object.values(tmp)
						const test = v.toString().split(",")
						const conclict = key.toString().toLowerCase()
						var string = "'" + test.join("','") + "'"
						console.log(key)
						const query =`INSERT INTO t001w VALUES(NEXTVAL('t001w_id_seq'),${string}) ON CONFLICT (werks) DO UPDATE SET (${conclict})= (${string});`
						const res = await clientPg.query(query)	
					}else if(tmp.ZMODE == 'D') {
						const query = `DELETE FROM t001w WHERE werks = '${plant}';`
						const res = await clientPg.query(query)	
					}
				}
			 }
			 
			 else if(img[0].IMG_ITEM[0].OBJNAME == 'T024W') {
				
				for(var i = 0; i < img[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM.length; i ++) {

					const tmp = img[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM[i]
					
					const plant = tmp.WERKS
					const purchasing = tmp.EKORG
					if(tmp.ZMODE == 'U') {
						const key = Object.keys(tmp)
						const v = Object.values(tmp)
						const test = v.toString().split(",")
						const conclict = key.toString().toLowerCase()
						var string = "'" + test.join("','") + "'"	

						const query = `INSERT INTO t024w VALUES (NEXTVAL('t024w_id_seq'),${string}) ON CONFLICT (werks) DO UPDATE SET (${conclict})= (${string});`
						const res = await clientPg.query(query)
					}else if(tmp.ZMODE == 'D') {
						const query = `DELETE FROM t024w WHERE werks = '${plant}';`
						const res = await clientPg.query(query)
					}	
						
				}
			}
			 else if(img[0].IMG_ITEM[0].OBJNAME == 'T024') {
				for(var i = 0; i < img[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM.length; i ++) {
					const tmp = img[0].IMG_ITEM[0].TABLE[0].TABLE_ITEM[i]
					const purchaseGroup = tmp.EKGRP
					const description  = tmp.EKNAM
					if(tmp.ZMODE == 'U') {
						const key = Object.keys(tmp)
						const v = Object.values(tmp)
						const test = v.toString().split(",")
						const conclict = key.toString().toLowerCase()
						var string = "'" + test.join("','") + "'"	
						const query = `INSERT INTO t024 VALUES (NEXTVAL('t024_id_seq'),${string}) ON CONFLICT (ekgrp) DO UPDATE SET (${conclict})= (${string});`
						const res = await clientPg.query(query)
					}else if(tmp.ZMODE == 'D') {
						const query = `DELETE FROM t024 WHERE ekgrp = '${purchaseGroup}';`
						const res = await clientPg.query(query)
					}	
						
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


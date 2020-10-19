var topic = 'wzs.sap.mm.info_record';
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
{ topic: topic, partition: 0},
]; 
var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 }; 

var consumer = new kafka.Consumer(client, topics, options); 
var offset = new kafka.Offset(client);
	
consumer.on('message', function (message) { 

	parsing(message.value, async function(err, result) {

		const info = result.SAP_XML.EINA
		if(info != null) {
			for(var i =0; i < info[0].EINA_ITEM.length; i++) {
				const tmp = info[0].EINA_ITEM[i]
				const key = Object.keys(tmp)
				const v = Object.values(tmp)
				const test = v.toString().split(",")
				const cutLastValue = test.slice(0, test.length-1)
				const cutLastKey = key.slice(0,key.length-1).toString().toLowerCase()
				//const conclict = key.toString().toLowerCase()
				var string = "'" + cutLastValue.join("','") + "'"
				const query = `INSERT INTO	wiprocurement.eina VALUES(${string}) ON CONFLICT(mandt,infnr) DO UPDATE SET (${cutLastKey})=(${string});`
				const res = await clientPg.query(query)
				//console.log(query)
				
				for(var t =0; t <tmp.EINA_EINE[0].EINA_EINE_ITEM.length; t ++) {

					const tmpEine = tmp.EINA_EINE[0].EINA_EINE_ITEM[t]
					const keyEine = Object.keys(tmpEine)
					const vEine = Object.values(tmpEine)
					const testEine = vEine.toString().split(",")
					const cutLastValueEine = testEine.slice(0, testEine.length-1)
					const cutLastKeyEine = keyEine.slice(0,keyEine.length-1).toString().toLowerCase()
					var stringEine = "'" + cutLastValueEine.join("','") + "'"
					const queryEine = `INSERT INTO	wiprocurement.eine VALUES(${stringEine}) ON CONFLICT(mandt,infnr, ekorg, esokz, werks) DO UPDATE SET (${cutLastKeyEine})=(${stringEine});`
				//	console.log(tmpEine.A018_KONP[0].A018_KONP_ITEM)
					const resEine = await clientPg.query(queryEine)
					for(var j =0; j<tmpEine.A018_KONP[0].A018_KONP_ITEM.length;j++) {
						const tmpA018 = tmpEine.A018_KONP[0].A018_KONP_ITEM[i]
						const keyA018 = Object.keys(tmpA018)
						const vA018 = Object.values(tmpA018)
						const testA018 = vA018.toString().split(",")
						const conclict = keyA018.toString().toLowerCase()
						const stringA018 = "'" + testEine.join("','") + "'"
						const queryA018 = `INSERT INTO	wiprocurement.a018_konp VALUES(${stringA018}) ON CONFLICT(mandt,kappl,kschl,lifnr,matnr,ekorg,esokz,datbi,datab,knumh,kopos) DO UPDATE SET (${conclict})=(${stringA018});`
						const resA018 = await clientPg.query(queryA018)
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


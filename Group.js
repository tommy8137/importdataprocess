var async = require('async');
var ConsumerGroup = require('kafka-node').ConsumerGroup;
var kafka = require('kafka-node')
var consumerOptions = {
  kafkaHost: '10.37.36.96:9092',
  groupId: 'ExampleTestGroup',
  sessionTimeout: 25000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest',// Boolean, if true, the consumer will fetch message from the specified offset, otherwise it will fetch message from the last commited offset of the topic.
  autoCommit: false,// true:after fetch message will update offset whether consume suceess or not ;false:if fetch message then update offset by hand if failed,if will not update and this message will consume again 
  fetchMaxWaitMs: 1000,
  fetchMaxBytes: 1024 * 1024,
  outOfRangeOffset: 'earliest'
};
const client = new kafka.KafkaClient()
// const offset = new kafka.Offset(client)

const admin = new kafka.Admin(client)


var topics = ['SAP_Outbound_FICO', 'SAP_Outbound_Others'];


var Consumer1 = new ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions), topics);// new consumer in this consumerGroup(groupId:ExampleTestGroup)
Consumer1.on('error', onError);
Consumer1.on('message', onMessage);



var Consumer2 = new ConsumerGroup(Object.assign({id: 'consumer2'}, consumerOptions), topics);
Consumer2.on('error', onError);
Consumer2.on('message', onMessage);
// Consumer2.on('connect', function () {
//   setTimeout(function () {
//     Consumer2.close(true, function (error) {
//       console.log('consumer2 closed', error);
//     });
//   }, 25000);
  
// });
var Consumer3 = new ConsumerGroup(Object.assign({id: 'consumer3'}, consumerOptions), topics)
Consumer3.on('error', onError)
Consumer3.on('message', onMessage)

admin.describeGroups(['ExampleTestGroup'], (err, res) => { //list consumer group detail
  console.log(JSON.stringify(res, null, 1))
})

function onError (error) {
  console.error(error);
  console.error(error.stack);
}

function onMessage (message) {// handle message
  console.log('%s read msg Topic="%s" Partition=%s Offset=%d Values=%d', this.client.clientId, message.topic, message.partition, message.offset, message.value);
}

process.once('SIGINT', function () {// sync offset of consumer in the same group
  async.each([Consumer1, Consumer2, Consumer3], function (consumer, callback) {
    consumer.close(true, callback);
  });
});
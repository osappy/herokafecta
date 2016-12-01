/**
 * Created by manabu.osada on 2016/11/21.
 */

'use strict';

const kafka = require('no-kafka');

const urls = "kafka+ssl://34.193.11.34:9096,kafka+ssl://34.193.184.236:9096,kafka+ssl://34.193.143.227:9096";
const clientCert = './config/client.crt';
const clientCertKey = './config/client.key';

const GROUP_ID = 'topicviewer_hokko';
const CLIENT_ID = 'topicviewer_consumer_hokko';
const TOPIC = process.argv[2] || 'qml_v1';
const PARTITION = 0;
const IDLE_TIMEOUT = 1000;
const LOGLEVEL = 5;

let consumer = new kafka.SimpleConsumer({
    idleTimeout: IDLE_TIMEOUT,
    groupId: GROUP_ID,
    clientId: CLIENT_ID,
    connectionString: urls.replace(/\+ssl/g, ''),
    ssl: {
        certFile: clientCert,
        keyFile: clientCertKey
    },
    logger: {
        logLevel: LOGLEVEL
    }
});

let dataHandler = (messageSet, topic, partition) => {
    messageSet.forEach((msg, index) => {
        console.log(`[index: ${index}, topic: ${topic}, partition: ${partition}, offset: ${msg.offset}]
${msg.message.value.toString('utf8')}
`);
    });
};

return consumer.init().then(function () {
    return consumer.subscribe(TOPIC, PARTITION, dataHandler);
});

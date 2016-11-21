/**
 * Created by manabu.osada on 2016/11/21.
 */

'use strict';

const kafka = require('no-kafka');
const debug = require('debug');

const urls = "kafka+ssl://52.44.69.117:9096,kafka+ssl://52.202.183.44:9096,kafka+ssl://52.45.180.80:9096";
const clientCert = './config/client.crt';
const clientCertKey = './config/client.key';

const GROUP_ID = 'herokafecta_hokko';
const CLIENT_ID = 'herokafecta_consumer_hokko';
const TOPIC = process.argv[2]　|| 'test';
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
    console.log("sub", new Date().toString());
    messageSet.forEach((msg, index) => {
        console.log(`index: ${index}, topic: ${topic}, partition: ${partition}, offset: ${msg.offset} => ${msg.message.value.toString('utf8')}`);
    });
};

return consumer.init().then(function () {
    return consumer.subscribe(TOPIC, PARTITION, dataHandler);
});

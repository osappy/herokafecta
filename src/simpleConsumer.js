/**
 * Created by manabu.osada on 2016/10/28.
 */

'use strict';

const kafka = require('no-kafka');
const debug = require('debug');

const urls = process.env.KAFKA_URL;
const clientCert = process.env.KAFKA_CLIENT_CERT || './config/client.crt';
const clientCertKey = process.env.KAFKA_CLIENT_CERT_KEY || './config/client.key';

const GROUP_ID = process.env.GROUPID || 'herokafecta01';
const CLIENT_ID = process.env.CLIENTID || 'herokafecta_consumer01';
const TOPIC = process.env.TOPIC || 'defaulttopic';
const PARTITION = process.env.PARTITION || null;
const IDLE_TIMEOUT = process.env.IDLE_TIMEOUT || 1000;
const LOGLEVEL = process.env.LOGLEVEL || 0;

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

// main
let dataHandler = (messageSet, topic, partition) => {
    console.log("sub", new Date().toString());
    messageSet.forEach((msg, index) => {
        console.log(`index: ${index}, topic: ${topic}, partition: ${partition}, offset: ${msg.offset} => ${msg.message.value.toString('utf8')}`);
    });
};

return consumer.init().then(function () {
    return consumer.subscribe(TOPIC, PARTITION, dataHandler);
});

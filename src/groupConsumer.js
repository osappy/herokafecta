/**
 * Created by manabu.osada on 2016/11/17.
 */

const Promise = require('bluebird');

const kafka = require('no-kafka');
const debug = require('debug');

// load config
const urls = process.env.KAFKA_URL;
const clientCert = process.env.KAFKA_CLIENT_CERT || './config/client.crt';
const clientCertKey = process.env.KAFKA_CLIENT_CERT_KEY || './config/client.key';

const GROUP_ID = process.env.GROUPID || 'herokafecta01';
const CLIENT_ID = process.env.CLIENTID || 'herokafecta_consumer01';
const TOPIC = process.env.TOPIC || 'defaulttopic';
const IDLE_TIMEOUT = process.env.IDLE_TIMEOUT || 1000;
const LOGLEVEL = process.env.LOGLEVEL || 0;

let consumer = new kafka.GroupConsumer({
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


let dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, function (m) {
        console.log(`${figureNum(partition)} ${partition}, ${m.offset}`);
        //console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
        // commit offset
        return consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
    });
};

let strategies = [{
    subscriptions: [TOPIC],
    handler: dataHandler
}];

return consumer.init(strategies);


function figureNum(num) {
    let res = "";
    for (let i = 0; i <= num; i++) {

        res += (i % 10 == 0) ? "☆ " : "★ ";
    }
    return res;
}

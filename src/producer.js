/**
 * Created by manabu.osada on 2016/10/28.
 */

'use strict';

const kafka = require('no-kafka');
const debug = require('debug');

const urls = process.env.KAFKA_URL;
const clientCert = process.env.KAFKA_CLIENT_CERT || './config/client.crt';
const clientCertKey = process.env.KAFKA_CLIENT_CERT_KEY || './config/client.key';

const CLIENTID = process.env.CLIENTID || 'herokafecta_consumer01';
const TOPIC = process.env.TOPIC || 'defaulttopic';
let PARTITION = (process.env.PARTITION === '0') ? 0 : Number(process.env.PARTITION) || null;

let producer = new kafka.Producer(
    {
        clientId: CLIENTID,
        connectionString: urls.replace(/\+ssl/g, ''),
        ssl: {
            certFile: clientCert,
            keyFile: clientCertKey
        }
    }
);

console.log(0, Number(0), PARTITION, process.env.PARTITION);

return producer.init().then(() => {
    setInterval(() => {
        return producer.send({
            topic: TOPIC,
            partition: PARTITION,
            message: {
                key : "key property is a string, Buffer, ArrayBuffer, Array, or array-like object.",
                value: "date: " + new Date().toString()
            }
        }).then(function (result) {
            console.log(result);
        })
    }, 500);
});

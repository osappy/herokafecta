/**
 * Created by manabu.osada on 2016/10/28.
 */

'use strict';

const kafka = require('no-kafka');
const debug = require('debug');

const GROUP_ID = process.env.GROUPID || 'herokafecta01';
const urls = process.env.KAFKA_URL;
const clientCert = process.env.KAFKA_CLIENT_CERT || './config/client.crt';
const clientCertKey = process.env.KAFKA_CLIENT_CERT_KEY || './config/client.key';


var admin = new kafka.GroupAdmin({
    idleTimeout: 10000,
    groupId: GROUP_ID,
    connectionString: urls.replace(/\+ssl/g, ''),
    ssl: {
        certFile: clientCert,
        keyFile: clientCertKey
    }
});

return admin.init().then(() => {
    return admin.listGroups().then((groups) => {
        groups.forEach((group, index, array)=> {
            return admin.describeGroup(group.groupId).then((details) => {
                console.log("\nGroupId: ", group.groupId, "\n", details);
            })
        });
    });
});

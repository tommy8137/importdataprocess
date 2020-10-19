// require modules
var fs = require('fs');
var exec = require('child_process').exec;
const moment = require('moment');

promiseFromChildProcess = (child) => {
    return new Promise((resolve, reject) => {
        exec(child, (error, stdout, stderr) => {
            if (error) {
                return reject(error);
            }
            resolve(true);
        });
    });
}

const compression = async () => {
    console.log('enter compression');
    fs.existsSync(`${__dirname}/../back`) || fs.mkdirSync(`${__dirname}/../back`);
    fs.existsSync(`${__dirname}/../mvMessage`) || fs.mkdirSync(`${__dirname}/../mvMessage`);

    var archiver = `back${moment().format('YYYYMMDD-hhmm')}.tar.gz`;
    var messageDataLength = fs.readdirSync(`${__dirname}/../Message`);
    console.log(`MessageData: ${messageDataLength}`);

    if (messageDataLength.length === 0) {
        console.log('Message no Data');
        return;
    }
    
    // 移動Message到mv資料夾
    var mvMessageProcess = 'mv Message/* mvMessage/';
    // 壓縮
    var tarProcess = `tar -czvf ${archiver} mvMessage`;
    // 移動壓縮黨到back    
    var mvArchiverProcess = `mv ${archiver} back/`;
    // 刪掉mv資料夾
    var rmrfProcess = 'rm -rf mvMessage';
    
    try {
        console.log('go exec');
        await promiseFromChildProcess('cd ..');
        await promiseFromChildProcess(mvMessageProcess);
        console.log('move Message finish');
        await promiseFromChildProcess(tarProcess);
        console.log('tar mvMessage finish');
        await promiseFromChildProcess(mvArchiverProcess);
        console.log('move tar finish');
        await promiseFromChildProcess(rmrfProcess);
        console.log('delete mvMessage finish');
        console.log('finish exec');
    } catch (err) {
        console.log(`err: ${err}`);
    }
}

module.exports = compression;

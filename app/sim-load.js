#!/usr/bin/env node
"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const assert = require('assert');
const chalk = require('chalk');
const debug_1 = require("./debug");
const config_generator_1 = require("./config-generator");
const async_1 = require("async");
const child_process_1 = require("child_process");
const _ = require("lodash");
const mongodb_1 = require("mongodb");
let configName = '';
const removeDuplicates = (arr) => arr.reduce((acc, current) => {
    for (const obj of acc) {
        if (_.isEqual(obj, current))
            return acc;
    }
    acc.push(current);
    return acc;
}, []);
const removeDepthOverlap = (arr) => arr.reduce((acc, current) => {
    for (const obj of acc) {
        if (_.isEqual(_.omit(obj, 'depth'), _.omit(current, 'depth'))) {
            obj.depth = Math.max(obj.depth, current.depth);
            return acc;
        }
    }
    acc.push(current);
    return acc;
}, []);
const removeRedundantPDFs = (arr) => arr.reduce((acc, current) => {
    for (const obj of acc) {
        if (_.isEqual(_.omit(obj, 'pdf'), _.omit(current, 'pdf'))) {
            return acc;
        }
    }
    acc.push(current);
    return acc;
}, []);
const start = (configGenerator, simDb) => __awaiter(this, void 0, void 0, function* () {
    const startProcessTime = process.hrtime();
    try {
        const generator = yield configGenerator.getGenerator();
        const tasks = [];
        let loadConfigs = [];
        while (true) {
            const { value: next, done } = generator.next();
            if (done)
                break;
            loadConfigs.push({
                exchange: next.exchange,
                market: next.market,
                depth: next.depth,
                timeFrame: next.timeFrame,
            });
        }
        let taskObjs = [];
        yield simDb.collection('loads').deleteMany({});
        for (const loadConfig of loadConfigs) {
            const taskObj = Object.assign({
                envId: configGenerator.config.envId,
                trim: configGenerator.config.trim,
                onlyOrderbooksWithTrades: configGenerator.config.onlyOrderbooksWithTrades,
                saveRedis: configGenerator.config.saveRedis
            }, loadConfig);
            taskObjs.push(taskObj);
        }
        debug_1.debug({ taskObjs });
        taskObjs = removeDuplicates(taskObjs);
        debug_1.debug({ taskObjs });
        taskObjs = removeDepthOverlap(taskObjs);
        debug_1.debug({ taskObjs });
        taskObjs = removeRedundantPDFs(taskObjs);
        debug_1.debug({ taskObjs });
        for (const taskObj of taskObjs) {
            const { insertedId: taskId } = yield simDb.collection('loads').insertOne(taskObj);
            tasks.push((callback) => {
                try {
                    console.log(chalk.blue(`Load (${configName}) ${taskId} Activated`));
                    child_process_1.exec(`load.py ${taskId}`, (err, stdout, stderr) => {
                        callback(err, stderr);
                    });
                }
                catch (err) {
                    throw err;
                }
            });
        }
        async_1.parallelLimit(tasks, configGenerator.config.parallelSimulations, (err, stdoutArray) => __awaiter(this, void 0, void 0, function* () {
            try {
                if (err) {
                    console.log({ err });
                    return Promise.reject(err);
                }
                if (stdoutArray) {
                    for (const str of stdoutArray) {
                        process.stdout.write(str);
                        console.log(chalk.green(`Load Complete`));
                    }
                }
                const [executionSeconds] = process.hrtime(startProcessTime);
                const hours = Math.floor(executionSeconds / 3600);
                const totalSeconds = executionSeconds % 3600;
                const minutes = Math.floor(totalSeconds / 60);
                const seconds = totalSeconds % 60;
                const elapsedTime = String(hours).padStart(2, '0') + ':' +
                    String(minutes).padStart(2, '0') + ':' +
                    String(seconds).padStart(2, '0');
                console.log(chalk.green(`Load Execution Time (hh:mm:ss): ${elapsedTime}`));
                process.exit(0);
                return;
            }
            catch (err) {
                return Promise.reject(err);
            }
        }));
    }
    catch (err) {
        return Promise.reject(err);
    }
});
const copyPDFs = (pdfsRemote, pdfsLocal) => __awaiter(this, void 0, void 0, function* () {
    try {
        yield pdfsLocal.deleteMany({});
        pdfsRemote.find({}).forEach((pdf) => __awaiter(this, void 0, void 0, function* () {
            yield pdfsLocal.insertOne(pdf);
        }), (err) => {
            return Promise.reject(err);
        });
    }
    catch (err) {
        return Promise.reject(err);
    }
});
(() => __awaiter(this, void 0, void 0, function* () {
    try {
        configName = process.argv[2];
        assert(process.env.MONGODB, 'MONGODB Not Defined');
        assert(process.env.SIMULATOR_DB, 'SIMULATOR_DB Not Defined');
        const mongoRemote = yield mongodb_1.MongoClient.connect(process.env.MONGODB, { useNewUrlParser: true });
        assert(process.env.LOCALDB, 'LOCALDB Not Defined');
        const mongoLocal = yield mongodb_1.MongoClient.connect(process.env.LOCALDB, { useNewUrlParser: true });
        const remoteSimDb = mongoRemote.db(process.env.SIMULATOR_DB);
        const localSimDb = mongoLocal.db("sim");
        yield copyPDFs(remoteSimDb.collection("PDFs"), localSimDb.collection("PDFs"));
        const configGenerator = new config_generator_1.ConfigGenerator(configName, remoteSimDb);
        yield start(configGenerator, remoteSimDb);
    }
    catch (err) {
        console.log(err);
        process.exit(1);
    }
}))();

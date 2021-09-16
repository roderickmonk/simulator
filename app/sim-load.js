#!/usr/bin/env node
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
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
const _ = __importStar(require("lodash"));
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
const start = (configGenerator, simDb) => __awaiter(void 0, void 0, void 0, function* () {
    const startProcessTime = process.hrtime();
    try {
        const generator = yield configGenerator.getGenerator();
        const tasks = [];
        let loadConfigs = [];
        while (true) {
            const { value: next, done } = generator.next();
            if (done)
                break;
            (0, debug_1.debug)({ next });
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
        (0, debug_1.debug)({ taskObjs });
        taskObjs = removeDuplicates(taskObjs);
        (0, debug_1.debug)({ taskObjs });
        taskObjs = removeDepthOverlap(taskObjs);
        (0, debug_1.debug)({ taskObjs });
        taskObjs = removeRedundantPDFs(taskObjs);
        (0, debug_1.debug)({ taskObjs });
        for (const taskObj of taskObjs) {
            const { insertedId: taskId } = yield simDb.collection('loads').insertOne(taskObj);
            tasks.push((callback) => {
                try {
                    console.log(chalk.blue(`Load (${configName}) ${taskId} Activated`));
                    (0, child_process_1.exec)(`load.py ${taskId}`, (err, stdout, stderr) => {
                        callback(err, stderr);
                    });
                }
                catch (err) {
                    throw err;
                }
            });
        }
        (0, async_1.parallelLimit)(tasks, configGenerator.config.parallelSimulations, (err, stdoutArray) => __awaiter(void 0, void 0, void 0, function* () {
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
const copyTunings = (tuningsRemote, tuningsLocal) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        yield tuningsLocal.deleteMany({});
        tuningsRemote.find({}).forEach((pdf) => __awaiter(void 0, void 0, void 0, function* () {
            yield tuningsLocal.insertOne(pdf);
        }), (err) => {
            return Promise.reject(err);
        });
    }
    catch (err) {
        return Promise.reject(err);
    }
});
(() => __awaiter(void 0, void 0, void 0, function* () {
    try {
        configName = process.argv[2];
        console.log({ argv: process.argv });
        assert(process.env.MONGODB, 'MONGODB Not Defined');
        assert(process.env.SIMULATOR_DB, 'SIMULATOR_DB Not Defined');
        const mongoConnectOptions = {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        };
        console.log(`Connecting to remote: ${process.env.MONGODB}`);
        const mongoRemote = yield mongodb_1.MongoClient.connect(process.env.MONGODB, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        console.log("Connected remote");
        const simConfigDb = mongoRemote.db("sim_configuration");
        const simDb = mongoRemote.db(process.env.SIMULATOR_DB);
        console.log(`here-1: ${configName}`);
        const configGenerator = new config_generator_1.ConfigGenerator(configName, simConfigDb);
        console.log("here-3");
        yield start(configGenerator, simDb);
    }
    catch (err) {
        console.log(err);
        process.exit(1);
    }
}))();

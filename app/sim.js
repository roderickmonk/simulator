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
const async_1 = require("async");
const config_generator_1 = require("./config-generator");
const debug_1 = require("./debug");
const child_process_1 = require("child_process");
const mongo_1 = require("./mongo");
const mongodb_1 = require("mongodb");
let configName = "";
const start = (configName, simConfig) => __awaiter(this, void 0, void 0, function* () {
    const startProcessTime = process.hrtime();
    const startTime = new Date();
    try {
        const generator = yield simConfig.getGenerator();
        const tasks = [];
        const runId = new mongodb_1.ObjectId();
        while (true) {
            const { value: nextSimConfig, done } = generator.next();
            if (done)
                break;
            const taskObj = Object.assign({
                runId,
                simVersion: process.env.SIM_VERSION,
                configName,
                ts: new Date(),
                status: "STARTED",
                envId: simConfig.config.envId,
                minNotional: simConfig.config.minNotional ? simConfig.config.minNotional : 0.0005,
                trim: simConfig.config.trim,
            }, nextSimConfig);
            debug_1.debug('taskObj:\n', JSON.stringify(taskObj, null, 4));
            const { insertedId: simId } = yield mongo_1.Mongo.simDb.collection('simulations')
                .insertOne(taskObj);
            tasks.push((callback) => {
                try {
                    console.log(chalk.blue(`Simulation (${configName}) ${simId} Activated`));
                    child_process_1.exec(`simulator ${simId}`, (err, stdout, stderr) => {
                        callback(err, stderr);
                    });
                }
                catch (err) {
                    throw err;
                }
            });
        }
        assert(simConfig.config, "simConfig.config Not Defined");
        async_1.parallelLimit(tasks, simConfig.config.parallelSimulations, (err, stdoutArray) => __awaiter(this, void 0, void 0, function* () {
            try {
                if (err) {
                    console.log({ err });
                }
                if (stdoutArray) {
                    for (const str of stdoutArray) {
                        process.stdout.write(str);
                        console.log(chalk.green(`Simulation Complete`));
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
                console.log(chalk.green(`Simulation Run ${runId} Execution Time (hh:mm:ss): ${elapsedTime}`));
                const runObj = {
                    runId,
                    simVersion: process.env.SIM_VERSION,
                    configName,
                    startTime,
                    endTime: new Date(),
                    elapsedTime,
                };
                yield mongo_1.Mongo.simDb.collection('runs').insertOne(runObj);
                process.exit(0);
            }
            catch (err) {
                console.log(err);
                process.exit(1);
            }
        }));
    }
    catch (err) {
        console.log(err);
        process.exit(1);
    }
    finally {
    }
});
(() => __awaiter(this, void 0, void 0, function* () {
    try {
        configName = process.argv[2];
        const simConfig = new config_generator_1.ConfigGenerator(configName);
        yield start(configName, simConfig);
    }
    catch (err) {
        console.log(err);
        process.exit(1);
    }
    finally {
    }
}))();

#!/usr/bin/env node

const assert = require('assert');
const chalk = require('chalk');
import { debug } from "./debug";
import { ConfigGenerator } from "./config-generator"
import { Mongo } from "./mongo";
import { parallelLimit } from 'async';
import { exec } from 'child_process';
import * as _ from "lodash";

import {
    Collection,
    Db,
    MongoClient,
    MongoError,
    ObjectID,
    ObjectId,
} from "mongodb";

let configName = '';

const removeDuplicates = (arr: object[]) =>

    arr.reduce((acc: Array<any>, current: any) => {

        for (const obj of acc) {
            if (_.isEqual(obj, current)) return acc;
        }
        acc.push(current);
        return acc;

    }, []);

const removeDepthOverlap = (arr: object[]) =>

    arr.reduce((acc: Array<any>, current: any) => {

        for (const obj of acc) {
            if (_.isEqual(_.omit(obj, 'depth'), _.omit(current, 'depth'))) {

                obj.depth = Math.max(obj.depth, current.depth);
                return acc;
            }
        }
        acc.push(current);
        return acc;

    }, []);

const removeRedundantPDFs = (arr: object[]) =>

    arr.reduce((acc: Array<any>, current: any) => {

        for (const obj of acc) {
            if (_.isEqual(_.omit(obj, 'pdf'), _.omit(current, 'pdf'))) {
                return acc;
            }
        }
        acc.push(current);
        return acc;
    }, []);

const start = async (
    simConfig: ConfigGenerator,
    simDb: Db)
    : Promise<void> => {

    const startProcessTime = process.hrtime();

    try {

        const generator = await simConfig.getGenerator();

        const tasks = [];

        // runId links all output documents
        const runId = new ObjectId();

        let loadConfigs: object[] = [];

        while (true) {

            const { value: next, done } = generator.next();

            if (done) break;

            loadConfigs.push({
                exchange: next.exchange,
                market: next.market,
                // pdf: next.pdf,
                depth: next.depth,
                timeFrame: next.timeFrame,
            });
        }

        let taskObjs: object[] = [];

        // Clear out from previous load
        await simDb.collection('loads').deleteMany({});

        console.log (JSON.stringify(loadConfigs, null,4));

        for (const loadConfig of loadConfigs) {

            const taskObj = {
                ...{
                    envId: simConfig.config!.envId,
                    trim: simConfig.config!.trim,
                }, ...loadConfig
            }

            taskObjs.push(taskObj);
        }

        debug({ taskObjs });
        taskObjs = removeDuplicates(taskObjs) as object[];
        debug({ taskObjs });
        taskObjs = removeDepthOverlap(taskObjs) as object[];
        debug({ taskObjs });
        taskObjs = removeRedundantPDFs(taskObjs) as object[];
        debug({ taskObjs });

        for (const taskObj of taskObjs) {

            const { insertedId: taskId } =
                await simDb.collection('loads')
                    .insertOne(taskObj);

            // Accumulate all tasks into one structure
            tasks.push(

                (callback: any) => {

                    try {

                        console.log(chalk.blue(`Load (${configName}) ${taskId} Activated`));

                        exec(
                            `load.py ${taskId}`,

                            (err: Error, stdout: string, stderr: string) => {

                                callback(err, stderr);
                            });

                    } catch (err) {
                        throw err;
                    }
                }
            );
        }

        // Now carry out the tasks, parallelizing according 
        // to the extent permitted by the configuration.
        parallelLimit(

            tasks,
            simConfig.config!.parallelSimulations,
            async (err, stdoutArray: undefined | Array<string>) => {

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

                    const elapsedTime =
                        String(hours).padStart(2, '0') + ':' +
                        String(minutes).padStart(2, '0') + ':' +
                        String(seconds).padStart(2, '0');

                    console.log(chalk.green(
                        `Load Execution Time (hh:mm:ss): ${elapsedTime}`
                    ));

                    process.exit(0);
                    return;

                } catch (err) {
                    return Promise.reject(err);
                }
            });

    } catch (err) {

        console.log(err);
        return Promise.reject(err);
    }
}

const copyPDFs = async (
    pdfsRemote: Collection,
    pdfsLocal: Collection,
): Promise<void> => {

    try {
        // Clear out all local PDFs
        await pdfsLocal.deleteMany({});

        pdfsRemote.find({}).forEach(async (pdf) => {
            await pdfsLocal.insertOne(pdf);
        }, (err) => {
            return Promise.reject(err);
        });

    } catch (err) {
        return Promise.reject(err);
    }
}

(async () => {

    try {

        configName = process.argv[2];

        assert(process.env.MONGODB, 'MONGODB Not Defined');
        assert(process.env.SIMULATOR_DB, 'SIMULATOR_DB Not Defined');

        const mongoRemote =
            await MongoClient.connect(
                process.env.MONGODB!,
                { useNewUrlParser: true },
            );

        const mongoLocal =
            await MongoClient.connect(
                "mongodb://127.0.0.1:27017",
                { useNewUrlParser: true },
            );

        const remoteSimDb: Db = mongoRemote.db(process.env.SIMULATOR_DB);
        const localSimDb: Db = mongoLocal.db("sim");

        await copyPDFs(
            remoteSimDb.collection("PDFs"),
            localSimDb.collection("PDFs")
        );

        const simConfig = new ConfigGenerator(configName, remoteSimDb);
        await start(simConfig, remoteSimDb);

    } catch (err) {

        console.log(err);
        process.exit(1);
    }
})();

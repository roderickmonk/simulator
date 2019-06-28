#!/usr/bin/env node

const assert = require('assert');
const chalk = require('chalk');
import { parallelLimit } from 'async';
import { ConfigGenerator } from "./config-generator"
import { simParams } from "./sim-params"
import { debug } from "./debug";
import { exec, } from 'child_process';

import {
    Collection,
    Db,
    MongoClient,
    MongoError,
    ObjectID,
    ObjectId,
} from "mongodb";

let configName = "";

const start = async (
    simConfig: ConfigGenerator,
    simDb: Db)
    : Promise<void> => {

    const startProcessTime = process.hrtime();
    const startTime = new Date();

    try {

        const generator = await simConfig.getGenerator();

        const tasks = [];

        // runId links all output documents
        const runId = new ObjectId();

        while (true) {

            const { value: nextSimConfig, done } = generator.next();

            if (done) break;

            // Bundle up an object to be recorded to the db
            const taskObj = {
                ...{
                    runId,
                    simVersion: process.env.SIM_VERSION,
                    configName,
                    ts: new Date(),
                    status: "STARTED",
                    envId: simConfig.config!.envId,
                    minNotional: simConfig.config!.minNotional ? simConfig.config!.minNotional : 0.0005,
                    trim: simConfig.config!.trim,
                    saveRedis: simConfig.config!.saveRedis,
                }, ...nextSimConfig
            }

            debug('taskObj:\n', JSON.stringify(taskObj, null, 4));

            const { insertedId: simId } =
                await simDb.collection('simulations')
                    .insertOne(taskObj);

            // Accumulate all tasks into one structure
            tasks.push(

                (callback: any) => {

                    try {

                        console.log(chalk.blue(`Simulation (${configName}) ${simId} Activated`));

                        exec(
                            `simulator ${simId}`,

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

        assert (simConfig.config, "simConfig.config Not Defined");

        parallelLimit(

            tasks,
            simConfig.config!.parallelSimulations,
            async (err, stdoutArray: undefined | Array<string>) => {

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

                    const elapsedTime =
                        String(hours).padStart(2, '0') + ':' +
                        String(minutes).padStart(2, '0') + ':' +
                        String(seconds).padStart(2, '0');

                    console.log(chalk.green(
                        `Simulation Run ${runId} Execution Time (hh:mm:ss): ${elapsedTime}`
                    ));

                    const runObj = {
                        runId,
                        simVersion: process.env.SIM_VERSION,
                        configName,
                        startTime,
                        endTime: new Date(),
                        elapsedTime,
                    }

                    await simDb.collection('runs').insertOne(runObj);

                    process.exit(0);

                } catch (err) {
                    console.log(err);
                    process.exit(1);
                }
            });

    } catch (err) {

        console.log(err);
        process.exit(1);

    } finally {

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

        const remoteSimDb: Db = mongoRemote.db(process.env.SIMULATOR_DB);

        const simConfig = new ConfigGenerator(configName, remoteSimDb);

        // await start(configName, simConfig);

        await start(simConfig, remoteSimDb);

    } catch (err) {

        console.log(err);
        process.exit(1);

    } finally {

    }
})();

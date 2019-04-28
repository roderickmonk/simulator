#!/usr/bin/env node

const assert = require('assert');
const chalk = require('chalk');
import { debug } from "../debug";
import { exec, spawn } from 'child_process';
import { parallelLimit } from 'async';
import * as moment from 'moment';
const { promisify } = require("util");
const setTimeoutPromise = promisify(setTimeout);

import {
    Collection,
    Db,
    MongoClient,
    MongoError,
    ObjectID,
    ObjectId,
} from "mongodb";

const GenerateSchema = require('generate-schema')

const simParams = {

    actualFeeRate: {
        type: "array",
        items: {
            type: "number",
        }
    },
    exchange: {
        type: "array",
        items: {
            type: "string"
        },
    },
    depth: {
        type: "array",
        items: {
            type: "number",
        }
    },
    feeRate: {
        type: "array",
        items: {
            type: "number",
        }
    },
    inventoryLimit: {
        type: "array",
        items: {
            type: "number",
        }
    },
    market: {
        type: "array",
        items: {
            type: "string",
        }
    },
    partitions: {
        type: "array",
        items: {
            type: "number",
        }
    },
    pdf: {
        type: "array",
        items: {
            type: "string",
        }
    },
    quantityLimit: {
        type: "array",
        items: {
            type: "number",
        }
    },
    timeFrame: {
        type: "array",
        items: {
            type: "object",
            properties: {
                startTime: {
                    type: "string",

                },
                endTime: {
                    type: "string",
                }
            }
        }
    },
    tick: {
        type: "array",
        items: {
            type: "number",
        }
    },
    trader: {
        type: "array",
        items: {
            type: "string",
        }
    },
}

const propertyLength = new Map();
const propertyLevel = new Map();
const propertyData = new Map();
const propertyType = new Map();
const propertySchema = new Map();

const validateSimConfig = (simConfig: Array<Array<object> | object>) => {

    // Capture Schema Output
    let schema: any = GenerateSchema.json('Product', simConfig).items.oneOf;
    schema.shift(); // Skip first one

    let properties: string[] = [];
    for (const level of schema) {

        debug('level:\n', JSON.stringify(level, null, 4));

        for (const prop of Object.keys(level.properties)) {

            properties.push(prop);

            propertySchema.set(prop, level.properties[prop]);
        }
    }

    for (const [prop, entry] of propertySchema.entries()) {

        assert(entry.type === 'array', `Parameter "${prop}" Data Not Array`);

        if (simParams.hasOwnProperty(prop)) {

            // This test only applies if it is a known parameter
            assert(
                //@ts-ignore
                simParams[prop].items.type === entry.items.type,
                `Parameter "${prop}" Wrong Type`
            );
        }

        // Ensure required params 
        Object.keys(simParams).forEach(property => {

            assert(
                propertySchema.has(property),
                `Required Parameter "${property}" Not Found`
            );
        })
    }

    assert(
        (new Set(properties)).size === properties.length,
        'Duplicate Parameters Detected',
    );

    for (const [i, config] of simConfig.entries()) {

        const levelConfig = i === 0 ?
            //@ts-ignore
            config["0"] :
            config;

        debug({ levelConfig });

        for (const prop in levelConfig) {

            debug(chalk.red(`${prop}[${levelConfig[prop]}]`));

            assert(
                levelConfig[prop].length > 0,
                'Empty Configuration Parameter Array'
            );
            propertyLength.set(prop, levelConfig[prop].length);
            propertyData.set(prop, levelConfig[prop]);

            propertyLevel.set(prop, i);
        }
    }

    debug(propertyData);
    debug(propertyLevel);
    debug(propertyLength);

    const levels = simConfig.length;
    debug({ levels });

    for (let i = 0; i < simConfig.length; ++i) {

        const levelProperties = Array.from(propertyLevel)
            .filter(level => level[1] === i)
            .map(level => level[0]);

        debug({ levelProperties });

        const levelPropertiesSameLength =
            levelProperties
                .map(prop => propertyLength.get(prop))
                .every((val, i, arr) => val === arr[0]);


        assert(
            levelPropertiesSameLength,
            'Mismatched Parameter Array Lengths'
        );
    }

    debug('levels: ', schema.length);

}

function* generatorSimParameters(level: number): any {

    const levelProperties: Array<string> = Array.from(propertyLevel)
        .filter(l => l[1] === level)
        .map(l => l[0]);

    if (levelProperties.length === 0) {

        yield {};

    } else {

        const depth = propertyData.get(levelProperties[0]).length;

        for (let i = 0; i < depth; ++i) {

            const returnObj = {};

            for (const prop of levelProperties) {

                //@ts-ignore
                returnObj[prop] = propertyData.get(prop)[i];
            }

            const genLoop = generatorSimParameters(level + 1);

            while (true) {

                const next = genLoop.next();

                if (next.done) break;

                yield { ...returnObj, ...next.value };
            }
        }
    }
}

interface IHistoryObject {
    mongoClient: any;
    exchange: string;
    market: string;
    startTime: any;
    endTime: any;
}

const orderbookTradesMap = new Map();

const startTime = new Date("2018-09-15");
const endTime = new Date("2018-09-15T00:01:30");

const getTrades = async (params: IHistoryObject) => {

    try {

        const filter = {
            e: 0,
            x: params.exchange,
            m: params.market,
            ts: {
                $gte: startTime,
                $lte: endTime,
            }
        }

        const tradesCount = await params.mongoClient.db('history').collection('trades').count(filter);

        console.log({ tradesCount });

        const cursor = params.mongoClient.db('history').collection('trades').find(filter);

        await cursor.forEach((trade: any) => {

            const arr = orderbookTradesMap.has(String(trade.ob)) ?
                orderbookTradesMap.get(String(trade.ob)) :
                [];

            arr.push(trade);

            console.log('Trade', trade.ob, trade.ts);

            orderbookTradesMap.set(String(trade.ob), arr);
        });

        console.log('orderbookTradesMap.size: ', orderbookTradesMap.size);

    } catch (err) {

        console.log({ err });
        process.exit(1);
    }
}

/*
def get_start_snapshot(
    envId: int,
    exchange: str,
    market: str,
    start: datetime,
    orderbooks_collection
):

    earlier_snapshots = list(orderbooks_collection.find(
        filter={
            "e": envId,
            "x": exchange,
            "m": market,
            # Look for a snapshot within the last 3 hours
            "ts": {
                "$lte": start,
                "$gte": start - timedelta(milliseconds=3*3600000)
            },
            "s": True,
        }))

    if len(earlier_snapshots) > 0:

        # Work with the most recent one
        start_snapshot = reduce(
            lambda x, y: x if x['ts'] > y['ts'] else y,
            earlier_snapshots,
        )

    else:

        # Nothing earlier; look for the next snapshot
        snapshots = list(
            orderbooks_collection.find(
                filter={
                    "e": envId,
                    "x": exchange,
                    "m": market,
                    "ts": {
                        "$gte": start,
                    },
                    "s": True,
                }).sort("ts", 1).limit(1)
        )
        if len(snapshots) == 0:
            raise StopIteration
        else:
            start_snapshot = snapshots[0]

    return start_snapshot
*/


const getOrderbooks = async (params: IHistoryObject) => {

    try {
        const cursor = params.mongoClient.db('history').collection('orderbooks').find({
            e: 0,
            x: params.exchange,
            m: params.market,
            ts: {
                $gte: startTime,
                $lte: endTime,
            }
        });

        await cursor.forEach(async (orderbook: any) => {

            try {
                if (orderbookTradesMap.has(String(orderbook._id))) {

                    orderbook.trades = orderbookTradesMap.get(String(orderbook._id));
                    console.log('Merged   : ', orderbook);

                    await params.mongoClient.db('sim_dev').collection('orderbooks').insertOne(orderbook);

                }
            } catch (err) {
                
                console.log({ err });
            }
        })

    } catch (err) {

        console.log(err);
    }
}


(async () => {

    const startProcessTime = process.hrtime();

    const startTime = new Date();

    try {

        assert(process.env.MONGODB, 'MONGODB Not Defined');
        // assert(process.env.LOCALDB, 'LOCALDB Not Defined');
        assert(process.env.SIMULATOR_DB, 'SIMULATOR_DB Not Defined');

        const sim_db = process.env.SIMULATOR_DB;

        // runId links all output documents
        const runId = new ObjectId();

        const mainMongoClient =
            await MongoClient.connect(
                process.env.MONGODB!,
                { useNewUrlParser: true },
            );
        const simDb = mainMongoClient.db(sim_db);

        const configName = process.argv[2];

        const config =
            await simDb.collection('configurations').findOne({ name: configName });

        assert(config, 'Unknown Configuration');

        debug({ config });

        validateSimConfig(config.simConfig);

        const trades = await getTrades({
            mongoClient: mainMongoClient,
            exchange: 'bittrex',
            market: 'btc-eth',
            startTime: moment("2018-10-01"),
            endTime: moment("2018-10-04"),
        });

        const orderbooks = await getOrderbooks({
            mongoClient: mainMongoClient,
            exchange: 'bittrex',
            market: 'btc-eth',
            startTime: moment("2018-10-01"),
            endTime: moment("2018-10-04"),
        });

        process.exit(1);

        const genLoop = generatorSimParameters(0);

        const simulations = [];

        while (true) {

            const { value: nextSimConfig, done } = genLoop.next();

            if (done) break;

            // Bundle up an object to be recorded to the db
            const simulationObj = {
                ...{
                    runId,
                    simVersion: process.env.SIM_VERSION,
                    configName,
                    ts: new Date(),
                    status: "STARTED",
                    envId: config.envId,
                    // minNotional defaults to 0.0005
                    minNotional: config.minNotional ? config.minNotional : 0.0005,
                }, ...nextSimConfig
            }

            debug(JSON.stringify(simulationObj, null, 4));

            const { insertedId: sim_id } =
                await simDb.collection('simulations')
                    .insertOne(simulationObj);

            // Accumulate all simulations into one structure
            simulations.push(

                (callback: any) => {

                    try {

                        console.log(`Simulation ${sim_id} Activated`);

                        exec(
                            `simulator.py ${sim_id}`,

                            (err: Error, stdout: string, stderr: string) => {

                                callback(err, stderr);
                            });

                    } catch (err) {
                        throw err;
                    }
                }
            );
        }

        // Now carry out the simulations, parallelizing according 
        // to the extent permitted by the configuration.

        parallelLimit(

            simulations,
            config.parallelSimulations,
            async (err, stdoutArray: undefined | Array<string>) => {

                try {

                    if (err) {
                        console.log({ err });
                    }

                    if (stdoutArray) {

                        for (const str of stdoutArray) {

                            console.log(str);
                        }
                    }

                    const [executionSeconds] = process.hrtime(startProcessTime);

                    const hours = Math.floor(executionSeconds / 3600);

                    const totalSeconds = executionSeconds % 3600;
                    const minutes = Math.floor(totalSeconds / 60);
                    const seconds = totalSeconds % 60;

                    const elapsedTime =
                        ('' + hours).padStart(2, '0') + ':' +
                        ('' + minutes).padStart(2, '0') + ':' +
                        ('' + seconds).padStart(2, '0');

                    console.log(
                        `Simulation ${runId} Execution Time (hh:mm:ss): ${elapsedTime}`
                    );

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
})();

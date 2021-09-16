#!/usr/bin/env node
"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const assert_1 = __importDefault(require("assert"));
const chalk = require('chalk');
const debug_1 = require("../debug");
const async_1 = require("async");
const moment_1 = __importDefault(require("moment"));
const { promisify } = require("util");
const setTimeoutPromise = promisify(setTimeout);
const mongodb_1 = require("mongodb");
const GenerateSchema = require('generate-schema');
const multiplyConfigParams = {
    actualFeeRate: {
        type: "array",
        items: {
            type: "number",
        }
    },
    allowOrderConflicts: {
        type: "array",
        items: {
            type: "boolean",
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
};
const propertyLength = new Map();
const propertyLevel = new Map();
const propertyData = new Map();
const propertyType = new Map();
const propertySchema = new Map();
const validateMultiplyConfig = (simConfig) => {
    let schema = GenerateSchema.json('Product', simConfig).items.oneOf;
    schema.shift();
    let properties = [];
    for (const level of schema) {
        (0, debug_1.debug)('level:\n', JSON.stringify(level, null, 4));
        for (const prop of Object.keys(level.properties)) {
            properties.push(prop);
            propertySchema.set(prop, level.properties[prop]);
        }
    }
    for (const [prop, entry] of propertySchema.entries()) {
        (0, assert_1.default)(entry.type === 'array', `Parameter "${prop}" Data Not Array`);
        if (multiplyConfigParams.hasOwnProperty(prop)) {
            (0, assert_1.default)(multiplyConfigParams[prop].items.type === entry.items.type, `Parameter "${prop}" Wrong Type`);
        }
        Object.keys(multiplyConfigParams).forEach(property => {
            (0, assert_1.default)(propertySchema.has(property), `Required Parameter "${property}" Not Found`);
        });
    }
    (0, assert_1.default)((new Set(properties)).size === properties.length, 'Duplicate Parameters Detected');
    for (const [i, config] of simConfig.entries()) {
        const levelConfig = i === 0 ?
            config["0"] :
            config;
        (0, debug_1.debug)({ levelConfig });
        for (const prop in levelConfig) {
            (0, debug_1.debug)(chalk.red(`${prop}[${levelConfig[prop]}]`));
            (0, assert_1.default)(levelConfig[prop].length > 0, 'Empty Configuration Parameter Array');
            propertyLength.set(prop, levelConfig[prop].length);
            propertyData.set(prop, levelConfig[prop]);
            propertyLevel.set(prop, i);
        }
    }
    (0, debug_1.debug)(propertyData);
    (0, debug_1.debug)(propertyLevel);
    (0, debug_1.debug)(propertyLength);
    const levels = simConfig.length;
    (0, debug_1.debug)({ levels });
    for (let i = 0; i < simConfig.length; ++i) {
        const levelProperties = Array.from(propertyLevel)
            .filter(level => level[1] === i)
            .map(level => level[0]);
        (0, debug_1.debug)({ levelProperties });
        const levelPropertiesSameLength = levelProperties
            .map(prop => propertyLength.get(prop))
            .every((val, i, arr) => val === arr[0]);
        (0, assert_1.default)(levelPropertiesSameLength, 'Mismatched Parameter Array Lengths');
    }
    (0, debug_1.debug)('levels: ', schema.length);
};
function* generatorSimParameters(level) {
    const levelProperties = Array.from(propertyLevel)
        .filter(l => l[1] === level)
        .map(l => l[0]);
    if (levelProperties.length === 0) {
        yield {};
    }
    else {
        const depth = propertyData.get(levelProperties[0]).length;
        for (let i = 0; i < depth; ++i) {
            const returnObj = {};
            for (const prop of levelProperties) {
                returnObj[prop] = propertyData.get(prop)[i];
            }
            const genLoop = generatorSimParameters(level + 1);
            while (true) {
                const next = genLoop.next();
                if (next.done)
                    break;
                yield Object.assign(Object.assign({}, returnObj), next.value);
            }
        }
    }
}
const orderbookTradesMap = new Map();
const startTime = new Date("2018-09-15");
const endTime = new Date("2018-09-15T00:01:30");
const getTrades = (params) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const filter = {
            e: 0,
            x: params.exchange,
            m: params.market,
            ts: {
                $gte: startTime,
                $lte: endTime,
            }
        };
        console.log("here-0");
        const tradesCount = yield params.mongoClient.db('history').collection('trades').count(filter);
        console.log({ tradesCount });
        const cursor = params.mongoClient.db('history').collection('trades').find(filter);
        yield cursor.forEach((trade) => {
            const arr = orderbookTradesMap.has(String(trade.ob)) ?
                orderbookTradesMap.get(String(trade.ob)) :
                [];
            arr.push(trade);
            console.log('Trade', trade.ob, trade.ts);
            orderbookTradesMap.set(String(trade.ob), arr);
        });
        console.log('orderbookTradesMap.size: ', orderbookTradesMap.size);
    }
    catch (err) {
        console.log({ err });
        process.exit(1);
    }
});
const getOrderbooks = (params) => __awaiter(void 0, void 0, void 0, function* () {
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
        yield cursor.forEach((orderbook) => __awaiter(void 0, void 0, void 0, function* () {
            try {
                if (orderbookTradesMap.has(String(orderbook._id))) {
                    orderbook.trades = orderbookTradesMap.get(String(orderbook._id));
                    console.log('Merged   : ', orderbook);
                    yield params.mongoClient.db('sim_dev').collection('orderbooks').insertOne(orderbook);
                }
            }
            catch (err) {
                console.log({ err });
            }
        }));
    }
    catch (err) {
        console.log(err);
    }
});
(() => __awaiter(void 0, void 0, void 0, function* () {
    const startProcessTime = process.hrtime();
    const startTime = new Date();
    try {
        (0, assert_1.default)(process.env.MONGODB, 'MONGODB Not Defined');
        (0, assert_1.default)(process.env.SIMULATOR_DB, 'SIMULATOR_DB Not Defined');
        const sim_db = process.env.SIMULATOR_DB;
        const runId = new mongodb_1.ObjectId();
        const mainMongoClient = yield mongodb_1.MongoClient.connect(process.env.MONGODB);
        const simDb = mainMongoClient.db(sim_db);
        const configName = process.argv[2];
        const config = yield simDb.collection('configurations').findOne({ name: configName });
        (0, assert_1.default)(config, 'Unknown Configuration');
        (0, debug_1.debug)({ config });
        validateMultiplyConfig(config.simConfig);
        const trades = yield getTrades({
            mongoClient: mainMongoClient,
            exchange: 'bittrex',
            market: 'btc-eth',
            startTime: (0, moment_1.default)("2018-10-01"),
            endTime: (0, moment_1.default)("2018-10-04"),
        });
        const orderbooks = yield getOrderbooks({
            mongoClient: mainMongoClient,
            exchange: 'bittrex',
            market: 'btc-eth',
            startTime: (0, moment_1.default)("2018-10-01"),
            endTime: (0, moment_1.default)("2018-10-04"),
        });
        process.exit(1);
        const genLoop = generatorSimParameters(0);
        const simulations = [];
        while (true) {
            const { value: nextSimConfig, done } = genLoop.next();
            if (done)
                break;
            const simulationObj = Object.assign({
                runId,
                simVersion: process.env.SIM_VERSION,
                configName,
                ts: new Date(),
                status: "STARTED",
                envId: config.envId,
                minNotional: config.minNotional ? config.minNotional : 0.0005,
                saveRedis: config.saveRedis,
            }, nextSimConfig);
            (0, debug_1.debug)(JSON.stringify(simulationObj, null, 4));
            const { insertedId: sim_id } = yield simDb.collection('simulations')
                .insertOne(simulationObj);
            simulations.push((callback) => {
                try {
                    console.log(`Simulation ${sim_id} Activated`);
                    (0, child_process_1.exec)(`simulator.py ${sim_id}`, (err, stdout, stderr) => {
                        callback(err, stderr);
                    });
                }
                catch (err) {
                    throw err;
                }
            });
        }
        (0, async_1.parallelLimit)(simulations, config.parallelSimulations, (err, stdoutArray) => __awaiter(void 0, void 0, void 0, function* () {
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
                const elapsedTime = ('' + hours).padStart(2, '0') + ':' +
                    ('' + minutes).padStart(2, '0') + ':' +
                    ('' + seconds).padStart(2, '0');
                console.log(`Simulation ${runId} Execution Time (hh:mm:ss): ${elapsedTime}`);
                const runObj = {
                    runId,
                    simVersion: process.env.SIM_VERSION,
                    configName,
                    startTime,
                    endTime: new Date(),
                    elapsedTime,
                };
                yield simDb.collection('runs').insertOne(runObj);
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
}))();

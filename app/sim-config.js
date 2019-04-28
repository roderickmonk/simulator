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
const sim_params_1 = require("./sim-params");
const mongo_1 = require("./mongo");
const mongodb_1 = require("mongodb");
const GenerateSchema = require('generate-schema');
class ConfigGenerator {
    constructor(configName) {
        this.configName = configName;
        this.propertyLength = new Map();
        this.propertyLevel = new Map();
        this.propertyData = new Map();
        this.propertySchema = new Map();
        this.validateSimConfig = (simConfig) => {
            let schema = GenerateSchema.json('Product', simConfig).items.oneOf;
            schema.shift();
            let properties = [];
            for (const level of schema) {
                debug_1.debug('level:\n', JSON.stringify(level, null, 4));
                for (const prop of Object.keys(level.properties)) {
                    properties.push(prop);
                    this.propertySchema.set(prop, level.properties[prop]);
                }
            }
            for (const [prop, entry] of this.propertySchema.entries()) {
                assert(entry.type === 'array', `Parameter "${prop}" Data Not Array`);
                if (sim_params_1.simParams.hasOwnProperty(prop)) {
                    assert(sim_params_1.simParams[prop].items.type === entry.items.type, `Parameter "${prop}" Wrong Type`);
                }
                Object.keys(sim_params_1.simParams).forEach(property => {
                    assert(this.propertySchema.has(property), `Required Parameter "${property}" Not Found`);
                });
            }
            assert((new Set(properties)).size === properties.length, 'Duplicate Parameters Detected');
            for (const [i, config] of simConfig.entries()) {
                const levelConfig = i === 0 ?
                    config["0"] :
                    config;
                debug_1.debug({ levelConfig });
                for (const prop in levelConfig) {
                    debug_1.debug(chalk.red(`${prop}[${levelConfig[prop]}]`));
                    assert(levelConfig[prop].length > 0, 'Empty Configuration Parameter Array');
                    this.propertyLength.set(prop, levelConfig[prop].length);
                    this.propertyData.set(prop, levelConfig[prop]);
                    this.propertyLevel.set(prop, i);
                }
            }
            debug_1.debug(this.propertyData);
            debug_1.debug(this.propertyLevel);
            debug_1.debug(this.propertyLength);
            const levels = simConfig.length;
            debug_1.debug({ levels });
            for (let i = 0; i < simConfig.length; ++i) {
                const levelProperties = Array.from(this.propertyLevel)
                    .filter(level => level[1] === i)
                    .map(level => level[0]);
                debug_1.debug({ levelProperties });
                const levelPropertiesSameLength = levelProperties
                    .map(prop => this.propertyLength.get(prop))
                    .every((val, i, arr) => val === arr[0]);
                assert(levelPropertiesSameLength, 'Mismatched Parameter Array Lengths');
            }
            debug_1.debug('levels: ', schema.length);
        };
        this.getGenerator = () => __awaiter(this, void 0, void 0, function* () {
            assert(process.env.MONGODB, 'MONGODB Not Defined');
            assert(process.env.SIMULATOR_DB, 'SIMULATOR_DB Not Defined');
            const mongoClient = yield mongodb_1.MongoClient.connect(process.env.MONGODB, { useNewUrlParser: true });
            mongo_1.Mongo.simDb = mongoClient.db(process.env.SIMULATOR_DB);
            this.config =
                yield mongo_1.Mongo.simDb.collection('configurations').findOne({ name: this.configName });
            assert(this.config, 'Unknown Configuration');
            debug_1.debug({ config: this.config });
            this.validateSimConfig(this.config.simConfig);
            return this.generate(0);
        });
    }
    *generate(level) {
        const levelProperties = Array.from(this.propertyLevel)
            .filter(l => l[1] === level)
            .map(l => l[0]);
        if (levelProperties.length === 0) {
            yield {};
        }
        else {
            const depth = this.propertyData.get(levelProperties[0]).length;
            for (let i = 0; i < depth; ++i) {
                const returnObj = {};
                for (const prop of levelProperties) {
                    returnObj[prop] = this.propertyData.get(prop)[i];
                }
                const genLoop = this.generate(level + 1);
                while (true) {
                    const next = genLoop.next();
                    if (next.done)
                        break;
                    yield Object.assign({}, returnObj, next.value);
                }
            }
        }
    }
}
exports.ConfigGenerator = ConfigGenerator;

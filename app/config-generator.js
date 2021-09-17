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
exports.ConfigGenerator = void 0;
const assert = require('assert');
const _ = __importStar(require("lodash"));
const chalk = require('chalk');
const debug_1 = require("./debug");
const config_manager_1 = require("./config-manager");
const GenerateSchema = require('generate-schema');
class ConfigGenerator {
    constructor(configName, simConfigurationDb) {
        this.configName = configName;
        this.simConfigurationDb = simConfigurationDb;
        this.propertyLength = new Map();
        this.propertyLevel = new Map();
        this.propertyData = new Map();
        this.propertySchema = new Map();
        this.config = null;
        this.validateMultiplyConfig = (multipleParams, simConfig) => {
            (0, debug_1.debug)("simConfig:\n", JSON.stringify(simConfig, null, 4));
            let schema = GenerateSchema.json('Product', simConfig.multiplyConfig).items.oneOf;
            schema.shift();
            let properties = [];
            for (const level of schema) {
                (0, debug_1.debug)('level:\n', JSON.stringify(level, null, 4));
                for (const prop of Object.keys(level.properties)) {
                    properties.push(prop);
                    this.propertySchema.set(prop, level.properties[prop]);
                }
            }
            for (const [prop, entry] of this.propertySchema.entries()) {
                assert(entry.type === 'array', `Multiply Parameter "${prop}" Data Not Array`);
                if (multipleParams.hasOwnProperty(prop)) {
                }
                Object.keys(multipleParams).forEach(property => {
                    assert(this.propertySchema.has(property), `Required Multiply Parameter "${property}" Not Found`);
                });
            }
            assert((new Set(properties)).size === properties.length, 'Duplicate Multiply Parameters Detected');
            for (const [i, config] of simConfig.multiplyConfig.entries()) {
                const levelConfig = i === 0 ?
                    config["0"] :
                    config;
                (0, debug_1.debug)({ levelConfig });
                for (const prop in levelConfig) {
                    (0, debug_1.debug)(chalk.red(`${prop}[${levelConfig[prop]}]`));
                    assert(levelConfig[prop].length > 0, 'Empty Configuration Parameter Array');
                    this.propertyLength.set(prop, levelConfig[prop].length);
                    this.propertyData.set(prop, levelConfig[prop]);
                    this.propertyLevel.set(prop, i);
                }
            }
            (0, debug_1.debug)(this.propertyData);
            (0, debug_1.debug)(this.propertyLevel);
            (0, debug_1.debug)(this.propertyLength);
            const levels = simConfig.multiplyConfig.length;
            (0, debug_1.debug)({ levels });
            for (let i = 0; i < simConfig.multiplyConfig.length; ++i) {
                const levelProperties = Array.from(this.propertyLevel)
                    .filter(level => level[1] === i)
                    .map(level => level[0]);
                (0, debug_1.debug)({ levelProperties });
                const levelPropertiesSameLength = levelProperties
                    .map(prop => this.propertyLength.get(prop))
                    .every((val, i, arr) => val === arr[0]);
                assert(levelPropertiesSameLength, 'Mismatched Parameter Array Lengths');
            }
            (0, debug_1.debug)('levels: ', simConfig.multiplyConfig.length);
        };
        this.getGenerator = () => __awaiter(this, void 0, void 0, function* () {
            try {
                process.on('unhandledRejection', _.noop);
                this.config = (yield this.simConfigurationDb
                    .collection('configurations')
                    .findOne({ name: this.configName }));
                (0, debug_1.debug)(`config: ${JSON.stringify(this.config, null, 4)}`);
                if (this.config) {
                    const multiplyConfigParams = yield this.simConfigurationDb
                        .collection('trader.multiply.params')
                        .findOne({
                        name: this.config.multiplyConfigParams
                    });
                    const multiplyParams = multiplyConfigParams.params;
                    (0, debug_1.debug)(JSON.stringify(multiplyParams, null, 4));
                    const validConfig = yield (0, config_manager_1.configValidator)(this.config);
                    (0, debug_1.debug)(JSON.stringify(this.config, null, 4));
                    if (validConfig) {
                        this.validateMultiplyConfig(multiplyParams, this.config);
                        return this.generate(0);
                    }
                    else {
                        return Promise.reject(new Error('Invalid Configuration'));
                    }
                }
                else {
                    return Promise.reject(new Error('Unknown Configuration'));
                }
            }
            catch (err) {
                return Promise.reject(err);
            }
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
            const levelDepth = this.propertyData.get(levelProperties[0]).length;
            for (let i = 0; i < levelDepth; ++i) {
                const returnObj = {};
                for (const prop of levelProperties) {
                    returnObj[prop] = this.propertyData.get(prop)[i];
                }
                const genLoop = this.generate(level + 1);
                while (true) {
                    const next = genLoop.next();
                    if (next.done)
                        break;
                    yield Object.assign(Object.assign({}, returnObj), next.value);
                }
            }
        }
    }
}
exports.ConfigGenerator = ConfigGenerator;

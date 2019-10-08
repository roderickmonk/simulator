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
const ajv_1 = __importDefault(require("ajv"));
const assert_1 = __importDefault(require("assert"));
const lodash_1 = __importDefault(require("lodash"));
const Barrel_1 = require("../Barrel");
const package_json = require('../../package.json');
console.log({ version: package_json.version });
class ConfigManagerBase {
    constructor(processSchema, botSchema) {
        this.processSchema = processSchema;
        this.botSchema = botSchema;
        this.mergeProcessConfigWithBotConfig = (bot, process) => {
            const temp = {};
            lodash_1.default.merge(temp, process, bot);
            lodash_1.default.merge(bot, temp);
        };
    }
    get configCollectionName() {
        return this._configCollectionName;
    }
    set configCollectionName(configCollectionName) {
        this._configCollectionName = configCollectionName;
    }
    get hftName() {
        return this._hftName;
    }
    set hftName(hftName) {
        this._hftName = hftName;
    }
    get configDb() {
        return this._configDB;
    }
    set configDb(db) {
        this._configDB = db;
    }
    get configCollection() {
        return this._configCollection;
    }
    set configCollection(configCollection) {
        this._configCollection = configCollection;
    }
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                assert_1.default(this.configDb, 'Configuration Db Not Defined');
                assert_1.default(this.configCollectionName, 'Config Collection Name Not Defined');
                assert_1.default(this.hftName, 'Process Name Not Defined');
                this.configCollection =
                    this.configDb.collection(this.configCollectionName);
                return yield this.refreshConfig();
            }
            catch (err) {
                return Promise.reject(new Error(err.message));
            }
        });
    }
    refreshConfig() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                assert_1.default(this.configCollection);
                const processConfig = yield this.configCollection
                    .findOne({ hftName: this.hftName });
                assert_1.default(processConfig, `Configuration Collection ` +
                    `${this.configCollectionName} Not Found`);
                processConfig.x =
                    this.exchangeMneumonic(processConfig.exchange);
                yield this.processConfigValidator(processConfig);
                if (!this.currentConfig || !lodash_1.default.isEqual(this.currentConfig, processConfig)) {
                    Barrel_1.Process.healthBroker.publish({
                        routingKey: "ProcessConfiguration",
                        content: Buffer.from(JSON.stringify(processConfig))
                    });
                }
                this.currentConfig = lodash_1.default.cloneDeep(processConfig);
                return processConfig;
            }
            catch (err) {
                return Promise.reject(new Error(err.message));
            }
        });
    }
    processConfigValidator(hftConfig) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const ajv = new ajv_1.default({
                    allErrors: true,
                    useDefaults: true,
                    verbose: true
                });
                Barrel_1.debugHft({ hftConfig });
                const validate = ajv.compile(this.processSchema);
                const valid = validate(hftConfig);
                if (!valid || validate.errors) {
                    const msg = `Invalid Configuration: ` +
                        `${JSON.stringify(validate.errors, null, 4)}`;
                    return Promise.reject(new Error('Invalid Process Configuration'));
                }
                yield this.validateBotConfigurations(hftConfig);
            }
            catch (err) {
                console.log(err);
                return Promise.reject(err);
            }
        });
    }
    validateBotConfiguration(market, botConfig) {
        try {
            const ajv = new ajv_1.default({
                allErrors: true,
                useDefaults: true,
                verbose: true
            });
            const validate = ajv.compile(this.botSchema);
            Barrel_1.debugHft(`(${market}) Bot Configuration Valid: `, validate.errors == null);
            const valid = validate(botConfig);
            const msg = `Invalid Bot Schema: ` +
                `${JSON.stringify(validate.errors, null, 4)}`;
            assert_1.default(validate.errors == null && valid, msg);
        }
        catch (err) {
            console.log(err);
            throw err;
        }
    }
    exchangeMneumonic(exchange) {
        return lodash_1.default.toLower(`${exchange[0]}${exchange.slice(-1)}`);
    }
}
exports.ConfigManagerBase = ConfigManagerBase;

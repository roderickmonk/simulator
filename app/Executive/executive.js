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
const lodash_1 = __importDefault(require("lodash"));
const MongoClient = require('mongodb').MongoClient;
const Barrel_1 = require("../Barrel");
const process_1 = require("../Process/process");
const CircularBuffer = require("circular-buffer");
class MarketMap {
    constructor() {
        this.map = new Map();
    }
    has(asset) {
        return this.map.has(asset);
    }
    get(asset) {
        return this.map.get(asset);
    }
    set(asset, market) {
        this.map.set(asset, market);
        return market;
    }
    clear() {
        this.map.clear();
    }
}
exports.MarketMap = MarketMap;
class Executive {
    constructor() {
        this.marketMap = new MarketMap();
        this.manageBots = () => __awaiter(this, void 0, void 0, function* () { return Promise.reject(new Error(`manageBots Undefined`)); });
        process.on("uncaughtException", (err) => __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.shutdown();
                console.log("uncaughtException: ", err);
                Barrel_1.logError(`Uncaught Exception: ${err.message}`, err);
                setTimeout(() => { process.exit(120); }, 1500);
            }
            catch (err) {
                process.exit(106);
            }
        }));
        process.on("SIGTERM", () => __awaiter(this, void 0, void 0, function* () {
            try {
                Barrel_1.logError("SIGTERM Received");
                yield this.shutdown();
            }
            catch (err) {
                Barrel_1.logError(`SIGTERM Error: ${err.message}`);
            }
            finally {
                Barrel_1.logError("SIGTERM Complete");
                process.exit(0);
            }
        }));
        process.on("SIGINT", () => __awaiter(this, void 0, void 0, function* () {
            try {
                Barrel_1.logError(`SIGINT Received`);
                yield this.shutdown();
            }
            catch (err) {
                Barrel_1.logError(`SIGINT Error: ${err.message}`);
            }
            finally {
                Barrel_1.logError(`SIGINT Complete`);
                process.exit(0);
            }
        }));
    }
    startProcess() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                setInterval(() => {
                    process.send ? process.send({ heartbeat: 'thump' }) : lodash_1.default.noop;
                }, 2000);
            }
            catch (err) {
                console.log(err.message);
                setTimeout(() => process.exit(107), 1000);
            }
        });
    }
    ;
    shutdown() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                console.log("Flushing Archivers");
                process_1.Process.active = false;
            }
            catch (err) {
                Barrel_1.logError(`Shutdown Error: ${err.message}`);
            }
        });
    }
    ;
}
exports.Executive = Executive;

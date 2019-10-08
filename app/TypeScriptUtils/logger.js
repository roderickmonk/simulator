"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const debug_1 = require("./debug");
const bunyan = require('bunyan');
const luvely = require('luvely');
const cloudWatch = require('bunyan-cloudwatch');
class Logger {
    constructor(logStreamName, logConfig = undefined) {
        this.logStreamName = logStreamName;
        this.logConfig = logConfig;
        this.trace = (...args) => this.logger.trace(...args);
        this.debug = (...args) => this.logger.debug(...args);
        this.info = (...args) => this.logger.info(...args);
        this.warn = (...args) => this.logger.warn(...args);
        this.error = (...args) => this.logger.error(...args);
        this.fatal = (...args) => this.logger.fatal(...args);
        this.logger = bunyan.createLogger({ name: 'hft', level: 61 });
        this.consoleStream = (config) => ({
            name: 'Console',
            level: config.logLevel,
            stream: luvely()
        });
        this.cloudWatchStream = (config) => ({
            name: 'CloudWatch',
            type: 'raw',
            level: config.logLevel,
            stream: new cloudWatch({
                logGroupName: Logger.logGroupName,
                logStreamName: this.logStreamName,
                cloudWatchLogsOptions: {
                    region: "us-west-2"
                }
            }),
        });
        debug_1.debugHft({
            logGroupName: Logger.logGroupName,
            logStreamName,
            logConfig
        });
        if (logConfig) {
            !logConfig.console.enable ||
                this.logger.addStream(this.consoleStream(logConfig.console));
            !logConfig.cloudWatch.enable ||
                this.logger.addStream(this.cloudWatchStream(logConfig.cloudWatch));
        }
    }
}
exports.Logger = Logger;
Logger.process = undefined;
var LogLevel;
(function (LogLevel) {
    LogLevel[LogLevel["Fatal"] = bunyan.FATAL] = "Fatal";
    LogLevel[LogLevel["Error"] = bunyan.ERROR] = "Error";
    LogLevel[LogLevel["Warn"] = bunyan.WARN] = "Warn";
    LogLevel[LogLevel["Info"] = bunyan.INFO] = "Info";
    LogLevel[LogLevel["Debug"] = bunyan.DEBUG] = "Debug";
    LogLevel[LogLevel["Trace"] = bunyan.TRACE] = "Trace";
})(LogLevel || (LogLevel = {}));
const log = (logLevel, msg, errObj = undefined) => {
    if (Logger.process != null) {
        let logLevelHandler = undefined;
        switch (logLevel) {
            case LogLevel.Fatal:
                logLevelHandler = Logger.process.fatal;
                break;
            case LogLevel.Error:
                logLevelHandler = Logger.process.error;
                break;
            case LogLevel.Warn:
                logLevelHandler = Logger.process.warn;
                break;
            case LogLevel.Info:
                logLevelHandler = Logger.process.info;
                break;
            case LogLevel.Debug:
                logLevelHandler = Logger.process.debug;
                break;
            case LogLevel.Trace:
                logLevelHandler = Logger.process.trace;
                break;
        }
        lodash_1.default.isEmpty(errObj) ?
            logLevelHandler(msg) :
            logLevelHandler(msg, errObj);
    }
    else {
        if (errObj) {
            console.log(`Logger: ${msg}\n${JSON.stringify(errObj, null, 4)}`);
        }
        else {
            console.log(`Logger: ${msg}`);
        }
    }
};
exports.logFatal = (msg, errObj = undefined) => {
    log(LogLevel.Fatal, msg, errObj);
};
exports.logError = (msg, errObj = undefined) => {
    log(LogLevel.Error, msg, errObj);
};
exports.logWarn = (msg, errObj = undefined) => {
    log(LogLevel.Warn, msg, errObj);
};
exports.logInfo = (msg, errObj = undefined) => {
    log(LogLevel.Info, msg, errObj);
};
exports.logDebug = (msg, errObj = undefined) => {
    log(LogLevel.Debug, msg, errObj);
};
exports.logTrace = (msg, errObj = undefined) => {
    log(LogLevel.Trace, msg, errObj);
};

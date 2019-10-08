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
const rxjs_1 = require("rxjs");
const assert_1 = __importDefault(require("assert"));
const logger_1 = require("./logger");
const debug_1 = require("./debug");
const amqplib_1 = __importDefault(require("amqplib"));
const envalid = require("envalid");
const { str } = envalid;
const env = envalid.cleanEnv(process.env, {
    RABBITMQ_DEFAULT_USER: str(),
    RABBITMQ_DEFAULT_PASS: str(),
    RABBITMQ_NODE_PORT: str(),
});
class Broker {
    constructor(brokerParams) {
        this.brokerParams = brokerParams;
        this.retryConnectTimer$ = rxjs_1.Observable.timer(0, 5000).startWith();
        this.retryConnection = () => {
            try {
                this.retryConnectSubscription = this.retryConnectTimer$.subscribe(() => __awaiter(this, void 0, void 0, function* () {
                    try {
                        yield this.connect();
                    }
                    catch (err) {
                        logger_1.logError(`Broker Retry Error: ${err.message}`);
                    }
                }), (err) => {
                    logger_1.logError(`Broker Retry Error: ${err.message}`);
                });
            }
            catch (err) {
                throw err;
            }
        };
        this.closeEvent = (event) => {
            debug_1.debugBroker(`Broker Close Event: `, { event });
            !this.brokerParams.disconnectHandler ||
                this.brokerParams.disconnectHandler();
            if (this.connection) {
                this.connection.removeListener("close", this.closeEvent);
                this.connection.removeListener("error", this.errorEvent);
            }
            this.retryConnection();
        };
        this.errorEvent = (err) => {
            debug_1.debugBroker(`Broker Error Event: ${JSON.stringify(err, null, 4)}`);
        };
        this.connect = () => __awaiter(this, void 0, void 0, function* () {
            const { exchange, host } = this.brokerParams;
            try {
                const connectString = `amqp://${env.RABBITMQ_DEFAULT_USER}:` +
                    `${env.RABBITMQ_DEFAULT_PASS}@` +
                    `${host}:${env.RABBITMQ_NODE_PORT}`;
                this.connection = yield amqplib_1.default.connect(connectString);
                !this.retryConnectSubscription ||
                    this.retryConnectSubscription.unsubscribe();
                this.connection.addListener("error", this.errorEvent);
                this.connection.addListener("close", this.closeEvent);
                this.channel = yield this.connection.createChannel();
                yield this.channel.prefetch(1);
                debug_1.debugBroker('Channel Created');
                yield this.channel.assertExchange(exchange, 'topic', { durable: true });
                if (this.brokerParams.topics) {
                    for (const topic of this.brokerParams.topics) {
                        const q = yield this.channel.assertQueue(topic.routingKey, { durable: false });
                        yield this.channel.bindQueue(q.queue, exchange, topic.routingKey);
                        yield this.channel.consume(q.queue, (msg) => __awaiter(this, void 0, void 0, function* () {
                            try {
                                yield topic.dataSink(msg);
                                !this.channel || this.channel.ack(msg);
                            }
                            catch (err) {
                                logger_1.logError(err.message);
                            }
                        }), { noAck: false });
                    }
                }
                debug_1.debugBroker('Connect Handler Available:', Boolean(this.brokerParams.connectHandler));
                !this.brokerParams.connectHandler ||
                    this.brokerParams.connectHandler();
                debug_1.debugBroker('Connect Handler Called');
            }
            catch (err) {
                debug_1.debugBroker('Broker Error:', err);
                logger_1.logFatal(err.message);
                return Promise.reject(new Error(err.message));
            }
        });
        this.publish = (params) => {
            try {
                const { exchange, expiration } = this.brokerParams;
                const { routingKey, content } = params;
                debug_1.debugHft({ brokerParams: this.brokerParams });
                assert_1.default(this.channel);
                const result = this.channel ?
                    this.channel.publish(exchange, routingKey, content, {
                        persistent: false,
                        expiration: expiration || 86400000,
                    }) :
                    false;
                return result;
            }
            catch (err) {
                logger_1.logError("Broker Publishing Failure:", err.message);
                return true;
            }
        };
        this.close = () => {
            !this.channel || this.channel.close();
            if (this.connection) {
                this.connection.removeListener("close", this.closeEvent);
                this.connection.removeListener("error", this.errorEvent);
                this.connection.close();
                this.connection = undefined;
            }
        };
        setTimeout(this.retryConnection, 5000);
        debug_1.debugBroker(JSON.stringify(brokerParams, null, 4));
    }
}
exports.Broker = Broker;

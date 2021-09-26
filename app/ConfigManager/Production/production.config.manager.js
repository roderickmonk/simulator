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
const assert_1 = __importDefault(require("assert"));
const Barrel_1 = require("../../Barrel");
class ProductionConfigManager extends Barrel_1.ConfigManagerBase {
    constructor() {
        super(Barrel_1.productionSchema, Barrel_1.productionBotSchema);
        this.getPDF = (pdfName) => __awaiter(this, void 0, void 0, function* () {
            try {
                assert_1.default(pdfName, 'PDF Name Not Defined');
                const pdf = yield this.PDFsCollection.findOne({ name: pdfName });
                assert_1.default(pdf &&
                    pdf.hasOwnProperty('x') &&
                    pdf.hasOwnProperty('y'), 'PDF.x or PDF.y Missing');
                const { x, y } = pdf;
                assert_1.default(x.length > 0, `Zero Length PDF.x`);
                assert_1.default(y.length > 0, `Zero Length PDF.y`);
                return [x, y];
            }
            catch (err) {
                return Promise.reject(err);
            }
        });
        this.randomTrader = () => __awaiter(this, void 0, void 0, function* () {
            try {
                const traders = Array.from(yield this.traderDefaultPDFsCollection
                    .find({})
                    .toArray());
                assert_1.default(traders && traders.length > 1, "No Trader to Default PDF Assignments");
                const randomSelection = Math.floor(Math.random() * Math.floor(traders.length));
                return traders[randomSelection];
            }
            catch (err) {
                return Promise.reject(new Error(err.message));
            }
        });
        this.randomizeTrader = (asset, botConfig) => __awaiter(this, void 0, void 0, function* () {
            try {
                const { trader, PDF } = yield this.randomTrader();
                botConfig.trader = trader;
                botConfig.PDF = PDF;
                const update = {};
                const traderKey = `bots.${asset}.trader`;
                update[traderKey] = botConfig.trader;
                const pdfKey = `bots.${asset}.PDF`;
                update[pdfKey] = botConfig.PDF;
                assert_1.default(this.configCollection);
                yield this.configCollection.updateOne({ hftName: this.hftName }, { $set: update });
            }
            catch (err) {
                return Promise.reject(err);
            }
        });
        this.randomizeTradeTrackings = (hftConfig) => __awaiter(this, void 0, void 0, function* () {
            try {
                for (const [asset, botConfig] of Object.entries(hftConfig.bots)) {
                    yield this.randomizeTradeTracking(asset, botConfig);
                }
            }
            catch (err) {
                return Promise.reject(err);
            }
        });
        this.randomizeTradeTracking = (asset, botConfig) => __awaiter(this, void 0, void 0, function* () {
            try {
                const active = lodash_1.default.sample([true, false]);
                if (lodash_1.default.isUndefined(botConfig.alternateMarketTracking)) {
                    Object.assign(botConfig, { alternateMarketTracking: { active } });
                }
                else {
                    Object.assign(botConfig.alternateMarketTracking, { active });
                }
                const update = {};
                const key = `bots.${asset}.alternateMarketTracking.active`;
                update[key] = active;
                assert_1.default(this.configCollection);
                yield this.configCollection.updateOne({ hftName: this.hftName }, { $set: update });
            }
            catch (err) {
                return Promise.reject(err);
            }
        });
        this.randomizeTraders = (hftConfig) => __awaiter(this, void 0, void 0, function* () {
            try {
                for (const [asset, botConfig] of Object.entries(hftConfig.bots)) {
                    yield this.randomizeTrader(asset, botConfig);
                }
            }
            catch (err) {
                return Promise.reject(err);
            }
        });
    }
    static get Instance() {
        return this.instance || (this.instance = new this());
    }
    start() {
        const _super = Object.create(null, {
            start: { get: () => super.start }
        });
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const processConfig = yield _super.start.call(this);
                this.latestConfig = lodash_1.default.cloneDeep(processConfig);
                assert_1.default(this.configDb);
                this.PDFsCollection =
                    this.configDb.collection('PDFs');
                this.traderDefaultPDFsCollection =
                    this.configDb.collection('trader-default-PDFs');
                ProductionConfigManager.randomizeTraders ?
                    yield this.randomizeTraders(processConfig) :
                    lodash_1.default.noop;
                ProductionConfigManager.randomizeTradeTracking ?
                    yield this.randomizeTradeTrackings(processConfig) :
                    lodash_1.default.noop;
                return processConfig;
            }
            catch (err) {
                return Promise.reject(new Error(err.message));
            }
        });
    }
    validateBotConfigurations(hftConfig) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                for (const [asset, botConfig] of Object.entries(hftConfig.bots)) {
                    const market = `${hftConfig.baseCurrency}-${asset}`;
                    this.mergeProcessConfigWithBotConfig(botConfig, lodash_1.default.pick(hftConfig, [
                        "QL",
                        "archiveCycles",
                        "backstop",
                        "sellDelay",
                        "sellAttempts",
                        "minNotional",
                        "PDF",
                        "depth",
                        "inventoryLimitSide",
                        "allowOrderConflicts",
                        "feeRate",
                        "priceDepthLimit",
                        "alternateMarketTracking",
                        "tuningGeneration"
                    ]));
                    if (lodash_1.default.isUndefined(botConfig.inventoryLimit)) {
                        if (lodash_1.default.isUndefined(hftConfig.inventoryLimit)) {
                            botConfig.inventoryLimit =
                                botConfig.QL;
                        }
                        else {
                            botConfig.inventoryLimit =
                                hftConfig.inventoryLimit;
                        }
                    }
                    if (lodash_1.default.isUndefined(botConfig.tick)) {
                        botConfig.pricePrecision =
                            -Math.round(Math.log10(botConfig.tick));
                    }
                    switch (lodash_1.default.isUndefined(hftConfig.trader)) {
                        case true:
                            if (lodash_1.default.isUndefined(botConfig.trader)) {
                                yield this.randomizeTrader(asset, botConfig);
                            }
                            break;
                        case false:
                            if (lodash_1.default.isUndefined(botConfig.trader)) {
                                botConfig.trader = hftConfig.trader;
                            }
                            break;
                    }
                    this.validateBotConfiguration(market, botConfig);
                }
            }
            catch (err) {
                return Promise.reject(err);
            }
        });
    }
}
exports.ProductionConfigManager = ProductionConfigManager;
ProductionConfigManager.randomizeTraders = false;
ProductionConfigManager.randomizeTradeTracking = false;

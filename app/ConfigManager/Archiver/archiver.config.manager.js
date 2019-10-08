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
const Barrel_1 = require("../../Barrel");
class ArchiverConfigManager extends Barrel_1.ConfigManagerBase {
    constructor() {
        super(Barrel_1.archiverSchema, Barrel_1.archiverBotSchema);
    }
    static get Instance() {
        return this.instance || (this.instance = new this());
    }
    validateBotConfigurations(hftConfig) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                for (const [asset, botConfig] of Object.entries(hftConfig.bots)) {
                    const market = `${hftConfig.baseCurrency}-${asset}`;
                    this.mergeProcessConfigWithBotConfig(botConfig, lodash_1.default.pick(hftConfig, [
                        "archiveOrderbooks",
                        "archiveTrades",
                        "pollingInterval",
                    ]));
                    this.validateBotConfiguration(market, botConfig);
                    Barrel_1.debugHft(botConfig);
                }
            }
            catch (err) {
                return Promise.reject(err);
            }
        });
    }
}
exports.ArchiverConfigManager = ArchiverConfigManager;

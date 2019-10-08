"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var Code;
(function (Code) {
    Code[Code["ApplicationError"] = 1] = "ApplicationError";
    Code[Code["ExchangeError"] = 2] = "ExchangeError";
    Code[Code["WebSocketFailure"] = 3] = "WebSocketFailure";
})(Code || (Code = {}));
class ApplicationError extends Error {
    constructor(message, code = 1) {
        super(message);
        this.code = code;
        super.name = 'ExchangeError';
        Object.setPrototypeOf(this, ApplicationError.prototype);
    }
    toString() {
        return `${this.name} "${this.message}"`;
    }
}
exports.ApplicationError = ApplicationError;
class ExchangeError extends ApplicationError {
    constructor(message) {
        super(message, Code.ExchangeError);
        super.name = 'ExchangeError';
        Object.setPrototypeOf(this, ExchangeError.prototype);
    }
}
exports.ExchangeError = ExchangeError;
class WebSocketFailure extends ApplicationError {
    constructor(message = 'WebSocketFailure') {
        super(message, Code.WebSocketFailure);
        super.name = 'WebSocketFailure';
        Object.setPrototypeOf(this, WebSocketFailure.prototype);
    }
}
exports.WebSocketFailure = WebSocketFailure;

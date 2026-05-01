"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.FirestoreFallbackTransport = exports.PubSub = void 0;
const pubsub_1 = __importDefault(require("./pubsub"));
var pubsub_2 = require("./pubsub");
Object.defineProperty(exports, "PubSub", { enumerable: true, get: function () { return __importDefault(pubsub_2).default; } });
var FirestoreFallbackTransport_1 = require("./transport/FirestoreFallbackTransport");
Object.defineProperty(exports, "FirestoreFallbackTransport", { enumerable: true, get: function () { return FirestoreFallbackTransport_1.FirestoreFallbackTransport; } });
__exportStar(require("./transport/types"), exports);
__exportStar(require("./rooms"), exports);
__exportStar(require("./envelope"), exports);
__exportStar(require("./errors"), exports);
exports.default = pubsub_1.default;

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
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RealtimeTransportCore = exports.RealtimeRouterCore = exports.RealtimeAuth = exports.RealtimeMetricsCore = exports.RealtimeEnvelopeCore = exports.RealtimeCore = exports.FirestoreFallbackTransport = exports.PubSub = void 0;
const pubsub_1 = __importDefault(require("./pubsub"));
var pubsub_2 = require("./pubsub");
Object.defineProperty(exports, "PubSub", { enumerable: true, get: function () { return __importDefault(pubsub_2).default; } });
var FirestoreFallbackTransport_1 = require("./transport/FirestoreFallbackTransport");
Object.defineProperty(exports, "FirestoreFallbackTransport", { enumerable: true, get: function () { return FirestoreFallbackTransport_1.FirestoreFallbackTransport; } });
__exportStar(require("./transport/types"), exports);
__exportStar(require("./rooms"), exports);
__exportStar(require("./envelope"), exports);
__exportStar(require("./errors"), exports);
__exportStar(require("./factory"), exports);
__exportStar(require("./client-sdk"), exports);
exports.RealtimeCore = __importStar(require("./core/types"));
exports.RealtimeEnvelopeCore = __importStar(require("./core/envelope"));
exports.RealtimeMetricsCore = __importStar(require("./core/metrics"));
exports.RealtimeAuth = __importStar(require("./core/auth"));
exports.RealtimeRouterCore = __importStar(require("./core/router"));
exports.RealtimeTransportCore = __importStar(require("./transport/RealtimeTransport"));
__exportStar(require("./transports/postgres"), exports);
__exportStar(require("./transports/mongo"), exports);
__exportStar(require("./transports/redis"), exports);
__exportStar(require("./transports/firestore"), exports);
__exportStar(require("./transports/hybrid"), exports);
__exportStar(require("./ws/protocol"), exports);
__exportStar(require("./ws/connection"), exports);
__exportStar(require("./ws/gateway"), exports);
__exportStar(require("./ws/room-manager"), exports);
exports.default = pubsub_1.default;

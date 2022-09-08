"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Subzero = void 0;
var subzero_wasm_1 = require("subzero-wasm");
var Subzero = /** @class */ (function () {
    function Subzero(dbType, schema) {
        this.backend = subzero_wasm_1.Backend.init(JSON.stringify(schema));
        this.dbType = dbType;
    }
    Subzero.prototype.get_main_query = function (method, schema_name, entity, path, get, body, headers, cookies, env) {
        var _a = this.backend.get_query(schema_name, entity, method, path, get, body !== null && body !== void 0 ? body : "", headers, cookies, env, this.dbType), query = _a[0], parameters = _a[1];
        return [query, parameters];
    };
    return Subzero;
}());
exports.Subzero = Subzero;

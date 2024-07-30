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
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
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
const path_1 = __importDefault(require("path"));
const fs_1 = require("fs");
const sync_1 = require("csv-parse/sync");
const dotenv = __importStar(require("dotenv"));
dotenv.config();
const pg_format_1 = __importDefault(require("pg-format"));
const db_1 = __importDefault(require("./db"));
const DIR_DATA = path_1.default.resolve(__dirname, "../data");
function checkTypeData(data) {
    const objectKeys = Object.keys(data);
    if (objectKeys.includes("COD_7"))
        return "MUNICIPAL";
    if (objectKeys.includes("UF"))
        return "ESTADUAL";
    return "FEDERAL";
}
function isValidUrl(urlString) {
    var urlPattern = new RegExp("^(https?:\\/\\/)?" + // validate protocol
        "((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|" + // validate domain name
        "((\\d{1,3}\\.){3}\\d{1,3}))" + // validate OR ip (v4) address
        "(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*" + // validate port and path
        "(\\?[;&a-z\\d%_.~+=-]*)?" + // validate query string
        "(\\#[-a-z\\d_]*)?$", "i"); // validate fragment locator
    return !!urlPattern.test(urlString || "");
}
function removeCaractersNDF(string) {
    if (!string)
        return '';
    return string.normalize('NFD').replace(/[\u0300-\u036f]/g, "");
}
function insertColeta(data) {
    return __awaiter(this, void 0, void 0, function* () {
        const columns = Object.keys(data).filter((key) => !/^_(.*)|(^id$)/.test(key)); // Remove Skips and id
        const values = columns.map((column) => data[column]);
        const _queryFromated = (0, pg_format_1.default)(`SELECT id FROM marcos_legais
    WHERE
      tipo = %L
      AND ano = %L
      AND unaccent(lower(lei)) = unaccent(lower(%L))
      ${data.estado ? "AND estado = %L" : ""}
      ${data.cod_municipio ? "AND cod_municipio = %L" : ""}
    `, ...[data.tipo, data.ano, data.lei, data.estado, data.cod_municipio].filter((val) => val));
        const res = yield db_1.default.query(_queryFromated);
        if (!res.rowCount) {
            const sqlQuery = `
    INSERT INTO marcos_legais (${columns.join(", ")})
    VALUES ($${[...columns.keys()].map((key) => key + 1).join(", $")})
    RETURNING *
    `;
            // const queryFormated = format(
            //   `INSERT INTO dados_pesquisa (cod_municipio, cod_dicionario, periodo, submission_id, valor, cod_interno) VALUES %L`,
            //   insertGroup
            // );
            const resInsertDB = yield db_1.default.query(sqlQuery, values);
            return resInsertDB.rowCount;
        }
        return null;
    });
}
function removeNull(obj) {
    return Object.keys(obj).reduce((acc, key) => {
        if (obj[key] != null) {
            acc = Object.assign(Object.assign({}, acc), { [key]: obj[key] });
        }
        return acc;
    }, {});
}
function main(file) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!file)
            throw new Error("File name is required");
        const fileContent = yield fs_1.promises.readFile(`${DIR_DATA}/${file}`);
        const allRecords = (0, sync_1.parse)(fileContent, {
            columns: true,
            skip_empty_lines: true,
            delimiter: ",",
            trim: true,
        });
        // const records = [allRecords[0], allRecords[1], allRecords[2]];
        const records = allRecords;
        const checkType = checkTypeData(records[0]);
        for (const record of records) {
            const nomeLei = record["LEI/DECRETO"] || record["LEI"] || "";
            const isURL = isValidUrl(record["URL"]);
            const insertData = {
                tipo: checkType,
                lei: nomeLei.replace("nº ", ""),
                nome: record["NOME"] || nomeLei.replace("nº ", ""),
                estado: record["UF"] || null,
                municipio: record["MUN_UF"] || null,
                ano: record["ANO"],
                link: isURL ? record["URL"] : null,
                ementa: record["EMENTA"],
                situacao: record["STATUS"] || null,
                cod_municipio: record["COD_7"],
                fonte: record["FONT"] || (!isURL ? record["URL"] : null),
            };
            const data = removeNull(insertData);
            yield insertColeta(data);
        }
        return;
    });
}
(() => __awaiter(void 0, void 0, void 0, function* () {
    yield main("marcos-legais/LEIS-FEDERAL.csv");
    yield main("marcos-legais/LEIS-ESTADUAL.csv");
    yield main("marcos-legais/LEIS-MUNICIPAL.csv");
}))();

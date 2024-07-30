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
const dotenv = __importStar(require("dotenv")); // see https://github.com/motdotla/dotenv#how-do-i-use-dotenv-with-import
dotenv.config();
const pg_format_1 = __importDefault(require("pg-format"));
const db_1 = __importDefault(require("./db"));
const dirData = path_1.default.resolve(__dirname, "../data");
const CONVER_ALL = (info) => info;
const REMOVE_VIRCULA = (info) => {
    const validPtBr = (info + "").split("").some((letter) => letter === ",");
    if (validPtBr)
        return (info + "")
            .replace(",", "VIRGULA")
            .replace(".", "")
            .replace("VIRGULA", ".");
    return info + "";
};
const CONVER_INT = (info) => Number.isNaN(parseInt(info)) ? "0" : REMOVE_VIRCULA(info);
const CONVER_FLOAT = (info, fix = 2) => Number.isNaN(parseInt(info))
    ? "0"
    : parseFloat(REMOVE_VIRCULA(info)).toFixed(fix);
const CONVERT_TYPES = {
    INTEIRO: CONVER_INT,
    MONETARIO: CONVER_FLOAT,
    DECIMAL: CONVER_FLOAT,
    NUMERICO: CONVER_FLOAT,
    PERCENTUAL: CONVER_FLOAT,
};
const removeDicionario = ["COD_7", "COD_6"];
const customWhere = [];
function getDicionario() {
    return __awaiter(this, void 0, void 0, function* () {
        const _query = `
    SELECT codigo, UPPER(tipo_info) tipo_info, cod_interno
    FROM public.dicionario
    WHERE codigo NOT IN ('${removeDicionario.join("', '")}')
    ${customWhere.length ? "AND" + customWhere.join(" AND ") : ""}
  `;
        // AND codigo not like 'INFO0%'
        // WHERE indicador = 'PESQUISA' OR capitulo = 'GATILHO'
        const { rows } = yield db_1.default.query(_query);
        return rows;
    });
}
function filterInsertPesquisa(dadosPesquisa) {
    return __awaiter(this, void 0, void 0, function* () {
        const insertGroup = [];
        for (const values of dadosPesquisa) {
            const _queryFromated = (0, pg_format_1.default)(`SELECT cod_municipio FROM dados_pesquisa WHERE cod_municipio = %L AND cod_dicionario = %L AND periodo = %L AND submission_id ${values[3] ? "= %L" : "IS %L"}`, ...values);
            const res = yield db_1.default.query(_queryFromated);
            if (!res.rowCount)
                insertGroup.push(values);
        }
        if (insertGroup.length) {
            const queryFormated = (0, pg_format_1.default)(`INSERT INTO dados_pesquisa (cod_municipio, cod_dicionario, periodo, submission_id, valor, cod_interno) VALUES %L`, insertGroup);
            const resInsertDB = yield db_1.default.query(queryFormated);
            return resInsertDB;
        }
        else {
            return {};
        }
    });
}
function filterInsertSnis(dadosPesquisa) {
    return __awaiter(this, void 0, void 0, function* () {
        const insertGroup = [];
        for (const values of dadosPesquisa) {
            const _queryFromated = (0, pg_format_1.default)(`SELECT cod_municipio FROM dados_snis WHERE cod_municipio = %L AND cod_dicionario = %L AND periodo = %L`, ...values);
            const res = yield db_1.default.query(_queryFromated);
            if (!res.rowCount)
                insertGroup.push(values);
        }
        if (insertGroup.length) {
            const queryFormated = (0, pg_format_1.default)(`INSERT INTO dados_snis (cod_municipio, cod_dicionario, periodo, valor) VALUES %L`, insertGroup);
            const resInsertDB = yield db_1.default.query(queryFormated);
            return resInsertDB;
        }
        else {
            return {};
        }
    });
}
// function splitValue(dados) {
//   const [codMunicipio, codDicionario, ...rest] = dados;
// }
// function validationData(dados) {
//   const [codMunicipio, codDicionario, ...rest] = dados;
//   if (codDicionario === "VPUB020") {
//     return split;
//   }
//   return [dados];
// }
function main(typeImport, periodo = 2021, file) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!file)
            throw new Error("File name is required");
        const fileContent = yield fs_1.promises.readFile(`${dirData}/${file}`);
        const allRecords = (0, sync_1.parse)(fileContent, {
            columns: true,
            skip_empty_lines: true,
            delimiter: ",",
            trim: true,
        });
        const dicionarioData = yield getDicionario();
        const PERIODO = periodo;
        const records = allRecords;
        for (const record of records) {
            const insertGroup = [];
            for (const cod_dicionario in record) {
                if (Object.prototype.hasOwnProperty.call(record, cod_dicionario)) {
                    const dicionario = dicionarioData.find((dic) => dic.codigo == cod_dicionario);
                    if (dicionario) {
                        const conververt = CONVERT_TYPES[dicionario.tipo_info] || CONVER_ALL;
                        const cod_municipio = record["GEOCOD7"] || record["COD_7"];
                        const submission_id = +record["SIDC000"] || null;
                        const valor = record[cod_dicionario].trim();
                        let itemInsert = [];
                        if (typeImport === "SNIS") {
                            itemInsert = [
                                cod_municipio,
                                cod_dicionario,
                                PERIODO,
                                conververt(valor || 0), // valor
                            ];
                        }
                        else {
                            itemInsert = [
                                cod_municipio,
                                cod_dicionario,
                                PERIODO,
                                submission_id,
                                conververt(valor || 0),
                                dicionario.cod_interno, // cod_interno
                            ];
                        }
                        insertGroup.push(itemInsert);
                    }
                }
            }
            if (typeImport === "SNIS") {
                yield filterInsertSnis(insertGroup);
            }
            else {
                yield filterInsertPesquisa(insertGroup);
            }
        }
    });
}
// main("SNIS", 2020);
(() => __awaiter(void 0, void 0, void 0, function* () {
    // await main("CICLOSOFT", 2022, "DB-FINAL-2023-CATADORES.csv");
    // Dados acima de 780448
    // await main("CICLOSOFT", 2022, "CICLOSOFT-2023-DADOS.csv");
    yield main("CICLOSOFT", 2022, "CICLOSOFT-TESTE-2023.csv");
}))();

import path from "path";
import { promises as fsPromises } from "fs";
import { parse } from "csv-parse/sync";
import * as dotenv from "dotenv";
dotenv.config();

import format from "pg-format";
import db from "./db";

import convertTypes from "./utils/convert.columns";

interface IDicionario {
  codigo: string;
  tipo_info: string;
  cod_interno: string;
}

const dirData: string = path.resolve(__dirname, "../data");

const removeDicionario: string[] = ["COD_7", "COD_6"];
const customWhere: string[] = [];

async function getDicionario(): Promise<IDicionario[]> {
  const _query = `
    SELECT codigo, UPPER(tipo_info) tipo_info, cod_interno
    FROM public.dicionario
    WHERE codigo NOT IN ('${removeDicionario.join("', '")}')
    ${customWhere.length ? "AND" + customWhere.join(" AND ") : ""}
  `;
  // AND codigo not like 'INFO0%'
  // WHERE indicador = 'PESQUISA' OR capitulo = 'GATILHO'
  const { rows } = await db.query(_query);
  return rows;
}

async function filterInsertPesquisa(dadosPesquisa: any[][]) {
  const insertGroup = [];

  for (const values of dadosPesquisa) {
    const _queryFromated = format(
      `SELECT cod_municipio FROM dados_pesquisa WHERE cod_municipio = %L AND cod_dicionario = %L AND periodo = %L AND submission_id ${
        values[3] ? "= %L" : "IS %L"
      }`,
      ...values
    );
    const res = await db.query(_queryFromated);
    if (!res.rowCount) insertGroup.push(values);
  }

  if (insertGroup.length) {
    const queryFormated = format(
      `INSERT INTO dados_pesquisa (cod_municipio, cod_dicionario, periodo, submission_id, valor, cod_interno) VALUES %L`,
      insertGroup
    );
    const resInsertDB = await db.query(queryFormated);
    return resInsertDB;
  } else {
    return {};
  }
}

async function filterInsertSnis(dadosPesquisa: any[][]) {
  const insertGroup = [];

  for (const values of dadosPesquisa) {
    const _queryFromated = format(
      `SELECT cod_municipio FROM dados_snis WHERE cod_municipio = %L AND cod_dicionario = %L AND periodo = %L`,
      ...values
    );
    const res = await db.query(_queryFromated);
    if (!res.rowCount) insertGroup.push(values);
  }

  if (insertGroup.length) {
    const queryFormated = format(
      `INSERT INTO dados_snis (cod_municipio, cod_dicionario, periodo, valor) VALUES %L`,
      insertGroup
    );

    const resInsertDB = await db.query(queryFormated);
    return resInsertDB;
  } else {
    return {};
  }
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

async function main(typeImport: string, periodo = 2021, file: string) {
  if (!file) throw new Error("File name is required");

  const fileContent: any = await fsPromises.readFile(`${dirData}/${file}`);

  const allRecords: Array<any> = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
    delimiter: ",",
    trim: true,
  });

  const dicionarioData = await getDicionario();
  const PERIODO = periodo;
  const records = allRecords;
  const promisesAll = []
  for (const record of records) {
    const insertGroup = [];
    for (const cod_dicionario in record) {
      if (Object.prototype.hasOwnProperty.call(record, cod_dicionario)) {
        const dicionario = dicionarioData.find(
          (dic) => dic.codigo == cod_dicionario
        );
        if (dicionario) {
          const conververt = convertTypes(dicionario.tipo_info);

          const cod_municipio = record["GEOCOD7"] || record["COD_7"];
          const submission_id = +record["SIDC000"] || null;
          const valor = record[cod_dicionario].trim();

          let itemInsert = [];
          if (typeImport === "SNIS") {
            itemInsert = [
              cod_municipio, // cod_municipio
              cod_dicionario, // cod_dicionario
              PERIODO, // periodo
              conververt(valor || 0), // valor
            ];
          } else {
            itemInsert = [
              cod_municipio, // cod_municipio
              cod_dicionario, // cod_dicionario
              PERIODO, // periodo
              submission_id, // Cod Submissão
              conververt(valor || 0), // valor
              dicionario.cod_interno, // cod_interno
            ];
          }
          insertGroup.push(itemInsert);
        }
      }
    }

    if (typeImport === "SNIS") {
      await filterInsertSnis(insertGroup);
    } else {
      await filterInsertPesquisa(insertGroup);
    }
  }
  // const a = await Promise.allSettled(promisesAll)
  // console.log(a.filter((value) => value.status === "rejected"));

}

// main("SNIS", 2020);
(async () => {
  await main("CICLOSOFT", 2022, "DB-FINAL-2023-CATADORES.csv");
  await main("CICLOSOFT", 2022, "CICLOSOFT_IBGE_2021.csv");
  await main("CICLOSOFT", 2022, "CICLOSOFT-2023-DADOS.csv");
  // await main("SNIS", 2020, "SNIS-2020-import.csv");
  // await main("CICLOSOFT", 2022, "CICLOSOFT-TESTE-2023.csv"); // Municipio Teste
})();

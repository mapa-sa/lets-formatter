import path from "path";
import { promises as fsPromises } from "fs";
import { parse } from "csv-parse/sync";
import * as dotenv from "dotenv";
dotenv.config();

import format, { literal } from "pg-format";
import db from "./db";

const DIR_DATA = path.resolve(__dirname, "../data");

function checkTypeData(data: Object): string {
  const objectKeys = Object.keys(data);
  if (objectKeys.includes("COD_7")) return "MUNICIPAL";
  if (objectKeys.includes("UF")) return "ESTADUAL";
  return "FEDERAL";
}

function isValidUrl(urlString: string) {
  var urlPattern = new RegExp(
    "^(https?:\\/\\/)?" + // validate protocol
      "((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|" + // validate domain name
      "((\\d{1,3}\\.){3}\\d{1,3}))" + // validate OR ip (v4) address
      "(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*" + // validate port and path
      "(\\?[;&a-z\\d%_.~+=-]*)?" + // validate query string
      "(\\#[-a-z\\d_]*)?$",
    "i"
  ); // validate fragment locator
  return !!urlPattern.test(urlString || "");
}

function removeCaractersNDF(string: string) {
  if (!string) return ''
  return string.normalize('NFD').replace(/[\u0300-\u036f]/g, "");
}

async function insertColeta(data: any) {
  const columns = Object.keys(data).filter((key) => !/^_(.*)|(^id$)/.test(key)); // Remove Skips and id
  const values = columns.map((column) => data[column]);

  const _queryFromated = format(
    `SELECT id FROM marcos_legais
    WHERE
      tipo = %L
      AND ano = %L
      AND unaccent(lower(lei)) = unaccent(lower(%L))
      ${data.estado ? "AND estado = %L" : ""}
      ${data.cod_municipio ? "AND cod_municipio = %L" : ""}
    `,
    ...[data.tipo, data.ano, data.lei, data.estado, data.cod_municipio].filter(
      (val) => val
    )
  );
  const res = await db.query(_queryFromated);

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

    const resInsertDB = await db.query(sqlQuery, values);
    return resInsertDB.rowCount;
  }

  return null;
}

function removeNull(obj: any) {
  return Object.keys(obj).reduce((acc, key) => {
    if (obj[key] != null) {
      acc = {
        ...acc,
        [key]: obj[key],
      };
    }
    return acc;
  }, {});
}

async function main(file: string) {
  if (!file) throw new Error("File name is required");

  const fileContent: any = await fsPromises.readFile(`${DIR_DATA}/${file}`);

  const allRecords: Array<any> = parse(fileContent, {
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
    await insertColeta(data);
  }

  return;
}

(async () => {
  await main("marcos-legais/LEIS-FEDERAL.csv");
  await main("marcos-legais/LEIS-ESTADUAL.csv");
  await main("marcos-legais/LEIS-MUNICIPAL.csv");
})();

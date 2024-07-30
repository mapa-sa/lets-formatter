const REMOVE_VIRCULA = (info: string) => {
  const validPtBr = (info + "").split("").some((letter) => letter === ",");
  if (validPtBr)
    return (info + "")
      .replace(",", "VIRGULA")
      .replace(".", "")
      .replace("VIRGULA", ".");

  return info + "";
};

export const CONVER_INT = (info: string) =>
  Number.isNaN(parseInt(info)) ? "0" : REMOVE_VIRCULA(info);

export const CONVER_FLOAT = (info: string, fix = 2) =>
  Number.isNaN(parseInt(info))
    ? "0"
    : parseFloat(REMOVE_VIRCULA(info)).toFixed(fix);

export const CONVER_ALL = (info: string) => info;

const CONVERT_TYPES: any = {
  INTEIRO: CONVER_INT,
  MONETARIO: CONVER_FLOAT,
  DECIMAL: CONVER_FLOAT,
  NUMERICO: CONVER_FLOAT,
  PERCENTUAL: CONVER_FLOAT,
};

const CONVERT = (typeConvert: string) => {
  if (!typeConvert) throw Error("Necessário passar o tipo");
  return CONVERT_TYPES[typeConvert] || CONVER_ALL;
};

export default CONVERT;

import { Pool, QueryConfig } from "pg";
const pool = new Pool();

export default {
  async query(text: string | QueryConfig<any>, params?: any) {
    const start = Date.now();
    const res = await pool.query(text, params);
    const duration = Date.now() - start;
    console.log("executed query", {
      text,
      params,
      duration,
      rows: res.rowCount,
    });
    return res;
  },
};

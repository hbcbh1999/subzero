import { beforeAll,  afterAll } from '@jest/globals';
import Subzero, { Statement, getIntrospectionQuery, fmtMySqlEnv, Env } from '../rest';
import mysql, {ResultSetHeader} from 'mysql2';
import * as fs from 'fs';
import * as path from 'path';
import { runPemissionsTest, runSelectTest, runUpdateTest, runInsertTest } from './shared/shared'
import dotenv from 'dotenv';
dotenv.config({ path: `${__dirname}/../../../.github/.env`});

const { MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE} = process.env;

const dbPool = mysql.createPool({
  host: 'localhost',
  port: 3306,
  user: MYSQL_USER,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE,
}).promise()
function normalize_statement(s: Statement) {
  return {
    query: s.query.replace(/\s+/g, ' ').trim(),
    parameters: s.parameters,
  };
}
const base_url = 'http://localhost:3000/rest';
//const subzero = new Subzero('postgresql', schema);
let subzero: Subzero;
beforeAll(async () => {
  const db = await dbPool.getConnection();
  const permissions = JSON.parse(fs.readFileSync(path.join(__dirname, 'permissions.json')).toString());
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placeholder_values = new Map<string, any>([['permissions.json', permissions]]);
  const { query, parameters } = getIntrospectionQuery('mysql', 'public', placeholder_values);
  //console.log(query, parameters);
  const [rows] = await db.query(query, parameters);
  //console.log('rows', rows[0])
  //const s = JSON.parse(result.rows[0].json_schema)
  //console.log(s.schemas[0].objects.map((o: any) => o.name))
  const schema = rows[0].json_schema;

  //initialize the subzero instance
  subzero = new Subzero('mysql', schema);
  await db.release();
});

// execute the queries for a given parsed request
async function run(role: string, req: Request, queryEnv?: Env) {
  const method = req.method || 'GET';
  const schema = 'public';
  const env = queryEnv || [];
  const prefix = `/rest/`;
  const { query: envQuery, parameters: envParameters } = fmtMySqlEnv(env);
  
  //console.log(query, parameters)
  let body = null;
  const db = await dbPool.getConnection();
  if (method == 'GET') { 
    let result;
    try {
      const { query, parameters } = await subzero.fmtStatement(schema, prefix, role, req, env);
      //const txMode = method === 'GET' ? 'READ ONLY' : 'READ WRITE';
      await db.query(`BEGIN`);
      await db.query(envQuery, envParameters);
      const [rows] = await db.query(query, parameters);
      result = rows[0];
      await db.query('ROLLBACK');
    } catch (e) {
      await db.query('ROLLBACK');
      throw e;
    } finally {
      db.release();
    }
    body = result.body;
  }
  else {
    try {
      await db.query(`BEGIN`);
      await db.query(envQuery, envParameters);
      const statement = await subzero.fmtTwoStepStatement('public', '/rest/', role, req, env);
      const { query: mutate_query, parameters: mutate_parameters } = statement.fmtMutateStatement();
      const [rows]: ResultSetHeader[] = await db.query(mutate_query, mutate_parameters);
      const { insertId, affectedRows } = rows;
      
      if (insertId > 0 && affectedRows > 0) {
        const ids: number[] = [];
        for (let i = 0; i < affectedRows; i++) {
          ids.push(insertId + i);
        }
        statement.setMutatedRows(ids);
      }
      else {
        const [rows] = await db.query(`
          select t.val 
          from
          json_table(
              @subzero_ids, 
              '$[*]' columns (val integer path '$')
          ) as t
          left join json_table(
              @subzero_ignored_ids, 
              '$[*]' columns (val integer path '$')
          ) as t2 on t.val = t2.val
          where t2.val is null;
        `)
        statement.setMutatedRows(rows);
      }

      const returnRepresentation = req.headers.get('Prefer')?.includes('return=representation');
      if (returnRepresentation) {
        const { query: select_query, parameters: select_parameters } = statement.fmtSelectStatement();
        //console.log(select_query,"\n",select_parameters);
        const [result2] = await db.query(select_query, select_parameters);
        //return JSON.parse(result2[0].body);
        //console.log(result2[0]);
        body = result2[0].body;
      }
      await db.query('ROLLBACK');
    } catch (e) {
      await db.query('ROLLBACK');
      throw e;
    } finally {
      db.release();
    }
  }

  return body ? JSON.parse(body) : null
}


runPemissionsTest('mysql', base_url, run);
runSelectTest('mysql', base_url, run);
runUpdateTest('mysql', base_url, run);
runInsertTest('mysql', base_url, run);

afterAll(async () => {
  await dbPool.end();
  
});
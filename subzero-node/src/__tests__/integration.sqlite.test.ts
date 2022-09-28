
import { expect, test, beforeAll, afterAll } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import * as sqlite3 from 'sqlite3'
import { open, Database } from 'sqlite'
import { Subzero, get_introspection_query } from '../index';

// Declare global variables
sqlite3.verbose();
let db: Database<sqlite3.Database>;
let subzero: Subzero;

beforeAll(async () => {
    db = await open({ filename: ':memory:', driver: sqlite3.Database });
    // Read the init SQL file
    const loadSql = fs.readFileSync(path.join(__dirname, 'load.sql')).toString().split(';');
    
    loadSql.forEach(async (sql) => await db.exec(sql));

    // note: although we have a second parameter that lists the schemas
    // in case of sqlite it is not used in the query (sqlite does not have the notion of schemas)
    // that is why we don't read/use the `parameters` from the result
    let { query } = get_introspection_query('sqlite', 'public');
    let result = await db.get(query);
    let schema = JSON.parse(result.json_schema);

    //initialize the subzero instance
    subzero = new Subzero('sqlite', schema);
});

test('sqlite', async () => {
    expect(true).toBe(true);
});

afterAll(async () => {
    await db.close();
});
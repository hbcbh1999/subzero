
import { expect, test, beforeAll, afterAll, describe } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';
import * as sqlite3 from 'sqlite3'
import { open, Database } from 'sqlite'
import { Subzero, Statement, get_introspection_query } from '../index';

// Declare global variables
sqlite3.verbose();
let db: Database<sqlite3.Database>;
let subzero: Subzero;

function normalize_statement(s : Statement) {
    return {
        query: s.query.replace(/\s+/g, ' ').trim(),
        parameters: s.parameters
    };
}
const base_url = 'http://localhost:3000/rest';

beforeAll(async () => {
    db = await open({ filename: ':memory:', driver: sqlite3.Database });
    db.loadExtension('carray');
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

    // let t = await db.all('select * from projects where id in (select value from json_each($1))', ['[1, 2, 3]']);
    // console.log('test', t);
});

// execute teh queries for a given parsed request
async function run(request: Request) {
    const subzeroRequest = await subzero.parse('public', '/rest/', request);
    if (request.method == 'GET') {
        let { query, parameters } = subzero.fmt_sqlite_mutate_query(subzeroRequest, []);
        //console.log(query,"\n",parameters);
        let result = await db.get(query, parameters);
        //console.log(result);
        return JSON.parse(result.body);
    }
    else {
        let { query:mutate_query, parameters:mutate_parameters } = subzero.fmt_sqlite_mutate_query(subzeroRequest, []);
        let result = await db.all(mutate_query, mutate_parameters);
        let ids = result.map(r => r.rowid);
        let { query: select_query, parameters: select_parameters } = subzero.fmt_sqlite_second_stage_select(subzeroRequest, ids, []);
        let result2 = await db.get(select_query, select_parameters);
        return JSON.parse(result2.body);
    }
}

describe('select', () => {

    test('simple', async () => {
        expect(await run(new Request(`${base_url}/tbl1?select=one,two`)))
        .toStrictEqual([
            {"one":"hello!","two":10},
            {"one":"goodbye","two":20}
        ])
    });

    test('with cast', async () => {
        expect(await run(new Request(`${base_url}/tbl1?select=one,two::text`)))
        .toStrictEqual([
            {"one":"hello!","two":"10"},
            {"one":"goodbye","two":"20"}
        ])
    });

    test("filter with in", async () => {
        expect(await run(new Request(`${base_url}/projects?select=id&id=in.(1,2)`)))
        .toStrictEqual([
            { "id": 1 },
            { "id": 2 }
        ])
    });

    test("children and parent", async () => {
        expect(await run(new Request(`${base_url}/projects?select=id,name,client:clients(id,name),tasks(id,name)&id=in.(1,2)`)))
        .toStrictEqual([
            { "id": 1, "name": "Windows 7", "tasks": [{ "id": 1, "name": "Design w7" }, { "id": 2, "name": "Code w7" }], "client": { "id": 1, "name": "Microsoft" } },
            { "id": 2, "name": "Windows 10", "tasks": [{ "id": 3, "name": "Design w10" }, { "id": 4, "name": "Code w10" }], "client": { "id": 1, "name": "Microsoft" } }
        ])
    });
});

test('insert query', async () => {
    const request = await subzero.parse('public', '/rest/', new Request(
        `${base_url}/clients?select=id,name`,
        {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Prefer':'return=representation,count=exact'
            },
            body: JSON.stringify({name:'new client'})
        }
    ));

    expect(
        normalize_statement(subzero.fmt_sqlite_mutate_query(request,[["env_var", "env_value"]]))
    )
    .toStrictEqual(
        normalize_statement({
            query: `
            with
                env as materialized (select $1 as "env_var") ,
                subzero_payload as ( select $2 as json_data ),
                subzero_body as ( 
                    select json_extract(value, '$.name') as "name"
                    from (select value from json_each(( select case when json_type(json_data) = 'array' then json_data else json_array(json_data) end as val from subzero_payload )))
                )
            insert into "clients" ("name")
            select "name" from subzero_body _ 
            where true
            returning "rowid", 1 as _subzero_check__constraint
            `,
            parameters: ["env_value",'{"name":"new client"}']
        })
    );

    expect(
        normalize_statement(subzero.fmt_sqlite_second_stage_select(request,['1'],[["env_var", "env_value"]]))
    )
    .toStrictEqual(
        normalize_statement({
            query: `
            with 
                env as materialized (select $1 as "env_var"),
                _subzero_query as (
                    select json_object('id', "clients"."id", 'name', "clients"."name") as row 
                    from "clients", env
                    where "clients"."rowid" in ( select value from json_each($2) )
                ) ,
                _subzero_count_query as (
                    select 1 from "clients"
                    where "clients"."rowid" in ( select value from json_each($3) )
                )
            select
                count(_subzero_t.row) AS page_total,
                (SELECT count(*) FROM _subzero_count_query) as total_result_set,
                json_group_array(json(_subzero_t.row)) as body,
                null as response_headers,
                null as response_status
            from ( select * from _subzero_query ) _subzero_t
            `,
            parameters: ['env_value', '["1"]','["1"]']
        })
    );
});

afterAll(async () => {
    await db.close();
});
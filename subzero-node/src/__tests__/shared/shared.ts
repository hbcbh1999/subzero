import { expect, test, describe } from '@jest/globals';
import { Env } from '../../nodejs';

type RunFn = (role: string, request: Request, env?: Env) => Promise<unknown>;

// function normalize_statement(s: Statement) {
//     return {
//         query: s.query.replace(/\s+/g, ' ').trim(),
//         parameters: s.parameters,
//     };
// }

export async function runPemissionsTest(base_url: string, run: RunFn) {
    describe('permissions', () => {
        test('alice can select public rows and her private rows', async () => {
            expect(
                await run('alice', new Request(`${base_url}/permissions_check?select=id,value,hidden,role`), [
                    ['request.jwt.claims', JSON.stringify({ role: 'alice' })],
                ]),
            ).toStrictEqual([
                { id: 1, value: 'One Alice Public', hidden: 'Hidden', role: 'alice' },
                { id: 2, value: 'Two Bob Public', hidden: 'Hidden', role: 'bob' },
                { id: 3, value: 'Three Charlie Public', hidden: 'Hidden', role: 'charlie' },
                { id: 10, value: 'Ten Alice Private', hidden: 'Hidden', role: 'alice' },
                { id: 11, value: 'Eleven Alice Private', hidden: 'Hidden', role: 'alice' },
            ]);
        });
    });
}

export async function runSelectTest(base_url: string, run: RunFn) {
    describe('select', () => {
        test('simple', async () => {
            expect(await run('anonymous', new Request(`${base_url}/tbl1?select=one,two`))).toStrictEqual([
                { one: 'hello!', two: 10 },
                { one: 'goodbye', two: 20 },
            ]);
        });

        test('with cast', async () => {
            expect(await run('anonymous', new Request(`${base_url}/tbl1?select=one,two::text`))).toStrictEqual([
                { one: 'hello!', two: '10' },
                { one: 'goodbye', two: '20' },
            ]);
        });

        test('filter with in', async () => {
            expect(await run('anonymous', new Request(`${base_url}/projects?select=id&id=in.(1,2)`))).toStrictEqual([
                { id: 1 },
                { id: 2 },
            ]);
        });

        test('children and parent', async () => {
            expect(
                await run(
                    'anonymous',
                    new Request(`${base_url}/projects?select=id,name,client:clients(id,name),tasks(id,name)&id=in.(1,2)`),
                ),
            ).toStrictEqual([
                {
                    id: 1,
                    name: 'Windows 7',
                    tasks: [
                        { id: 1, name: 'Design w7' },
                        { id: 2, name: 'Code w7' },
                    ],
                    client: { id: 1, name: 'Microsoft' },
                },
                {
                    id: 2,
                    name: 'Windows 10',
                    tasks: [
                        { id: 3, name: 'Design w10' },
                        { id: 4, name: 'Code w10' },
                    ],
                    client: { id: 1, name: 'Microsoft' },
                },
            ]);
        });
    });
}

// export async function runInsertTest(base_url: string, run: RunFn) {
//     describe('insert', () => {
//         test('insert query', async () => {
//             const statement = await subzero.fmtSqliteTwoStepStatement(
//                 'public',
//                 '/rest/',
//                 'anonymous',
//                 new Request(`${base_url}/clients?select=id,name`, {
//                     method: 'POST',
//                     headers: {
//                         'Content-Type': 'application/json',
//                         Prefer: 'return=representation,count=exact',
//                     },
//                     body: JSON.stringify({ name: 'new client' }),
//                 }),
//                 [['env_var', 'env_value']],
//             );

//             expect(normalize_statement(statement.fmtMutateStatement())).toStrictEqual(
//                 normalize_statement({
//                     query: `
//                   with
//                       env as materialized (select ? as "env_var") ,
//                       subzero_payload as ( select ? as json_data ),
//                       subzero_body as ( 
//                           select json_extract(value, '$.name') as "name"
//                           from (select value from json_each(( select case when json_type(json_data) = 'array' then json_data else json_array(json_data) end as val from subzero_payload )))
//                       )
//                   insert into "clients" ("name")
//                   select "name" from subzero_body _ 
//                   where true
//                   returning "rowid", ((true)) as _subzero_check__constraint
//                   `,
//                     parameters: ['env_value', '{"name":"new client"}'],
//                 }),
//             );
//             statement.setMutatedRows([{ rowid: 1, _subzero_check__constraint: 1 }]);
//             expect(normalize_statement(statement.fmtSelectStatement())).toStrictEqual(
//                 normalize_statement({
//                     query: `
//                   with 
//                       env as materialized (select ? as "env_var"),
//                       _subzero_query as (
//                           select json_object('id', "subzero_source"."id", 'name', "subzero_source"."name") as row 
//                           from "clients" as "subzero_source", env
//                           where "subzero_source"."rowid" in ( select value from json_each(?) )
//                       ) ,
//                       _subzero_count_query as (
//                           select 1 from "clients"
//                           where "clients"."rowid" in ( select value from json_each(?) )
//                       )
//                   select
//                       count(_subzero_t.row) AS page_total,
//                       (SELECT count(*) FROM _subzero_count_query) as total_result_set,
//                       json_group_array(json(_subzero_t.row)) as body,
//                       null as response_headers,
//                       null as response_status
//                   from ( select * from _subzero_query ) _subzero_t
//                   `,
//                     parameters: ['env_value', '["1"]', '["1"]'],
//                 }),
//             );
//         });
//     });
// }

export async function runUpdateTest(base_url: string, run: RunFn) {
    describe('update', () => {
        test('basic no representation', async () => {
            expect(
                await run(
                    'anonymous',
                    new Request(`${base_url}/tasks?id=eq.1`, {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ name: 'Design w7 updated' }),
                    }),
                ),
            ).toBeNull();
        });
        test('basic with representation', async () => {
            expect(
                await run(
                    'anonymous',
                    new Request(`${base_url}/tasks?select=id,name&id=in.(1,3)`, {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json', 'Prefer': 'return=representation, count=exact' },
                        body: JSON.stringify({ name: 'updated' }),
                    }),
                ),
            ).toStrictEqual([
                { id: 1, name: 'updated' },
                { id: 3, name: 'updated' },
            ]);
        });
        test('with embedding', async () => {
            expect(
                await run(
                    'anonymous',
                    new Request(`${base_url}/projects?select=id,name,client:clients(id),tasks(id)&id=in.(1,3)`, {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json', Prefer: 'return=representation, count=exact' },
                        body: JSON.stringify({ name: 'updated' }),
                    }),
                ),
            ).toStrictEqual([
                { id: 1, name: 'updated', client: { id: 1 }, tasks: [{ id: 1 }, { id: 2 }] },
                { id: 3, name: 'updated', client: { id: 2 }, tasks: [{ id: 5 }, { id: 6 }] },
            ]);
        });
        test('with embedding many to many', async () => {
            expect(
                await run(
                    'anonymous',
                    new Request(`${base_url}/tasks?select=id,name,project:projects(id),users(id,name)&id=in.(1,3)`, {
                        method: 'PATCH',
                        headers: {
                            Accept: 'application/json',
                            'Content-Type': 'application/json',
                            Prefer: 'return=representation, count=exact',
                        },
                        body: JSON.stringify({ name: 'updated' }),
                    }),
                ),
            ).toStrictEqual([
                {
                    id: 1,
                    name: 'updated',
                    project: { id: 1 },
                    users: [
                        { id: 1, name: 'Angela Martin' },
                        { id: 3, name: 'Dwight Schrute' },
                    ],
                },
                { id: 3, name: 'updated', project: { id: 2 }, users: [{ id: 1, name: 'Angela Martin' }] },
            ]);
        });
    });
}

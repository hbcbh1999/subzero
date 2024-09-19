// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
import { expect, test, describe } from '@jest/globals';
import { Env } from '../../rest';

type RunFn = (role: string, request: Request, env?: Env) => Promise<unknown>;

// function normalize_statement(s: Statement) {
//     return {
//         query: s.query.replace(/\s+/g, ' ').trim(),
//         parameters: s.parameters,
//     };
// }

export async function runPermissionsTest(db_type: string, base_url: string, run: RunFn) {
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

export async function runSelectTest(db_type: string, base_url: string, run: RunFn) {
    describe('select', () => {
        test('simple', async () => {
            expect(await run('anonymous', new Request(`${base_url}/tbl1?select=one,two`))).toStrictEqual([
                { one: 'hello!', two: 10 },
                { one: 'goodbye', two: 20 },
            ]);
        });

        const castTo = db_type === 'mysql'?'char':'text';
        test('with cast', async () => {
            expect(await run('anonymous', new Request(`${base_url}/tbl1?select=one,two::${castTo}`))).toStrictEqual([
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

    // it "basic with representation" $ do
    //     request methodPost "/clients?select=id,name"
    //       [("Prefer", "return=representation,count=exact")]
    //       [json|r#"{"name":"new client"}"#|]
    //       shouldRespondWith
    //       [json|r#"[{"id":3,"name":"new client"}]"#|]
    //       { matchStatus  = 201
    //         , matchHeaders = [ "Content-Type" <:> "application/json"
    //                          //, "Location" <:> "/projects?id=eq.6"
    //                          , "Content-Range" <:> "*/1" ]
    //       }
    // it "basic no representation" $ do
    //     request methodPost "/projects"
    //       [json|r#"{"name":"new project"}"#|]
    //       shouldRespondWith
    //       [text|""|]
    //       { matchStatus  = 201
    //         , matchHeaders = [ "Content-Type" <:> "application/json"
    //                           //, "Location" <:> "/projects?id=eq.6"
    //                           , "Content-Range" <:> "*/*" ]
    //       }
export async function runInsertTest(db_type: string, base_url: string, run: RunFn) {
    describe('insert', () => {
        test('basic with representation', async () => {
            const expected = [{ name: 'new client' }];
            expect(
                await run(
                    'anonymous',
                    new Request(`${base_url}/clients?select=name`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'Prefer': 'return=representation,count=exact' },
                        body: JSON.stringify({"name":"new client"}),
                    }),
                ),
            ).toStrictEqual(expected);
        });
        test('basic no representation', async () => {
            expect(
                await run(
                    'anonymous',
                    new Request(`${base_url}/projects`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({"name":"new project"}),
                    }),
                ),
            ).toBeNull();
        });
    });
}


export async function runUpdateTest(db_type: string, base_url: string, run: RunFn) {
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

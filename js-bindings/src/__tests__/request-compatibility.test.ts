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
import { expect, test, beforeAll, jest, afterAll } from '@jest/globals';

import Subzero from '../rest';
import type { NextApiRequest} from "next";
import { createRequest } from "node-mocks-http";
type ApiRequest = NextApiRequest & ReturnType<typeof createRequest>;

const schema = {
  schemas: [
    {
      name: 'public',
      objects: [
        {
          kind: 'view',
          name: 'tasks',
          columns: [
            {
              name: 'id',
              data_type: 'int',
              primary_key: true,
            },
            {
              name: 'name',
              data_type: 'text',
            },
          ],
          foreign_keys: [
            {
              name: 'project_id_fk',
              table: ['api', 'tasks'],
              columns: ['project_id'],
              referenced_table: ['api', 'projects'],
              referenced_columns: ['id'],
            },
          ],
          permissions: [
            {
              "role": "public",
              "grant": ["all"]
            }
          ]
        },
        {
          kind: 'table',
          name: 'projects',
          columns: [
            {
              name: 'id',
              data_type: 'int',
              primary_key: true,
            },
          ],
          foreign_keys: [],
          permissions: [
            {
              "role": "public",
              "grant": ["all"]
            }
          ]
        },
      ],
    },
  ],
};

const base_url = 'http://localhost:3000/rest';
let subzero: Subzero; 
beforeAll(async () => {
  console.warn = jest.fn();
  //jest.useFakeTimers();
  subzero = new Subzero('postgresql', schema);
});
afterAll(() => {
  jest.runOnlyPendingTimers();
  subzero.free();
  //jest.useRealTimers();
});

test('dummy', async () => {
  expect(true).toBeTruthy()
});
test('fetch request', async () => {
  expect(await subzero.fmtStatement('public', '/rest/', 'anonymous',
    new Request(`${base_url}/tasks?id=eq.1`, {
      method: 'GET',
      headers: {
        Accept: 'application/json',
      },
    }),
    []
  )).toBeTruthy()
});

test('node request get', async () => {
  expect(await subzero.fmtStatement('public', '/rest/', 'anonymous',
    createRequest<ApiRequest>({
      method: 'GET',
      url: `${base_url}/tasks?id=eq.1`,
      headers: {
        Accept: 'application/json',
      }
    }),
    []
  )).toBeTruthy()
});

test('node request post', async () => {
  const request = createRequest<ApiRequest>({
      method: 'POST',
      url: `${base_url}/tasks`,
      headers: {
        Accept: 'application/json',
      },
      body: {"id": 1, "name": "test"}
  })
  expect(await subzero.fmtStatement('public', '/rest/', 'anonymous',request, [])).toBeTruthy()
});

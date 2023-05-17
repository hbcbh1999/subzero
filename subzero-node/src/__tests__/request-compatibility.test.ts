import { expect, test } from '@jest/globals';

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
        },
      ],
    },
  ],
};

const base_url = 'http://localhost:3000/rest';
const subzero = new Subzero('postgresql', schema);
// beforeAll(async () => {
//   await subzero.init();
// });

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

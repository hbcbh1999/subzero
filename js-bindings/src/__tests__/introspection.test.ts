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
import { expect, test } from '@jest/globals';
import { getRawIntrospectionQuery, getIntrospectionQuery } from '../rest';
import * as fs from 'fs';
import { beforeAll, jest, afterAll } from '@jest/globals';
beforeAll(() => {
  console.warn = jest.fn();
  //jest.useFakeTimers();
});
afterAll(() => {
  jest.runOnlyPendingTimers();
  //jest.useRealTimers();
});

test('getRawIntrospectionQuery', () => {
  expect(getRawIntrospectionQuery('postgresql')).toStrictEqual(
    fs.readFileSync(`introspection/postgresql_introspection_query.sql`, 'utf8'),
  );
});

test('getIntrospectionQuery', () => {
  const statement = getIntrospectionQuery('sqlite', 'public');
  expect(statement.query).toStrictEqual(fs.readFileSync(`src/__tests__/expected_sqlite_introspection_query.sql`, 'utf8'));
  expect(statement.parameters).toStrictEqual([['["public"]']]);
});

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
  jest.useRealTimers();
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

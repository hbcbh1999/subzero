import { expect, test } from '@jest/globals';
import { getRawIntrospectionQuery, getIntrospectionQuery } from '../index';
import * as fs from 'fs';

test('getRawIntrospectionQuery', () => {
  expect(getRawIntrospectionQuery('postgresql')).toStrictEqual(
    fs.readFileSync(`introspection/postgresql_introspection_query.sql`, 'utf8'),
  );
});

test('getIntrospectionQuery', () => {
  expect(getIntrospectionQuery('sqlite', 'public')).toStrictEqual({
    query: fs.readFileSync(`src/__tests__/expected_sqlite_introspection_query.sql`, 'utf8'),
    parameters: [['public']],
  });
});

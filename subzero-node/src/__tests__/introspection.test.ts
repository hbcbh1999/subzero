import { expect, test } from '@jest/globals';
import { get_raw_introspection_query, get_introspection_query } from '../index';
import * as fs from 'fs';

test('get_raw_introspection_query', () => {
  expect(get_raw_introspection_query('postgresql')).toStrictEqual(
    fs.readFileSync(`introspection/postgresql_introspection_query.sql`, 'utf8'),
  );
});

test('get_introspection_query', () => {
  expect(get_introspection_query('sqlite', 'public')).toStrictEqual({
    query: fs.readFileSync(`src/__tests__/expected_sqlite_introspection_query.sql`, 'utf8'),
    parameters: [['public']],
  });
});

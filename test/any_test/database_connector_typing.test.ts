import test from 'node:test';
import type * as BetterSqlite3 from 'better-sqlite3';
import type * as SQLite3 from 'sqlite3';

import {
  WorkerProcedureCall,
  type database_connector_definition_t,
  type define_database_connection_params_t
} from '../../src/index';

declare global {
  interface wpc_dependency_by_alias_i {
    path_dep_typed: typeof import('node:path');
  }

  interface wpc_database_connection_type_by_name_i {
    sqlite_connection_1: 'sqlite';
  }

  interface wpc_database_connection_handle_by_name_i {
    sqlite_better_connection_1: BetterSqlite3.Database;
  }
}

const mongodb_connection_params = {
  name: 'mongodb_connection_1',
  connector: {
    type: 'mongodb',
    semantics: {
      uri: 'mongodb://127.0.0.1:27017',
      database_name: 'app_db',
      client_options: {
        maxPoolSize: 10
      }
    }
  }
} satisfies define_database_connection_params_t<'mongodb'>;

const sqlite_connection_params = {
  name: 'sqlite_connection_1',
  connector: {
    type: 'sqlite',
    semantics: {
      filename: ':memory:',
      driver: 'better-sqlite3'
    }
  }
} satisfies define_database_connection_params_t<'sqlite'>;

const mongodb_connector_definition = {
  type: 'mongodb',
  semantics: {
    uri: 'mongodb://127.0.0.1:27017'
  }
} satisfies database_connector_definition_t;

const method_param_shape = {
  name: 'mongodb_connection_2',
  connector: {
    type: 'mongodb',
    semantics: {
      uri: 'mongodb://127.0.0.1:27017'
    }
  }
} satisfies Parameters<WorkerProcedureCall['defineDatabaseConnection']>[0];

const invalid_mongodb_connection_params = {
  name: 'invalid_mongodb_connection',
  connector: {
    type: 'mongodb',
    semantics: {
      uri: 'mongodb://127.0.0.1:27017',
      // @ts-expect-error mongodb semantics do not include sqlite filename.
      filename: ':memory:'
    }
  }
} satisfies define_database_connection_params_t;

async function AssertTypedSqliteConnectionWithoutCast(): Promise<void> {
  const sqlite_database = await wpc_database_connection('sqlite_connection_1');
  sqlite_database.exec('SELECT 1');

  const select_statement = sqlite_database.prepare('SELECT 1 as value');
  select_statement.get();
}

async function AssertSqliteConnectionUsesPackageTypes(): Promise<void> {
  const sqlite_database = await wpc_database_connection('sqlite_connection_1');
  const sqlite_database_package_type: BetterSqlite3.Database | SQLite3.Database =
    sqlite_database;

  void sqlite_database_package_type;
}

async function AssertGenericLookupWorksWithoutGlobalMapping(): Promise<void> {
  const sqlite_database = await wpc_database_connection<BetterSqlite3.Database>({
    name: 'sqlite_connection_generic_1',
    type: 'sqlite'
  });

  const statement = sqlite_database.prepare<unknown[], { value: number }>(
    'SELECT 1 as value'
  );
  statement.get();
}

async function AssertInvalidSqliteMethodFailsTypeCheck(): Promise<void> {
  const sqlite_database = await wpc_database_connection('sqlite_connection_1');
  // @ts-expect-error sqlite handles do not include this method.
  sqlite_database.method_that_does_not_exist();
}

async function AssertUnknownConnectionNameFallsBackToUnknown(params: {
  connection_name: string;
}): Promise<void> {
  const unknown_database = await wpc_database_connection(params.connection_name);
  // @ts-expect-error unknown fallback should not allow method access.
  unknown_database.exec('SELECT 1');
}

async function AssertNameSpecificHandleOverrideWorks(): Promise<void> {
  const sqlite_database = await wpc_database_connection('sqlite_better_connection_1');
  const count_row = sqlite_database
    .prepare<unknown[], { total_record_count: number }>(
      'SELECT 1 as total_record_count'
    )
    .get();

  void count_row?.total_record_count;
}

async function AssertDependencyAliasInferenceWorks(): Promise<void> {
  const path_module = await wpc_import<typeof import('node:path')>({
    alias: 'path_dep_typed'
  });
  path_module.basename('/tmp/path_dep_typed.txt');
}

async function AssertDependencyObjectAliasInferenceWorks(): Promise<void> {
  const path_module = await wpc_import({ alias: 'path_dep_typed' });
  path_module.basename('/tmp/path_dep_object.txt');
}

async function AssertDependencyGenericLookupWorksWithoutMapping(): Promise<void> {
  const crypto_module = await wpc_import<typeof import('node:crypto')>({
    alias: 'crypto_dep_typed'
  });
  crypto_module.randomUUID();
}

async function AssertDependencyInvalidMethodFailsTypeCheck(): Promise<void> {
  const path_module = await wpc_import<typeof import('node:path')>({
    alias: 'path_dep_typed'
  });
  // @ts-expect-error path module does not include this method.
  path_module.method_that_does_not_exist();
}

async function AssertDependencyDynamicAliasFallsBackToUnknown(params: {
  dependency_alias: string;
}): Promise<void> {
  const unknown_dependency = await wpc_import(params.dependency_alias);
  // @ts-expect-error unknown fallback should not allow method access.
  unknown_dependency.basename('/tmp/value');
}

async function AssertSharedAccessGenericTypingWorks(): Promise<void> {
  const shared_object = await wpc_shared_access<{
    count: number;
    label: string;
  }>({
    id: 'typed_shared_object_1'
  });

  shared_object.count.toFixed(0);
  shared_object.label.toUpperCase();
}

async function AssertSharedUnknownFallbackIsSafe(): Promise<void> {
  const shared_value = await wpc_shared_access({
    id: 'typed_shared_unknown_1'
  });

  // @ts-expect-error unknown fallback should not allow property access.
  shared_value.method_that_does_not_exist();
}

async function AssertSharedWriteGenericTypingWorks(): Promise<void> {
  await wpc_shared_write<{ count: number }>({
    id: 'typed_shared_write_1',
    content: {
      count: 1
    }
  });

  await wpc_shared_release({
    id: 'typed_shared_write_1'
  });

  await wpc_shared_free({
    id: 'typed_shared_write_1'
  });
}

async function AssertSharedWriteInvalidShapeFailsTypeCheck(): Promise<void> {
  await wpc_shared_write<{ count: number }>({
    id: 'typed_shared_write_invalid_1',
    content: {
      // @ts-expect-error shared write content shape does not match generic type.
      wrong: 'value'
    }
  });
}

test('database connector typing assertions compile', function () {
  const workerprocedurecall = new WorkerProcedureCall();

  void workerprocedurecall;
  void mongodb_connection_params;
  void sqlite_connection_params;
  void mongodb_connector_definition;
  void method_param_shape;
  void invalid_mongodb_connection_params;
  void AssertTypedSqliteConnectionWithoutCast;
  void AssertSqliteConnectionUsesPackageTypes;
  void AssertGenericLookupWorksWithoutGlobalMapping;
  void AssertInvalidSqliteMethodFailsTypeCheck;
  void AssertUnknownConnectionNameFallsBackToUnknown;
  void AssertNameSpecificHandleOverrideWorks;
  void AssertDependencyAliasInferenceWorks;
  void AssertDependencyObjectAliasInferenceWorks;
  void AssertDependencyGenericLookupWorksWithoutMapping;
  void AssertDependencyInvalidMethodFailsTypeCheck;
  void AssertDependencyDynamicAliasFallsBackToUnknown;
  void AssertSharedAccessGenericTypingWorks;
  void AssertSharedUnknownFallbackIsSafe;
  void AssertSharedWriteGenericTypingWorks;
  void AssertSharedWriteInvalidShapeFailsTypeCheck;
});

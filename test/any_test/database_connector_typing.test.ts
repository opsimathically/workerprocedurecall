import test from 'node:test';
import type { Database as better_sqlite3_database_t } from 'better-sqlite3';
import type { Database as sqlite3_database_t } from 'sqlite3';

import {
  WorkerProcedureCall,
  type database_connector_definition_t,
  type define_database_connection_params_t
} from '../../src/index';

declare global {
  interface wpc_database_connection_type_by_name_i {
    sqlite_connection_1: 'sqlite';
  }

  interface wpc_database_connection_handle_by_name_i {
    sqlite_better_connection_1: better_sqlite3_database_t;
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
  const sqlite_database_package_type: better_sqlite3_database_t | sqlite3_database_t =
    sqlite_database;

  void sqlite_database_package_type;
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
  void AssertInvalidSqliteMethodFailsTypeCheck;
  void AssertUnknownConnectionNameFallsBackToUnknown;
  void AssertNameSpecificHandleOverrideWorks;
});

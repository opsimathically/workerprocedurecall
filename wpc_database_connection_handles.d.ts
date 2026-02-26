import type { Database as better_sqlite3_database_t } from 'better-sqlite3';

declare global {
  interface wpc_database_connection_handle_by_name_i {
    db_connection_1: better_sqlite3_database_t;
  }
}

export {};

/* eslint-disable */
require('./support/env');
const assert = require('assert');
const sqb = require('sqb');
const createTestTables = require('./support/createTestTables');
const waterfall = require('putil-waterfall');

sqb.use(require('../'));

describe('sqb-connect-oracle', function() {

  let pool;
  let client1;
  let schema;
  let table;
  let metaData;

  after(function() {
    if (pool)
      pool.close(true);
  });

  describe('Driver', function() {

    it('should initialize pool with pg driver', function() {
      pool = sqb.pool({
        dialect: 'oracle',
        user: (process.env.DB_USER || 'SYSDBA'),
        password: (process.env.DB_PASS || ''),
        host: (process.env.DB_HOST || 'localhost'),
        port: (parseInt(process.env.DB_PORT, 10) || 1521),
        database: (process.env.DB || 'test'),
        pool: {
          validate: true,
          max: 1
        },
        defaults: {
          naming: 'lowercase',
          objectRows: true,
          autoCommit: false
        }
      });
      assert(pool.dialect, 'pg');
    });

    it('should test pool', function() {
      return pool.test();
    });

    it('should create a connection', function() {
      return pool.acquire(connection => {
        client1 = connection._client; // Will be used later
      });
    });

    if (!process.env.SKIP_CREATE_TABLES) {
      it('create test tables', function() {
        this.slow(4000);
        return pool.acquire(connection => {
          return createTestTables(connection._client.intlcon);
        });
      }).timeout(4000);
    }

    it('should fetch "airports" table (objectRows=false)', function() {
      return pool.select()
          .from('sqb_test.airports')
          .limit(100)
          .orderBy(['id'])
          .execute({objectRows: false}).then(result => {
            const rows = result.rows;
            assert(rows);
            assert.equal(rows.length, 100);
            assert.equal(rows[0][0], 'AIGRE');
          });
    });

    it('should fetch "airports" table (objectRows=true)', function() {
      return pool.select()
          .from('sqb_test.airports')
          .limit(100)
          .orderBy(['id'])
          .execute().then(result => {
            const rows = result.rows;
            assert(rows);
            assert.equal(rows.length, 100);
            assert.equal(rows[0].id, 'AIGRE');
          });
    });

    it('should fetch test table (cursor)', function() {
      this.slow(200);
      return pool.select()
          .from('sqb_test.airports')
          .orderBy(['id'])
          .execute({objectRows: false, cursor: true, fetchRows: 100})
          .then(result => {
            const cursor = result.cursor;
            assert(cursor);
            return cursor.next().then((row) => {
              assert.equal(cursor.row, row);
              assert.equal(cursor.row[0], 'AIGRE');
              return cursor.close();
            });
          });
    });

    it('should fetch test table (cursor, objectRows)', function() {
      this.slow(200);
      return pool.select()
          .from('sqb_test.airports')
          .orderBy(['id'])
          .execute({
            cursor: true,
            fetchRows: 100
          }).then(result => {
            const cursor = result.cursor;
            assert(cursor);
            return cursor.next().then((row) => {
              assert.equal(cursor.row, row);
              assert.equal(cursor.row.id, 'AIGRE');
              return cursor.close();
            });
          });
    });

    it('should invalid sql return error', function(done) {
      pool.execute('invalid sql').then(() => {
        done(new Error('Failed'));
      }).catch(() => done());
    });

    it('should call startTransaction more than one', function() {
      return pool.acquire(connection => {
        return waterfall([
          () => connection.startTransaction(),
          () => connection.startTransaction()
        ]);
      });
    });

    it('should call commit more than one', function() {
      return pool.acquire(connection => {
        return waterfall([
          () => connection.startTransaction(),
          () => connection.commit(),
          () => connection.commit()
        ]);
      });
    });

    it('should call rollback more than one', function() {
      return pool.acquire(connection => {
        return waterfall([
          () => connection.startTransaction(),
          () => connection.rollback(),
          () => connection.rollback()
        ]);
      });
    });

    it('should start transaction when autoCommit is off', function() {
      return pool.acquire(conn => {
        return waterfall([
          () => conn.update('sqb_test.airports', {Catalog: 1234})
              .where({ID: 'LFOI'})
              .execute(),
          () => conn.rollback(),
          () => conn.select()
              .from('sqb_test.airports')
              .where({ID: 'LFOI'})
              .execute({objectRows: true}).then((result) => {
                assert.notEqual(result.rows[0].Catalog, 1234);
              })
        ]);
      });
    });

    it('should not start transaction when autoCommit is on', function() {
      return pool.acquire({autoCommit: true}, conn => {
        return waterfall([
          () => conn.update('sqb_test.airports', {catalog: 1234})
              .where({ID: 'LFOI'})
              .execute(),
          () => conn.rollback(),
          () => conn.select()
              .from('sqb_test.airports')
              .where({ID: 'LFOI'})
              .execute({objectRows: true}).then((result) => {
                assert.equal(result.rows[0].catalog, 1234);
              })
        ]);
      });
    });

  });

  describe('Meta-Data', function() {

    it('should initialize DBMeta', function() {
      metaData = new sqb.DBMeta(pool);
      metaData.invalidate();
    });

    it('should select schemas', function() {
      return metaData.select()
          .from('schemas')
          .where({schema_name: 'SQB_TEST'})
          .execute().then(result => {
            assert.equal(result.rows.length, 1);
          });
    });

    it('should select tables', function() {
      this.slow(100);
      return metaData.select()
          .from('tables')
          .where({schema_name: 'SQB_TEST'})
          .execute().then(result => {
            assert.equal(result.rows.length, 2);
            assert.equal(result.rows[0].table_name, 'AIRPORTS');
          });
    });

    it('should select columns', function() {
      assert.equal(pool.acquired, 0);
      this.slow(100);
      return metaData.select()
          .from('columns')
          .where({schema_name: 'SQB_TEST', table_name: 'AIRPORTS'})
          .execute().then(result => {
            assert.equal(result.rows.length, 13);
            assert.equal(result.rows[0].column_name, 'ID');
          });
    });

    it('should select primary keys', function() {
      assert.equal(pool.acquired, 0);
      this.slow(100);
      return metaData.select()
          .from('primary_keys')
          .where({schema_name: 'SQB_TEST', table_name: 'AIRPORTS'})
          .execute().then(result => {
            assert.equal(result.rows.length, 1);
            assert.equal(result.rows[0].column_names, 'ID');
          });
    });

    it('should select foreign keys', function() {
      assert.equal(pool.acquired, 0);
      return metaData.select()
          .from('foreign_keys')
          .where({schema_name: 'SQB_TEST', table_name: 'AIRPORTS'})
          .execute().then(result => {
            assert.equal(result.rows.length, 1);
            assert.equal(result.rows[0].column_name, 'REGION');
            assert.equal(result.rows[0].foreign_table_name, 'REGIONS');
            assert.equal(result.rows[0].foreign_column_name, 'ID');
          });
    });

    it('should get schema objects with metaData.getSchemas()', function() {
      assert.equal(pool.acquired, 0);
      return metaData.getSchemas('SQB_TEST')
          .then(schemas => {
            assert.notEqual(schemas.length, 0);
            schema = schemas[0];
          });
    });

    it('should not get table objects with metaData.getTables()', function(done) {
      assert.equal(pool.acquired, 0);
      metaData.getTables('AIRPORTS')
          .then(() => done('Failed'))
          .catch(() => done());
    });

    it('should get table objects with schema.getTables()', function() {
      assert.equal(pool.acquired, 0);
      this.slow(100);
      return schema.getTables()
          .then(tables => {
            assert.equal(tables.length, 2);
            table = tables[0];
            assert.equal(table.meta.table_name, 'AIRPORTS');
          });
    });

    it('should get table columns', function() {
      assert.equal(pool.acquired, 0);
      return table.getColumns().then(result => {
        assert(result);
        assert(result.ID);
        assert(result.ID.data_type);
      });
    });

    it('should get table primary key', function() {
      return table.getPrimaryKey().then(result => {
        assert(result);
        assert.equal(result.column_names, 'ID');
      });
    });

    it('should get table foreign keys', function() {
      return table.getForeignKeys().then(result => {
        assert(result);
        assert(result.length);
        assert.equal(result[0].column_name, 'REGION');
      });
    });

  });

  describe('Finalize', function() {

    it('should have no active connection after all tests', function() {
      assert.equal(pool.acquired, 0);
    });

    it('should shutdown pool', function() {
      return pool.close().then(() => {
        if (!pool.isClosed)
          throw new Error('Failed');
      });
    });

    it('should closed connection ignore close()', function() {
      return client1.close();
    });

    it('should not call execute on closed connection', function(done) {
      client1.execute('', {})
          .then(() => done('Failed'))
          .catch(() => done());
    });

  });

});

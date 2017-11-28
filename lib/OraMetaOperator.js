/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

const waterfall = require('putil-waterfall');

/**
 * Expose `OraMetaOperator`.
 */
module.exports = OraMetaOperator;

/**
 * @param {Object} sqbObj
 * @constructor
 */
function OraMetaOperator(sqbObj) {
}

const proto = OraMetaOperator.prototype = {};
proto.constructor = OraMetaOperator;

proto.query = function(sqbObj, request, callback) {
  const response = {
    schemas: {}
  };

  waterfall([
    function(next) {
      fetchSchemas(sqbObj, request, response, next);
    },
    function(next) {
      fetchTables(sqbObj, request, response, next);
    }
  ], function(err) {
    if (err)
      return callback(err);
    callback(undefined, response);
  });

};

function fetchSchemas(sqbObj, request, response, callback) {
  var schemaKeys;
  if (request.schemas !== '*') {
    // Replace schema names to upper case names
    schemaKeys = Object.getOwnPropertyNames(request.schemas)
        .map(function(key) {
          const upname = String(key).toUpperCase();
          if (upname !== key) {
            request.schemas[upname] = request.schemas[key];
            delete request.schemas[key];
          }
          return upname;
        });
  }

  const query = sqbObj
      .select('username')
      .from('dba_users u')
      .where(['default_tablespace', '!=', ['SYSTEM', 'SYSAUX']]);
  if (schemaKeys && schemaKeys.length)
    query.where(['username', schemaKeys]);

  query.execute({fetchRows: 1000}, function(err, result) {
    if (err)
      return callback(err);
    const rowset = result.rowset;
    while (rowset.next()) {
      response.schemas[rowset.get('username')] = {};
    }
    callback();
  });
}

function fetchTables(sqbObj, request, response, callback) {
  const schemaKeys = Object.getOwnPropertyNames(response.schemas);
  const where = [];
  if (schemaKeys && schemaKeys.length) {
    schemaKeys.forEach(function(t) {
      const g = [['t.owner', t]];
      const sch = typeof request.schemas === 'object' && request.schemas[t];
      if (sch && sch !== '*' && sch.tables && sch.tables !== '*') {
        g.push(['t.table_name', sch.tables.map(function(t2) {
          return String(t2).toUpperCase();
        })]);
      }
      if (where.length)
        where.push('or');
      where.push(g);
    });
  }

  waterfall([
    // Fetch tables
    function(next) {
      const query = sqbObj
          .select('owner', 'table_name', 'num_rows', 'temporary',
              sqbObj.select('comments').from('all_tab_comments atc')
                  .where(['atc.owner', sqbObj.raw('t.owner')],
                      ['atc.table_name', sqbObj.raw('t.table_name')])
                  .as('table_comments')
          )
          .from('all_tables t')
          .orderBy('t.owner', 't.table_name');
      if (where.length)
        query.where.apply(query, where);
      else query.where(['t.tablespace_name', '!=', ['SYSTEM', 'SYSAUX']]);

      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000,
        naming: 'uppercase'
      }, function(err, result) {
        if (err)
          return callback(err);
        const cursor = result.cursor;
        var schema;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.OWNER];
          schema.tables = schema.tables || {};
          var o = {
            num_rows: row.NUM_ROWS
          };
          if (row.TABLE_COMMENTS)
            o.comments = schema.tables[row.TABLE_COMMENTS];
          if (row.TEMPORARY === 'Y')
            o.temporary = true;
          o.columns = {};
          schema.tables[row.TABLE_NAME] = o;
          more();
        });
      });
    },

    // Fetch columns
    function(next) {
      const query = sqbObj
          .select('t.owner', 't.table_name', 'c.column_name', 'c.data_type',
              'c.data_length', 'c.data_precision', 'c.data_scale',
              'c.char_length', 'c.nullable',
              sqbObj.select('comments').from('all_col_comments acc')
                  .where(['acc.owner', sqbObj.raw('t.owner')],
                      ['acc.table_name', sqbObj.raw('t.table_name')],
                      ['acc.column_name', sqbObj.raw('c.column_name')]
                  )
                  .as('column_comments')
          )
          .from('all_tables t')
          .join(sqbObj.join('all_tab_columns c')
              .on(['c.owner', sqbObj.raw('t.OWNER')],
                  ['c.table_name', sqbObj.raw('t.table_name')]))
          .orderBy('t.owner', 't.table_name', 'c.column_id');
      if (where.length)
        query.where(where);
      else query.where(['t.tablespace_name', '!=', ['SYSTEM', 'SYSAUX']]);

      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000,
        naming: 'uppercase'
      }, function(err, result) {
        if (err)
          return callback(err);
        var schema;
        var table;
        var o;
        var v;
        const cursor = result.cursor;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.OWNER];
          table = schema && schema.tables[row.TABLE_NAME];
          if (table) {
            o = {
              data_type: row.DATA_TYPE,
              data_type_org: row.DATA_TYPE
            };
            switch (o.data_type) {
              case 'NCHAR':
                o.data_type = 'CHAR';
                break;
              case 'NCLOB':
                o.data_type = 'CLOB';
                break;
              case 'VARCHAR2':
              case 'NVARCHAR2':
              case 'LONG':
              case 'ROWID':
              case 'UROWID':
                o.data_type = 'VARCHAR';
                break;
              case 'LONG RAW':
              case 'BINARY_FLOAT':
              case 'BINARY_DOUBLE':
              case 'data_type':
                o.dataType = 'BUFFER';
                break;
            }
            if (o.data_type.substring(0, 9) === 'TIMESTAMP')
              o.data_type = 'TIMESTAMP';
            if ((v = row.CHAR_LENGTH))
              o.char_length = v;
            else if ((v = row.DATA_LENGTH))
              o.data_size = v;
            if ((v = row.DATA_PRECISION))
              o.precision = v;
            if ((v = row.DATA_SCALE))
              o.scale = v;
            if (row.NULLABLE !== 'Y')
              o.notnull = true;
            if ((v = row.COLUMN_COMMENTS))
              o.comments = v;
            table.columns[row.COLUMN_NAME] = o;
          }
          more();
        });
      });
    },

    // Fetch primary keys
    function(next) {
      const query = sqbObj
          .select('t.owner', 't.table_name', 't.constraint_name', 't.status',
              sqbObj.raw('to_char(listagg(acc.column_name, \',\') within group (order by null)) columns')
          )
          .from('all_constraints t')
          .join(
              sqbObj.join('all_cons_columns acc')
                  .on(['acc.owner', sqbObj.raw('t.owner')],
                      ['acc.constraint_name', sqbObj.raw('t.constraint_name')]
                  )
          ).where(['t.constraint_type', 'P'])
          .groupBy('t.owner', 't.table_name', 't.constraint_name', 't.status');
      if (where.length)
        query.where(where);

      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000,
        naming: 'uppercase'
      }, function(err, result) {
        if (err)
          return callback(err);
        var schema;
        var table;
        const cursor = result.cursor;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.OWNER];
          table = schema && schema.tables[row.TABLE_NAME];
          if (table) {
            table.primaryKey = {
              constraint_name: row.CONSTRAINT_NAME,
              columns: row.COLUMNS
            };
            if (row.STATUS !== 'ENABLED')
              table.primaryKey.disabled = true;
          }
          more();
        });
      });
    },

    // Foreign keys
    function(next) {
      const query = sqbObj
          .select('t.owner', 't.table_name', 't.constraint_name', 'acc.column_name',
              't.r_owner', 'acr.table_name r_table_name', 't.status',
              sqbObj.raw('to_char(listagg(acr.column_name, \',\') within group (order by null)) r_columns')
          )
          .from('all_constraints t')
          .join(
              sqbObj.join('all_cons_columns acc')
                  .on(['acc.owner', sqbObj.raw('t.owner')],
                      ['acc.constraint_name', sqbObj.raw('t.constraint_name')]
                  ),
              sqbObj.join('all_cons_columns acr')
                  .on(['acr.owner', sqbObj.raw('t.r_owner')],
                      ['acr.constraint_name', sqbObj.raw('t.r_constraint_name')]
                  )
          ).where(['t.constraint_type', 'R'])
          .groupBy('t.owner', 't.table_name', 't.constraint_name', 'acc.column_name',
              't.r_owner', 'acr.table_name', 't.status');
      if (where.length)
        query.where(where);

      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000,
        naming: 'uppercase'
      }, function(err, result) {
        if (err)
          return callback(err);
        var schema;
        var table;
        var o;
        const cursor = result.cursor;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.OWNER];
          table = schema && schema.tables[row.TABLE_NAME];
          if (table) {
            table.foreignKeys = table.foreignKeys || [];
            o = {
              constraint_name: row.CONSTRAINT_NAME,
              column_name: row.COLUMN_NAME,
              foreign_schema: row.R_OWNER,
              foreign_table_name: row.R_TABLE_NAME,
              foreign_columns: row.R_COLUMNS
            };
            if (row.STATUS !== 'ENABLED')
              o.disabled = true;
            table.foreignKeys.push();
          }
          more();
        });
      });
    }
  ], callback);
}

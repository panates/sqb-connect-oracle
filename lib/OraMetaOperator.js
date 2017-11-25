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
      .select('username', 'created')
      .from('dba_users u');
  if (schemaKeys && schemaKeys.length)
    query.where(['upper(username)', schemaKeys]);

  query.execute({fetchRows: 1000}, function(err, result) {
    if (err)
      return callback(err);
    const rowset = result.rowset;
    var s;
    while (rowset.next()) {
      s = rowset.get('username');
      response.schemas[s] = {
        created: rowset.get('created')
      };
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
          .select('owner', 'table_name', 'num_rows',
              sqbObj.select('comments').from('all_tab_comments atc')
                  .where(['atc.owner', sqbObj.raw('t.owner')],
                      ['atc.table_name', sqbObj.raw('t.table_name')])
                  .as('table_comments')
          )
          .from('all_tables t');
      if (where.length)
        query.where.apply(query, where);
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
          schema.tables[row.TABLE_NAME] = {
            num_rows: row.NUM_ROWS,
            comments: row.TABLE_COMMENTS,
            columns: {}
          };
          more();
        });
      });
    },

    // Fetch columns
    function(next) {
      const query = sqbObj
          .select('owner', 'table_name', 'column_name', 'data_type',
              'data_length', 'data_precision', 'data_scale', 'nullable',
              sqbObj.select('comments').from('all_col_comments acc')
                  .where(['acc.owner', sqbObj.raw('t.owner')],
                      ['acc.table_name', sqbObj.raw('t.table_name')],
                      ['acc.column_name', sqbObj.raw('t.column_name')]
                  )
                  .as('column_comments')
          )
          .from('all_tab_columns t');
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
            var dataType = row.DATA_TYPE;
            switch (dataType) {
              case 'NCHAR':
                dataType = 'CHAR';
                break;
              case 'NCLOB':
                dataType = 'CLOB';
                break;
              case 'VARCHAR2':
              case 'NVARCHAR2':
              case 'LONG':
              case 'ROWID':
              case 'UROWID':
                dataType = 'VARCHAR';
                break;
              case 'LONG RAW':
              case 'BINARY_FLOAT':
              case 'BINARY_DOUBLE':
              case 'RAW':
                dataType = 'BUFFER';
                break;
            }
            if (dataType.substring(0, 9) === 'TIMESTAMP')
              dataType = 'TIMESTAMP';
            o = {
              data_type: dataType,
              data_type_org: row.DATA_TYPE
            };
            if ((v = row.DATA_LENGTH))
              o.data_size = v;
            if ((v = row.DATA_PRECISION))
              o.data_precision = v;
            if ((v = row.DATA_SCALE))
              o.data_scale = v;
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
            table.foreignKeys.push({
              constraint_name: row.CONSTRAINT_NAME,
              column_name: row.COLUMN_NAME,
              remote_schema: row.R_OWNER,
              remote_table_name: row.R_TABLE_NAME,
              remote_columns: row.R_COLUMNS,
              disabled: row.STATUS !== 'ENABLED'
            });
          }
          more();
        });
      });
    }
  ], callback);
}

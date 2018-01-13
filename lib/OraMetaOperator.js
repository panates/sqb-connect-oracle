/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/**
 * Expose `OraMetaOperator`.
 */
module.exports = OraMetaOperator;

/**
 * @param {Object} sqbObj
 * @constructor
 */
function OraMetaOperator(sqbObj) {
  this.supportsSchemas = true;
}

const proto = OraMetaOperator.prototype;

proto.querySchemas = function(db) {
  return db
      .select('username schema_name')
      .from('dba_users u');
};

proto.queryTables = function(db) {
  const Op = db.Op;
  const query = db
      .select('t.owner schema_name', 'table_name', 'num_rows', 'temporary',
          db.select('comments').from('all_tab_comments atc')
              .where(Op.eq('atc.owner', db.raw('t.owner')),
                  Op.eq('atc.table_name', db.raw('t.table_name')))
              .as('table_comments')
      )
      .from('all_tables t')
      .orderBy('t.owner', 't.table_name');
  query.on('fetch', function(row) {
    row.set('temporary', row.get('temporary') === 'Y');
  });
  return query;
};

proto.queryColumns = function(db) {
  const Op = db.Op;
  const query = db
      .select('t.owner schema_name', 't.table_name', 'c.column_name',
          'c.column_id column_number', 'c.data_type', 'c.data_type data_type_mean',
          'c.data_length', 'c.data_precision', 'c.data_scale',
          'c.char_length', 'c.data_default default_value',
          db.case()
              .when(Op.eq('c.nullable', 'Y'))
              .then(0)
              .else(1)
              .as('not_null'),
          db.select('comments').from('all_col_comments acc')
              .where(Op.eq('acc.owner', db.raw('t.owner')),
                  Op.eq('acc.table_name', db.raw('t.table_name')),
                  Op.eq('acc.column_name', db.raw('c.column_name'))
              )
              .as('column_comments')
      )
      .from('all_tables t')
      .join(db.join('all_tab_columns c')
          .on(Op.eq('c.owner', db.raw('t.OWNER')),
              Op.eq('c.table_name', db.raw('t.table_name'))))
      .orderBy('t.owner', 't.table_name', 'c.column_id');
  query.on('fetch', function(row) {
    switch (row.get('data_type')) {
      case 'NCHAR':
        row.set('data_type_mean', 'CHAR');
        break;
      case 'NCLOB':
        row.set('data_type_mean', 'CLOB');
        break;
      case 'VARCHAR2':
      case 'NVARCHAR2':
      case 'LONG':
      case 'ROWID':
      case 'UROWID':
        row.set('data_type_mean', 'VARCHAR');
        break;
      case 'LONG RAW':
      case 'BINARY_FLOAT':
      case 'BINARY_DOUBLE':
      case 'data_type':
        row.set('data_type_mean', 'BUFFER');
        break;
    }
    if (row.get('data_type').substring(0, 9) === 'TIMESTAMP')
      row.set('data_type_mean', 'TIMESTAMP');
  });
  return query;
};

proto.queryPrimaryKeys = function(db) {
  const Op = db.Op;
  const query = db
      .select('t.owner schema_name', 't.table_name', 't.constraint_name', 't.status enabled',
          db.raw('to_char(listagg(acc.column_name, \',\') within group (order by null)) column_names')
      )
      .from('all_constraints t')
      .join(
          db.join('all_cons_columns acc')
              .on(Op.eq('acc.owner', db.raw('t.owner')),
                  Op.eq('acc.constraint_name', db.raw('t.constraint_name'))
              )
      ).where(Op.eq('t.constraint_type', 'P'))
      .groupBy('t.owner', 't.table_name', 't.constraint_name', 't.status');
  query.on('fetch', function(row) {
    row.enabled = row.enabled === 'ENABLED';
  });
  return query;
};

proto.queryForeignKeys = function(db) {
  const Op = db.Op;
  const query = db
      .select('t.owner schema_name', 't.table_name', 't.constraint_name', 'acc.column_name',
          't.r_owner foreign_schema', 'acr.table_name foreign_table_name',
          db.raw('to_char(listagg(acr.column_name, \',\') within group (order by null)) foreign_column_name'),
          't.status enabled'
      )
      .from('all_constraints t')
      .join(
          db.join('all_cons_columns acc')
              .on(Op.eq('acc.owner', db.raw('t.owner')),
                  Op.eq('acc.constraint_name', db.raw('t.constraint_name'))
              ),
          db.join('all_cons_columns acr')
              .on(Op.eq('acr.owner', db.raw('t.r_owner')),
                  Op.eq('acr.constraint_name', db.raw('t.r_constraint_name'))
              )
      ).where(Op.eq('t.constraint_type', 'R'))
      .groupBy('t.owner', 't.table_name', 't.constraint_name', 'acc.column_name',
          't.r_owner', 'acr.table_name', 't.status');
  query.on('fetch', function(row) {
    row.enabled = row.enabled === 'ENABLED';
  });
  return query;
};

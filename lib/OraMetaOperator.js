/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/**
 * @param {Object} sqbObj
 * @constructor
 */
class OraMetaOperator {

  constructor() {
    // noinspection JSUnusedGlobalSymbols
    this.supportsSchemas = true;
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  querySchemas(db) {
    return db
        .select('username schema_name')
        .from('dba_users u');
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryTables(db) {
    return db
        .select('t.owner schema_name', 'table_name', 'num_rows',
            db.case()
                .when({'temporary': 'Y'})
                .then(1)
                .else(0)
                .as('not_null'),
            db.select('comments').from('all_tab_comments atc')
                .where({
                  'atc.owner': db.raw('t.owner'),
                  'atc.table_name': db.raw('t.table_name')
                })
                .as('table_comments')
        )
        .from('all_tables t')
        .orderBy('t.owner', 't.table_name');
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryColumns(db) {
    const query = db
        .select('t.owner schema_name', 't.table_name', 'c.column_name',
            'c.column_id column_number', 'c.data_type', 'c.data_type data_type_mean',
            'c.data_length', 'c.data_precision', 'c.data_scale',
            'c.char_length', 'c.data_default default_value',
            db.case().when({'c.nullable': 'Y'}).then(0).else(1).as('not_null'),
            db.select('comments').from('all_col_comments acc')
                .where({
                  'acc.owner': db.raw('t.owner'),
                  'acc.table_name': db.raw('t.table_name'),
                  'acc.column_name': db.raw('c.column_name')
                })
                .as('column_comments')
        )
        .from('all_tables t')
        .join(db.join('all_tab_columns c')
            .on({
              'c.owner': db.raw('t.OWNER'),
              'c.table_name': db.raw('t.table_name')
            }))
        .orderBy('t.owner', 't.table_name', 'c.column_id');
    query.on('fetch', (row) => {
      const dataType = row.data_type || row.DATA_TYPE;
      let dataTypeMean = row.data_type_mean || row.DATA_TYPE_MEAN;
      switch (dataType) {
        case 'NCHAR':
          dataTypeMean = 'CHAR';
          break;
        case 'NCLOB':
          dataTypeMean = 'CLOB';
          break;
        case 'VARCHAR2':
        case 'NVARCHAR2':
        case 'LONG':
        case 'ROWID':
        case 'UROWID':
          dataTypeMean = 'VARCHAR';
          break;
        case 'LONG RAW':
        case 'BINARY_FLOAT':
        case 'BINARY_DOUBLE':
        case 'data_type':
          dataTypeMean = 'BUFFER';
          break;
      }
      if (dataType.substring(0, 9) === 'TIMESTAMP')
        dataTypeMean = 'TIMESTAMP';
      if (row.data_type_mean)
        row.data_type_mean = dataTypeMean;
      else row.DATA_TYPE_MEAN = dataTypeMean;
    });
    return query;
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryPrimaryKeys(db) {
    return db
        .select('t.owner schema_name', 't.table_name', 't.constraint_name',
            db.case()
                .when({'t.status': 'ENABLED'})
                .then(1)
                .else(0)
                .as('enabled'),
            db.raw('to_char(listagg(acc.column_name, \',\') within group (order by null)) column_names')
        )
        .from('all_constraints t')
        .join(
            db.join('all_cons_columns acc')
                .on({
                  'acc.owner': db.raw('t.owner'),
                  'acc.constraint_name': db.raw('t.constraint_name')
                })
        ).where({'t.constraint_type': 'P'})
        .groupBy('t.owner', 't.table_name', 't.constraint_name', 't.status');
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryForeignKeys(db) {
    return db
        .select('t.owner schema_name', 't.table_name', 't.constraint_name', 'acc.column_name',
            't.r_owner foreign_schema', 'acr.table_name foreign_table_name',
            db.raw('to_char(listagg(acr.column_name, \',\') within group (order by null)) foreign_column_name'),
            db.case()
                .when({'t.status': 'ENABLED'})
                .then(1)
                .else(0)
                .as('enabled')
        )
        .from('all_constraints t')
        .join(
            db.join('all_cons_columns acc')
                .on({
                  'acc.owner': db.raw('t.owner'),
                  'acc.constraint_name': db.raw('t.constraint_name')
                }),
            db.join('all_cons_columns acr')
                .on({
                  'acr.owner': db.raw('t.r_owner'),
                  'acr.constraint_name': db.raw('t.r_constraint_name')
                })
        ).where({'t.constraint_type': 'R'})
        .groupBy('t.owner', 't.table_name', 't.constraint_name', 'acc.column_name',
            't.r_owner', 'acr.table_name', 't.status');
  }

}

/**
 * Expose `OraMetaOperator`.
 */
module.exports = OraMetaOperator;

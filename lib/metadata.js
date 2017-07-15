/* SQB
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb/
 */

/* External module dependencies. */
//noinspection NpmUsedModulesInstalled
const sqb = require('sqb');

/**
 * @constructor
 * @public
 */
class OracledbMetaData extends sqb.MetaData {

  //noinspection JSUnusedGlobalSymbols
  /**
   *
   * @param {Object} options
   * @return {Query}
   * @protected
   */
  _getQuery(options) {
    if (options.type === 'list_schemas')
      return this._getListSchemasQuery(options);

    if (options.type === 'list_tables')
      return this._getListTablesQuery(options);

    if (options.type === 'list_columns')
      return this._getListColumnsQuery(options);

    if (options.type === 'list_primary_keys')
      return this._getListPrimaryKeysQuery(options);

    if (options.type === 'list_foreign_keys')
      return this._getListForeignKeysQuery(options);

    else
      throw new Error('Unknown or unimplemented metadata query type (' +
          options.type + ')');
  }

  /**
   *
   * @return {Query}
   * @protected
   */
  _getListSchemasQuery() {
    return this.dbobj
        .select('username schema_name', 'created create_date')
        .from('all_users schemas');
  }

  /**
   *
   * @return {Query}
   * @protected
   */
  _getListTablesQuery() {
    return this.dbobj
        .select('owner schema_name', 'table_name',
            'num_rows', 'read_only',
            sqb.select('comments')
                .from('all_tab_comments atc')
                .where(['atc.owner', sqb.raw('tbl.owner')],
                    ['atc.table_name', sqb.raw('tbl.table_name')])
                .as('table_comments'))
        .from('all_tables tbl')
        .orderBy('owner', 'table_name');
  }

  /**
   *
   * @return {Query}
   * @protected
   */
  _getListColumnsQuery() {
    return this.dbobj
        .select('owner schema_name', 'table_name',
            'column_name', 'data_type', 'data_length', 'data_precision',
            'data_scale', 'nullable',
            sqb.select('comments')
                .from('all_col_comments acc')
                .where(['acc.owner', sqb.raw('atc.owner')],
                    ['acc.table_name', sqb.raw('atc.table_name')],
                    ['acc.column_name', sqb.raw('atc.column_name')])
                .as('column_comments')
        )
        .from('all_tab_columns atc')
        .orderBy('owner', 'table_name', 'column_id')
        .onFetchRow((row, idx, meta) => {
          let dataType = meta.getValue('data_type', row);
          /* Map oracle data types to generic data types */
          if (dataType) {
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
              case 'DATE':
                dataType = 'TIMESTAMP';
                break;
              default: {
                let m = dataType.match(/TIMESTAMP\(?(\d+)?\)?(.+)?/);
                if (m) {
                  dataType = 'TIMESTAMP';
                }
              }
            }
            meta.setValue('data_type', dataType, row);
          }
          if (row.NULLABLE !== null)
            meta.setValue('nullable', (row.NULLABLE === 'Y'), row);
        });
  }

  /**
   *
   * @return {Query}
   * @protected
   */
  _getListPrimaryKeysQuery() {
    return this.dbobj
        .select('ac.owner schema_name', 'ac.table_name',
            'ac.constraint_name', 'ac.status',
            sqb.raw('to_char(listagg(acc.column_name, \',\') within group (order by null)) columns'))
        .from('all_constraints ac')
        .join(
            sqb.innerJoin('all_cons_columns acc').on(
                ['acc.owner', sqb.raw('ac.owner')],
                ['acc.table_name', sqb.raw('ac.table_name')],
                ['acc.constraint_name', sqb.raw('ac.constraint_name')]
            ))
        .where(['ac.constraint_type', 'P'])
        .groupBy('ac.owner', 'ac.table_name', 'ac.constraint_name', 'ac.status')
        .orderBy('ac.owner', 'ac.table_name', 'ac.constraint_name');
  }

  /**
   *
   * @return {Query}
   * @protected
   */
  _getListForeignKeysQuery() {
    return this.dbobj
        .select('ac.owner schema_name', 'ac.table_name',
            'ac.constraint_name', 'ac.status',
            sqb.raw('to_char(listagg(acc.column_name, \',\') within group (order by null)) column_name'),
            sqb.raw('to_char(listagg(ac.r_owner, \',\') within group (order by null)) r_schema'),
            sqb.raw('to_char(listagg(acr.table_name, \',\') within group (order by null)) r_table_name'),
            sqb.raw('to_char(listagg(acr.column_name, \',\') within group (order by null)) r_columns')
        )
        .from('all_constraints ac')
        .join(
            sqb.innerJoin('all_cons_columns acc').on(
                ['acc.owner', sqb.raw('ac.owner')],
                ['acc.table_name', sqb.raw('ac.table_name')],
                ['acc.constraint_name', sqb.raw('ac.constraint_name')]
            ),
            sqb.innerJoin('all_cons_columns acr').on(
                ['acr.owner', sqb.raw('ac.r_owner')],
                ['acr.constraint_name', sqb.raw('ac.r_constraint_name')]
            ))
        .where(['ac.constraint_type', 'R'])
        .groupBy('ac.owner', 'ac.table_name', 'ac.constraint_name', 'ac.status')
        .orderBy('ac.owner', 'ac.table_name', 'ac.constraint_name');
  }
}

/**
 * @external Query
 */

/**
 * @external sqb.MetaData
 */

module.exports = OracledbMetaData;

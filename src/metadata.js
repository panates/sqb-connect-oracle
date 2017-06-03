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
   * @return {Statement}
   * @protected
   */
  _getStatement(options) {
    if (options.type === 'list_schemas')
      return this._getListSchemasStatement(options);

    if (options.type === 'list_tables')
      return this._getListTablesStatement(options);

    if (options.type === 'list_columns')
      return this._getListColumnsStatement(options);

    if (options.type === 'list_primary_keys')
      return this._getListPrimaryKeysStatement(options);

    if (options.type === 'list_foreign_keys')
      return this._getListForeignKeysStatement(options);

    else
      throw new Error('Unknown or unimplemented metadata statement type (' +
          options.type + ')');
  }

  /**
   *
   * @param {Object} options
   * @param {string} options.fields
   * @return {Statement}
   * @protected
   */
  _getListSchemasStatement(options) {
    const srcflds = options.fields;
    const fields = [];
    if (!srcflds.length || srcflds.includes('schema_name'))
      fields.push('username schema_name');
    if (!srcflds.length || srcflds.includes('create_date'))
      fields.push('created create_date');
    return this.dbobj.select(...fields)
        .from('all_users schemas');
  }

  /**
   *
   * @param {Object} options
   * @param {string} options.fields
   * @return {Statement}
   * @protected
   */
  _getListTablesStatement(options) {
    const srcflds = options.fields;
    const fields = [];
    if (!srcflds.length || srcflds.includes('schema'))
      fields.push('owner schema_name');
    if (!srcflds.length || srcflds.includes('table_name'))
      fields.push('table_name');
    if (!srcflds.length || srcflds.includes('num_rows'))
      fields.push('num_rows');
    if (!srcflds.length || srcflds.includes('logging'))
      fields.push(sqb.case()
          .when(['logging', 'YES'])
          .then(1)
          .else(0)
          .as('logging'));
    if (!srcflds.length || srcflds.includes('partitioned'))
      fields.push(sqb.case()
          .when(['partitioned', 'YES'])
          .then(1)
          .else(0)
          .as('partitioned'));
    if (!srcflds.length || srcflds.includes('read_only'))
      fields.push(sqb.case()
          .when(['read_only', 'YES'])
          .then(1)
          .else(0)
          .as('read_only'));
    if (!srcflds.length || srcflds.includes('table_comments'))
      fields.push(
          sqb.select('comments')
              .from('all_tab_comments atc')
              .where(['atc.owner', sqb.raw('tbl.owner')],
                  ['atc.table_name', sqb.raw('tbl.table_name')])
              .as('table_comments')
      );
    return this.dbobj.select(...fields)
        .from('all_tables tbl');
  }

  /**
   *
   * @param {Object} options
   * @param {string} options.fields
   * @return {Statement}
   * @protected
   */
  _getListColumnsStatement(options) {
    const srcflds = options.fields;
    const fields = [];
    if (!srcflds.length || srcflds.includes('schema_name'))
      fields.push('owner schema_name');
    if (!srcflds.length || srcflds.includes('table_name'))
      fields.push('table_name');
    if (!srcflds.length || srcflds.includes('column_name'))
      fields.push('column_name');
    if (!srcflds.length || srcflds.includes('data_type'))
      fields.push('data_type');
    if (!srcflds.length || srcflds.includes('data_length'))
      fields.push('data_length');
    if (!srcflds.length || srcflds.includes('data_precision'))
      fields.push('data_precision');
    if (!srcflds.length || srcflds.includes('data_scale'))
      fields.push('data_scale');
    if (!srcflds.length || srcflds.includes('nullable'))
      fields.push('nullable');
    if (!srcflds.length || srcflds.includes('column_comments'))
      fields.push(
          sqb.select('comments')
              .from('all_col_comments acc')
              .where(['acc.owner', sqb.raw('atc.owner')],
                  ['acc.table_name', sqb.raw('atc.table_name')],
                  ['acc.column_name', sqb.raw('atc.column_name')])
              .as('column_comments')
      );
    return this.dbobj.select(...fields)
        .from('all_tab_columns atc');
  }

  /**
   *
   * @param {Object} options
   * @param {string} options.fields
   * @return {Statement}
   * @protected
   */
  _getListPrimaryKeysStatement(options) {
    const srcflds = options.fields;
    const fields = [];
    if (!srcflds.length || srcflds.includes('schema_name'))
      fields.push('ac.owner schema_name');
    if (!srcflds.length || srcflds.includes('table_name'))
      fields.push('ac.table_name');
    if (!srcflds.length || srcflds.includes('constraint_name'))
      fields.push('ac.constraint_name');
    if (!srcflds.length || srcflds.includes('status'))
      fields.push(sqb.case()
          .when(['ac.status', 'ENABLED'])
          .then(1)
          .else(0)
          .as('status'));
    if (!srcflds.length || srcflds.includes('columns'))
      fields.push(sqb.raw('to_char(list(acc.column_name))', 'columns'));
    return this.dbobj.select(...fields)
        .from('all_constraints ac')
        .join(
            sqb.innerJoin('all_cons_columns acc').on(
                ['acc.owner', sqb.raw('ac.owner')],
                ['acc.table_name', sqb.raw('ac.table_name')],
                ['acc.constraint_name', sqb.raw('ac.constraint_name')]
            ))
        .where(['ac.constraint_type', 'P'])
        .groupBy('ac.owner', 'ac.table_name', 'ac.constraint_name', 'ac.status');
  }

  /**
   *
   * @param {Object} options
   * @param {string} options.fields
   * @return {Statement}
   * @protected
   */
  _getListForeignKeysStatement(options) {
    const srcflds = options.fields;
    const fields = [];
    if (!srcflds.length || srcflds.includes('schema_name'))
      fields.push('ac.owner schema_name');
    if (!srcflds.length || srcflds.includes('table_name'))
      fields.push('ac.table_name');
    if (!srcflds.length || srcflds.includes('constraint_name'))
      fields.push('ac.constraint_name');
    if (!srcflds.length || srcflds.includes('status'))
      fields.push(sqb.case()
          .when(['ac.status', 'ENABLED'])
          .then(1)
          .else(0)
          .as('status'));
    if (!srcflds.length || srcflds.includes('column_name'))
      fields.push(sqb.raw('to_char(list(acc.column_name))', 'column_name'));
    if (!srcflds.length || srcflds.includes('r_schema'))
      fields.push(sqb.raw('to_char(list(ac.r_owner))', 'r_schema'));
    if (!srcflds.length || srcflds.includes('r_table_name'))
      fields.push(sqb.raw('to_char(list(acr.table_name))', 'r_table_name'));
    if (!srcflds.length || srcflds.includes('r_columns'))
      fields.push(sqb.raw('to_char(list(acr.column_name))', 'r_columns'));

    return this.dbobj.select(...fields)
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
        .groupBy('ac.owner', 'ac.table_name', 'ac.constraint_name', 'ac.status');
  }
}

/**
 * @external Statement
 */

/**
 * @external sqb.MetaData
 */

module.exports = OracledbMetaData;

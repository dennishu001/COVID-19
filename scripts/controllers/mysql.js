'use strict';

const fs = require('fs')
const csv = require('csv')
const csvCtrl = require('./csv')
const _ = require('underscore')
const mysql = require('mysql')
const path = require('path')
const knophyg = require('konphyg')(path.join(__dirname, '../config'))
const config = knophyg('main')
// config.mysql.debug = true
let pool = mysql.createPool(config.mysql)

const utils = require("./utilities.helper")

// console.log('env:', process.env.NODE_ENV)
// console.log(config.mysql)

/**
 * Wraps connection in promise.
 * Generally, we will use pool.query to let mysql handle connection
 * automatically. Sometime we want to execute a series queries on
 * the same connection. In that case, we want to get the connection
 * first.
 */
function getConnection(currentPool) {
  currentPool = currentPool || pool
  return new Promise((resolve, reject) => {
    currentPool.getConnection((err, conn) => {
      if (err)
        reject(err)
      else
        resolve(conn)
    })
  })
}

/**
 * Recreates pool. Usefully when environment is changed, and we want to connect
 * to a new conneciton.
 */
function recreatePool() {
  // console.log('recreating pool...')
  return new Promise((resolve, reject) => {
    // closing all connection in the current pool
    pool.end(err => {
      if (err)
        reject(err)
      else {
        // reload config
        const knophyg = require('konphyg')(path.join(__dirname, '../config'))
        const config = knophyg('main')
        // console.log('creating new pool ...')
        console.log(config.mysql)
        pool = mysql.createPool(config.mysql)
        resolve()
      }
    })
  })
}

/**
 * Wraps query in promise.
 * @param {String} sql: sql query
 * @param {Array} params: array of param strings.
 * @param {Object} conn: optional connection.
 */
async function query(sql, params, conn, debug) {
  params = params || []
  conn = conn || pool
  return new Promise((resolve, reject) => {    
    conn.query(sql, params, function(err, data) {
      if (debug) utils.log('sql:', this.sql)
      if (err) {
        utils.log('sql:' + this.sql)
        reject(err)
      }
      else
        resolve(data)
    })
  })
}

/**
 * Runs query silently without throwing error
 */
async function querySilent (sql, param, msg, conn) {
  try {
    return await query(sql, param, conn)
  }
  catch (e) {
    utils.log(msg || 'mysql error', e)
  }
}

async function queryJSON(sql, param, conn, debug) {  
  return toJSON(await query(sql, param, conn, debug))  
}

function toJSON(obj) {
  if (!obj) return obj;
  return JSON.parse(JSON.stringify(obj));
}

async function queryMessage(sql, param, conn, debug) {  
  const res = await query(sql, param, conn, debug)
  return res && res.message
}

/**
 * Returns single row from query
 */
async function queryRow(sql, param, conn, debug) {
  const res = await query(sql, param, conn, debug)
  return res && res.length && res[0] || null
}

/**
 * Returns single field value
 */
async function queryValue(sql, param, conn, debug) {
  const res = await queryRow(sql, param, conn, debug)
  return res && res[Object.keys(res)[0]]
}

// Helpers
// ----------------------------------------------

/**
 * Prepares data for insert sql construction.
 * @param {[Object]} fields: Array of fields definition
 * - @param {String} field: Field name in database.
 * - @param {String} key: Property name of the value in data source. Optional. If missing, same as `field`
 * - @param {String} param: Property name of the value in params provided. Optional.
 * @param {[Object]} data: Array of data
 * - [key]: value
 * @param {Object} params: addition data values
 * - [key]: value
 * @return {Object}
 * - {[String]} fields Array of escaped field name.
 * - {[Array]} values Array of escaped values.
 *
 * @example
 * var fields = [
 *   {field: 'name', (key: 'AOI_NAME')},
 *   {field: 'date', param: 'month'}
 * ];
 * var data = [
 *   [{'AOI_NAME': 'company name'}]
 * ];
 * var params = {month: '201401'};
 * var rtn = {
 *   fields: ['name', 'date'],
 *   values: ['("company name", "201401")']
 * };
 */

function parseValues(fields, data, params) {
  // wrap data in array
  if (!_.isArray(data)) data = [data]

  return data.map(row => {
    const rtn = {}

    _.each(fields, function (item) {
      // normalize single field name string
      if (_.isString(item)) item = {field: item, key: item}
      
      // get value from params
      if (item.param) {
        rtn[item.field] = params[item.param]
      }
      // get value from key
      else if (item.key) {
        rtn[item.field] = row[item.key]
      }
      // default to field name
      else
        rtn[item.field] = row[item.field]
    })

    return rtn
  })
}

function prepareInsert(fields, data, params) {
  // wrap data in array
  if (!_.isArray(data)) data = [data]
  // console.log('prepare data:', data)
  var rtn = {};
  //console.log(fields);
  // get values from data row
  var _getValues = function(row) {
    return fields.map(function (item) {
      // single field name string
      if (_.isString(item)) item = {field: item, key: item};
      let val = null;
      
      // get value from params
      if (item.param) {
        val = params[item.param];
      }
      // get value from key
      else if (item.key) {
        val = row[item.key];
      }
      // default to field name
      else
        val = row[item.field];
      return mysql.escape(val);
    });
  };

  if (!data || !data.length) return rtn;

  rtn.fields = fields.map(function (item) {
    if (_.isString(item)) item = {field: item, key: item};
    return mysql.escapeId(item.field);
  });

  rtn.values = data.map(function (row) {
    var values = _getValues(row);
    return '(' + values.join(',') + ')';
  });
  //console.log(rtn);
  return rtn;

}

/**
 * Inserts data to table
 * @param {Object[]} fields 
 * @param {Object[]} data 
 * @param {Object} params
 * @param {string} params.insertType - insertion type, e.g. 'IGNORE'
 * @param {string} table - table name
 * @param {boolean} [debug] - toggle debug mode.
 */
async function queryInsert(fields, data, params, table, debug) {
  params = params || {}
  const prepared = prepareInsert(fields, data, params)
  return await queryMessage('INSERT ' + (params.insertType || '') + ' INTO ?? (' + prepared.fields.join(',') + ') VALUES ' + prepared.values.join(','), table, params.conn, debug)
}

/**
 * Inserts object data to table.
 * @param {Object} data - data object to be inserted 
 * @param {Object} [params]
 * @param {string} params.insertType
 * @param {String} table - table name
 */
async function queryInsertObject(data, params, table) {
  return await queryInsert(_.keys(data), data, params, table)
}

/**
 * Inserts array data to table.
 * @param {Object[]} data - data object to be inserted 
 * @param {Object} [params]
 * @param {string} table - table name
 */
async function queryInsertArray(data, params, table) {
  return await Promise.all(_.map(data, async (item) => await queryInsertObject(item, params, table)))
}

/**
 * Prepares data for update sql construction.
 * @params {[Object]} fields Array of fields definition
 * - {String} field: Field name
 * - {String} key Property name of the value in data.
 * - {String} param Property name of the value in params.
 *
 * @example
 * var fields = [
 *   {field: name, key: 'AOI_NAME'},
 *   {field: date, param: 'month'}
 * ];
 * var data = [
 *   {'AOI_NAME': 'company name'}
 * ];
 * var params = {month: '201401'};
 * var rtn = ['`field` = "value"'];
 */
function prepareUpdate(fields, data, params) {
  var rtn = [];

  if (!data || !data.length) throw new Error('no valeus');

  rtn = fields.map(function(item) {
    let value = null;
    if (item.param) {
      value = params[item.param];
    }
    else
      value = data[0][item.key || item.field];
    return mysql.escapeId(item.field) + '=' + mysql.escape(value);
  });

  return rtn;

}

/**
 * Converts object to update set string
 * @param {Object} setter Object of key value pairs
 * @return {String} sql set string
 */
function parseSetter(setter) {
  if (_.isEmpty(setter)) return '';

  return _.map(setter, (val, key) => {
    return mysql.escapeId(key) + '=' + mysql.escape(val);
  }).join(',');

}

/**
 * Streams query result to csv file.
 * @param {string} file - file location
 * @param {string} sql
 * @param {string[]} [params] - query params
 * @param {Object} [options]
 * @param {Object} [options.conn] - shared connection
 */
async function toCsv(file, sql, params, options) {
  params = params || []
  options = options || {}
  const conn = options.conn || pool
  
  // remove existing if any
  // if (fs.existsSync(file)) fs.unlinkSync(file)

  return new Promise((resolve, reject) => {
    let returned = false
    const fstream = fs.createWriteStream(file, 'utf8')

    conn.query(sql, params)
    .on('fields', (fields) => {
      // console.log('fields:', fields)
      fstream.write(
        fields.map(field => `"${field.name}"`).join(',') + "\n"
      )
    })
    .stream({highWaterMark: 5})
    .pipe(csv.stringify({
      cast: {
        string: function(value) { return { value: value, quote: true} },
      }
    }))
    .pipe(fstream)
    .on('end', () => {
      if (returned) return
      returned = true
      resolve()
    })
    .on('finish', () => {
      if (returned) return
      returned = true
      resolve()
    })
    .on('error', (err) => {
      if (returned) return
      returned = true
      reject(err)
    })

  })
}

/**
 * Restore data from csv file. This will read the whole file to memory.
 * @param {string} file 
 * @param {string} table
 * @param {Object} [options]
 * @param {string} [options.insertType]
 * @param {boolean} [options.debug]
 */
async function fromCsv(file, table, options) {
  options = options || {}
  // read file
  // console.log('file:', file)
  const rows = await csvCtrl.parse(fs.readFileSync(file))
  // console.log('rows:', rows)
  const dataset = _breakData(rows, options.limit || 5000)

  // insert data
  for (let i = 0; i < dataset.length; i++) {
    await queryInsertData(dataset[i], table, options)
  }
}

/**
 * Imports data from csv file
 * @param {Object} options
 * @param {string} options.file - pull path the file
 * @param {string} options.table - table name
 * @param {string} [options.fieldBy=","]
 * @param {string} [options.encloseBy='"']
 * @param {string} [options.lineBy='\n']
 * @param {number} [options.ignore=1]
 * @param {boolean} [options.debug]
 */
async function loadFile(options, conn) {
  const params =  [
    options.file,
    options.table,
    options.fieldBy || ',',
    options.encloseBy || '"',
    options.lineBy || '\n',
    options.ignore || 1
  ]
  await query(`LOAD DATA LOCAL INFILE ?
    INTO TABLE ${options.table}
    CHARACTER SET 'utf8'
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;`, [
      options.file,
    ], conn, options.debug)
}

function _breakData(data, limit) {
  if (!data || !data.length)  return []

  const rtn = []
  const iteration = Math.ceil((data.length - 1) / limit)

  for (let i = 0; i < iteration; i++) {
    // remove rows from dataset after the first first 
    const set = data.splice(1, limit)
    // Add the first row in rows to the new set
    set.unshift(data[0])
    // add to dataset
    rtn.push(set)
  }

  return rtn
}

/**
 * Inserts dataset.
 * @param {string|number[]} data - data rows with first row as fields.
 * @param {string} table
 * @param {Object} [options]
 * @param {string} [options.insertType] - ignore etc.
 * @param {boolean} [options.debug] - enable debugging.
 */
async function queryInsertData(data, table, options) {
  options = options || {}
  return await query('INSERT ' + (options.insertType || '') + ' INTO ?? (' 
    + data[0].map(field => mysql.escapeId(field)).join(',') 
  + ') VALUES '
  + data.filter((row, i) => !!i)
    .map(row => '(' + row.map(item => mysql.escape(item)).join(',') + ')')
    .join(',')
  , [table], null, null, options.debug)
} 

/**
 * Streams query result to csv file.
 */
// async function queryFile(file, sql, params, conn) {
//   params = params || []
//   conn = conn || pool
//   // We need action connection
//   const connection = conn.pause ? conn : await getConnection(conn)
  
//   // remove existing if any
//   if (fs.existsSync(file)) fs.unlinkSync(file)

//   return new Promise((resolve, reject) => {
//     let returned = false
//     const fstream = fs.createWriteStream(file, 'utf8')

//     conn.query(sql, params)
//     .on('fields', (fields) => {
//       // console.log('fields:', fields)
//       fstream.write(
//         fields.map(field => `"${field.name}"`).join(',') + "\n"
//       )
//     })
//     .on('result', (row) => {
//       // wait for write
//       connection.pause()
//       fstream.write(JSON.stringify(row) + "\n")
//       connection.resume()
//     })
//     // .stream({highWaterMark: 5})
//     // .pipe(csv.stringify({
//     //   cast: {
//     //     string: function(value) { return { value: value, quote: true} },
//     //   }
//     // }))
//     // .pipe(fstream)
//     .on('end', () => {
//       if (returned) return
//       returned = true
//       resolve()
//     })
//     .on('finish', () => {
//       if (returned) return
//       returned = true
//       resolve()
//     })
//     .on('error', (err) => {
//       if (returned) return
//       returned = true
//       reject(err)
//     })

//   })
// }

/**
 * Copy whole table.
 * @param {string} source - source table name
 * @param {string} target - target table name
 */
async function copyTable(source, target) {
  await query('DROP TABLE IF EXISTS ??', target)
  await query('CREATE TABLE ?? LIKE ??', [target, source])
  await query('INSERT ?? SELECT * FROM ??', [target, source])
}

module.exports = {
  escape: mysql.escape,
  escapeId: mysql.escapeId,
  
  prepareInsert,
  prepareUpdate,
  parseSetter,
  parseValues,

  getConnection,
  recreatePool,

  query,
  queryJSON,
  queryRow,
  queryValue,
  queryObject: queryRow, // synonymous
  // queryField,
  queryMessage,
  querySilent,
  queryInsert,
  queryInsertObject,
  queryInsertArray,

  toCsv,
  fromCsv,
  loadFile,

  copyTable
}

const _ = require('underscore')
const fs = require('fs')
const moment = require('moment')

///
/// Object operation
///

function deepDo(obj, path, fn){
  path = path.split('.');
  for (i = 0; i < path.length - 1; i++)
    obj = obj[path[i]];

  fn.apply(this, [obj, path[i]]);
}

/**
 * Delete object property by dot anotation
 * @param {Object} obj 
 * @param {String} prop: dot anotated property name 
 */
function deepDelete(obj, prop){
  return deepDo(obj, prop, function(obj, prop){
    delete obj[prop];
  });
}

/**
 * Wraps console.log
 */
function log(...args) {
  if (process.env.NODE_ENV !== 'production')
    console.log.apply(null, args)
}

/**
 * Converts to float
 */
function toFloat(str, num) {
  if (_.isUndefined(num)) num = 0
  const parsed = parseFloat(str)
  return _.isNaN(parsed) ? num : parsed
}

/**
 * Escape special characters in JSON string
 */
function escapeJSONString(text) {
  return text.replace(/\\n/g, "\\n")
             .replace(/\\'/g, "\\'")
             .replace(/\\"/g, '\\"')
             .replace(/\\&/g, "\\&")
             .replace(/\\r/g, "\\r")
             .replace(/\\t/g, "\\t")
             .replace(/\\b/g, "\\b")
             .replace(/\\f/g, "\\f");
};

/**
 * Converts field key to lowercase.
 * @param {Object} obj 
 */
function toLowerCaseField(obj) {
  if (!obj) return;
  const newObj = {};
  _.each(obj, (val, key) => {
    // console.log('from %s to %s ...', key, key.toLowerCase());
    newObj[key.toLowerCase()] = val;
  });
  return newObj;
}

/**
 * Logs text to file.
 * @param {string} file - filename
 * @param {string} text  - text message
 */
function logToFile(file, text) {
  fs.writeFileSync(file, `${fs.readFileSync(file, 'utf8')}\n${moment().format('YYYY-MM-DD hh:mm:ss')}: ${text}`)
}

/**
 * Finds deep property value by dot notation
 * @param {Object} obj 
 * @param {String} key: property name with dot notation 
 */
function deepGet(obj, key) {
  try {
    return key.split('.').reduce((o,i)=>o[i], obj);
  }
  catch(e) {
    return undefined;
  }
}

module.exports = {
  deepDelete,
  deepGet,
  log,
  logToFile,
  toFloat,
  escapeJSONString,
  toLowerCaseField
}
const csv = require('csv')

/**
 * Parses csv string
 */
async function parse(data, options) {
  return new Promise((resolve, reject) => {
    csv.parse(data, options, function(err, data) {
      if (err)
        reject(err)
      else
        resolve(data)
    })
  })
}

/**
 * Creates csv
 */
async function generate(data) {
  return new Promise((resolve, reject) => {
    csv.generate(data, (err, output) => {
      if (err)
        reject(err)
      else
        resolve(output)
    })
  })
}

/**
 * Stringify object
 */
async function stringify(data) {
  return new Promise((resolve, reject) => {
    csv.stringify(data, (err, output) => {
      if (err)
        reject(err)
      else
        resolve(output)
    })
  })
};

module.exports = {
  parse,
  generate,
  stringify
}
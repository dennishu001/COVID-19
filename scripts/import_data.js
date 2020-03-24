const fs = require('fs')
const Path = require('path')
const mysql = require('./controllers/mysql')

const dir = Path.join(__dirname, '../data')

async function isNew(file) {
  
}

async function run() {
  const files = fs.readdirSync(dir)

  for (let i = 0; i < files.length; i++) {
    const file = files[i]

    console.log('checking %s ...', file)

    // Check if we have already imported data for this date.
    // TODO: as we progress, we may not need to check every file.
    if (await isNew(file)) {
      // import data
      await importData(file)
    }
  }
}
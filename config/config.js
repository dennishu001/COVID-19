'use strice';

var path = require('path'),
    knophyg = require('konphyg')(path.join(__dirname, './')),
    config = knophyg('main');

var appendFiles = function(key) {
  //console.log(key);
  //console.log(config.jslibs[key]);
  config.jslibs[key].forEach(function(file) {
    if (typeof file === 'string') {
      config.libfiles[key].push(file);
    }
    else if (file.pre) {
      config.libfiles[key].unshift(file.pre);
    }
  });  
};

for (var key in config.jslibs) {
  // excluding main && appended methods
  if (key.substr(0, 1) !== '_' && key !== 'main') {
    // make a clone so we won't change its original value
    config.libfiles[key] = JSON.parse(JSON.stringify(config.jslibs['main']));
    // add additional files
    appendFiles(key);
  }
}

module.exports = config;
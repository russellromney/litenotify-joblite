'use strict';

const nativeBinding = require('./index.js');

module.exports = require('./api')(nativeBinding);

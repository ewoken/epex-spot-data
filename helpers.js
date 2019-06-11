const { chunk } = require('lodash');

function transpose (matrix) {
  const lineCount = matrix.length;
  const columnCount = matrix[0].length;

  const res = Array.from({ length: columnCount }).map(() => Array.from({ length: lineCount }));

  for(let i = 0; i < lineCount; i++) {
    for(let j = 0; j < columnCount; j++) {
      res[j][i] = matrix[i][j];
    }
  }

  return res;
}

function chunkAndChainPromises(data, dataToPromiseFn, chunkSize) {
  return chunk(data, chunkSize).reduce((last, items) => {
    return last.then(array => {
      return Promise.all(items.map(dataToPromiseFn)).then(values => {
        return array.concat(values);
      })
    })
  }, Promise.resolve([]))
}

function toCSV(array, keys) {
  const headers = keys.join(',');
  const lines = array.map(value => {
    return keys.map(key => value[key]).join(',');
  });
  const content = [headers, ...lines, '\n'];
  return content.join('\n');
}

module.exports = {
  transpose,
  chunkAndChainPromises,
  toCSV,
}

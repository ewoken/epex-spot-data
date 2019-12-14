mkdir -p data

cp historicData/*.json data

node index.js || exit 1

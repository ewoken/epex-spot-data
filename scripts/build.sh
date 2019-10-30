mkdir -p data

node --tls-min-v1.0 index.js || exit 1

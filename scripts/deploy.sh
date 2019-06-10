rm -rf build

mkdir build

node generateIndex.js || exit 1

cp -r public/* build/
cp -r data build

./node_modules/.bin/gh-pages -d build --message "Circle CI deploy" --user "Circle CI <circleci@circleci.org>"

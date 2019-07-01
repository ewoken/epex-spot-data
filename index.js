const fs = require('fs');

const fetch = require('node-fetch');
const { parse: parseHTML } = require('node-html-parser');
const moment = require('moment-timezone');
const { chunk, flatten, groupBy, forIn, sortBy, uniqBy, uniq } = require('lodash');

const { transpose, chunkAndChainPromises, toCSV } = require('./helpers');

const DEFAULT_START_DATE = '2005-04-25';

async function fetchDayAheadAuctionData(startOfWeekInput) {
  const startOfWeek = moment(startOfWeekInput).tz('Europe/Paris').startOf('day');
  const endOfWeek = moment(startOfWeek).add(6, "days").format('YYYY-MM-DD');

  console.log(startOfWeek.format());
  const res = await fetch(`https://www.epexspot.com/en/market-data/dayaheadauction/auction-table/${endOfWeek}`);
  const html = await res.text();

  const document = parseHTML(html);
  const hoursTable = document.querySelector('.list.hours.responsive');
  const lines = hoursTable.querySelectorAll('tr');

  const [_, ...data] = lines.map(line => {
    const [__, ___, ...cells] = line.querySelectorAll('th').concat(line.querySelectorAll('td'));
    return cells.map(c => Number(c.innerHTML.replace(',', '')));
  });
  const dataByHour = chunk(flatten(transpose(data)), 2)
    .filter(d => !Number.isNaN(d[0])); // when changing to summer/winter time

  return dataByHour.map((d, i) => ({
    startDate: moment(startOfWeek).add(i, 'hours').format(),
    endDate: moment(startOfWeek).add(i + 1, 'hours').format(),
    price_euros_mwh: d[0],
    volume_mwh: d[1],
  }))
}

function csvFormatter(data) {
  const formattedData = data.map(item => {
    return {
      date: moment(item.startDate).tz('Europe/Paris').format('YYYY-MM-DD'),
      start_hour: moment(item.startDate).tz('Europe/Paris').format('HH:mm'),
      end_hour: moment(item.endDate).tz('Europe/Paris').format('HH:mm'),
      price_euros_mwh: item.price_euros_mwh,
      volume_mwh: item.volume_mwh,
    }
  });
  return toCSV(formattedData, Object.keys(formattedData[0]));
}

async function main() {
  const today = moment().tz('Europe/Paris').startOf('day');
  let startDate = DEFAULT_START_DATE;
  let years = [];

  if (fs.existsSync('./data/years.json')) {
    years = JSON.parse(fs.readFileSync('./data/years.json')).sort();
    const lastYear = years[years.length - 1];
    const currentYear = today.year();
    const firstYear = lastYear === currentYear ? currentYear : lastYear + 1;

    console.log('Cache found, first year :', firstYear);

    startDate = moment().dayOfYear(1).year(firstYear);
  }

  const weekCount = Math.ceil(today.diff(startDate, 'weeks', true));
  const weeks = Array.from({ length: weekCount }).map((_, i) => {
    return moment(startDate).add(i, 'week').format();
  });

  const weekData = await chunkAndChainPromises(weeks, date => fetchDayAheadAuctionData(date), 1);
  const allData = uniqBy(flatten(weekData), 'startDate')
    .filter(item => item.startDate < today.format());

  const dataByYear = groupBy(allData, d => moment(d.startDate).tz('Europe/Paris').year());
  const finalYears = uniq(Object.keys(dataByYear).map(i => Number(i)).concat(years)).sort();

  forIn(dataByYear, (values, year) => {
    const sorted = sortBy(values, d => moment(d.startDate).tz('Europe/Paris').unix());

    console.log(`Write files for year ${year} (${sorted.length} items)`);
    fs.writeFileSync(`./data/${year}.json`, JSON.stringify(sorted));
    fs.writeFileSync(`./data/${year}.csv`, csvFormatter(sorted));
  });

  fs.writeFileSync('./data/years.json', JSON.stringify(finalYears));
}

main();


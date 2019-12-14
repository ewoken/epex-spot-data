const fs = require('fs');

require('dotenv').config()

const fetch = require('node-fetch');
const moment = require('moment-timezone');
const qs = require('qs');
const xml2js = require('xml2js');
const { flatten, groupBy, forIn, sortBy, uniqBy, uniq, range } = require('lodash');


const { ENTSOE_TOKEN } = process.env;
const FRANCE_DOMAIN = '10YFR-RTE------C';
const DATE_FORMAT = "YYYYMMDDHHmm"

const { chunkAndChainPromises, toCSV } = require('./helpers');

const historicYears = range(2005, 2019);
const DEFAULT_START_DATE = '2019-01-01';

moment.tz.setDefault('Europe/Paris');

async function fetchDayAheadAuctionData(startOfWeek) {
  const params = {
    securityToken: ENTSOE_TOKEN,
    documentType: 'A44',
    in_Domain: FRANCE_DOMAIN,
    out_Domain: FRANCE_DOMAIN,
    periodStart: moment(startOfWeek).tz('Europe/London').format(DATE_FORMAT),
    periodEnd: moment(startOfWeek).tz('Europe/London').add(1, 'week').format(DATE_FORMAT),
  }

  console.log(moment(startOfWeek).toISOString());
  const url = `https://transparency.entsoe.eu/api?${qs.stringify(params)}`;
  const res = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
    }
  });
  const xml = await res.text();
  const data = await xml2js.parseStringPromise(xml);
  const timeseries = data.Publication_MarketDocument.TimeSeries
  const days = timeseries.map(t => t.Period[0].Point.map(p => p['price.amount'][0]));

  const dataByHour = days.reduce((res, v, i) => {
    return res.concat(v.map((price, j) => ({
      startDate: moment(startOfWeek).add(i, 'days').add(j, 'hours').toISOString(),
      endDate: moment(startOfWeek).add(i, 'days').add(j + 1, 'hours').toISOString(),
      price_euros_mwh: price
    })))
  }, [])

  return dataByHour;
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
  const today = moment().startOf('day');
  let startDate = moment(DEFAULT_START_DATE).startOf('day');
  let years = [];

  historicYears.map(year => {
    const data = JSON.parse(fs.readFileSync(`./historicData/${year}.json`));
    fs.writeFileSync(`./data/${year}.csv`, csvFormatter(data));
  })

  if (fs.existsSync('./data/years.json')) {
    years = JSON.parse(fs.readFileSync('./data/years.json')).sort();
    const lastYear = years[years.length - 1];
    const currentYear = today.year();
    const firstYear = lastYear === currentYear ? currentYear : lastYear + 1;

    console.log('Cache found, first year :', firstYear);

    startDate = moment().year(firstYear).startOf('year');
  }

  const weekCount = Math.ceil(today.diff(startDate, 'weeks', true));
  const weeks = Array.from({ length: weekCount }).map((_, i) => {
    return moment(startDate).add(i, 'week').format();
  });

  const weekData = await chunkAndChainPromises(weeks, date => fetchDayAheadAuctionData(date), 1);
  const allData = uniqBy(flatten(weekData), 'startDate')
    .filter(item => item.startDate < today.format());

  const dataByYear = groupBy(allData, d => moment(d.startDate).year());
  const finalYears = uniq(Object.keys(dataByYear).map(i => Number(i)).concat(years)).sort();

  forIn(dataByYear, (values, year) => {
    const sorted = sortBy(values, d => moment(d.startDate).unix());

    console.log(`Write files for year ${year} (${sorted.length} items)`);
    fs.writeFileSync(`./data/${year}.json`, JSON.stringify(sorted));
    fs.writeFileSync(`./data/${year}.csv`, csvFormatter(sorted));
  });

  fs.writeFileSync('./data/years.json', JSON.stringify(finalYears));
}

// fetchDayAheadAuctionData(moment('2019-01-01'));

main()
.catch(error => {
  console.error(error);
  process.exit(1);
});


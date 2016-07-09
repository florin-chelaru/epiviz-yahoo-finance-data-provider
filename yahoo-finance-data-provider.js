/**
 * Created by Florin Chelaru ( florinc [at] umd [dot] edu )
 * Date: 5/4/14
 * Time: 10:50 PM
 */

goog.provide('epiviz.plugins.data.YahooFinanceDataProvider');

/**
 * @constructor
 * @extends {epiviz.data.DataProvider}
 */
epiviz.plugins.data.YahooFinanceDataProvider = function () {
  epiviz.data.DataProvider.call(this);

  this._aaplStock = new epiviz.measurements.Measurement(
    'aapl', // The column in the data source table that contains the values for this feature measurement
    'AAPL', // A name not containing any special characters (only alphanumeric and underscores)
    epiviz.measurements.Measurement.Type.FEATURE,
    'stocks', // Data source: the table/data frame containing the data
    'stocks', // An identifier for use to group with other measurements from different data providers
    // that have the same seqName, start and end values
    this.id(), // Data provider
    null, // Formula: always null for measurements coming directly from the data provider
    'Line Track', // Default chart type filter
    null, // Annotation
    0, // Min Value
    600, // Max Value
    ['date'] // Metadata
  );

  this._amznStock = new epiviz.measurements.Measurement(
    'amzn', // The column in the data source table that contains the values for this feature measurement
    'AMZN', // A name not containing any special characters (only alphanumeric and underscores)
    epiviz.measurements.Measurement.Type.FEATURE,
    'stocks', // Data source: the table/data frame containing the data
    'stocks', // An identifier for use to group with other measurements from different data providers
    // that have the same seqName, start and end values
    this.id(), // Data provider
    null, // Formula: always null for measurements coming directly from the data provider
    'Line Track', // Default chart type filter
    null, // Annotation
    0, // Min Value
    300, // Max Value
    ['date'] // Metadata
  );

  this._msInDay = 1000 * 60 * 60 * 24;
  this._aaplStartDate = Math.floor(new Date('1980-12-12').getTime() / this._msInDay);
  this._aaplEndDate = Math.floor(new Date().getTime() / this._msInDay);
};

/**
 * Copy methods from upper class
 */
epiviz.plugins.data.YahooFinanceDataProvider.prototype = epiviz.utils.mapCopy(epiviz.data.DataProvider.prototype);
epiviz.plugins.data.YahooFinanceDataProvider.constructor = epiviz.plugins.data.YahooFinanceDataProvider;

epiviz.plugins.data.YahooFinanceDataProvider.DEFAULT_ID = 'stocks';

/**
 * @param {epiviz.data.Request} request
 * @param {function(epiviz.data.Response)} callback
 * @override
 */
epiviz.plugins.data.YahooFinanceDataProvider.prototype.getData = function (request, callback) {
  var requestId = request.id();
  var action = request.get('action');
  var seqName = request.get('seqName');
  var start = request.get('start');
  var end = request.get('end');
  var datasource = request.get('datasource');
  var measurement = request.get('measurement');

  if (start && end) {
    var startDate = new Date(start * this._msInDay);
    var endDate = new Date(end * this._msInDay);
    var globalStartIndex = start - this._aaplStartDate + 1;
    var dbQuery = sprintf(
      'select * from yahoo.finance.historicaldata where symbol = "%s" and startDate = "%s" and endDate = "%s"',
      (!measurement) ? 'aapl' : measurement,
      sprintf('%04s-%02s-%02s', startDate.getFullYear(), startDate.getMonth() + 1, startDate.getDate()),
      sprintf('%04s-%02s-%02s', endDate.getFullYear(), endDate.getMonth() + 1, endDate.getDate()));
    var query =
      'https://query.yahooapis.com/v1/public/yql?format=json&diagnostics=false&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=&q=' +
      encodeURIComponent(dbQuery);
  }

  var self = this;

  switch (action) {
    case epiviz.data.Request.Action.GET_ROWS:
      if (seqName != 'stocks') {
        // Nothing to return
        callback(epiviz.data.Response.fromRawObject({
          data: {
            values: { id: null, start: [], end:[], strand: [], metadata:{date:[]} },
            globalStartIndex: null,
            useOffset: false
          },
          requestId: requestId
        }));
        return;
      }

      epiviz.data.WebServerDataProvider.makeGetRequest(query, function(jsondata) {
        if (!jsondata['query'] || !jsondata['query']['count']) {
          // Nothing to return
          callback(epiviz.data.Response.fromRawObject({
            data: {
              values: { id: null, start: [], end:[], strand: [], metadata:{date:[]} },
              globalStartIndex: null,
              useOffset: false
            },
            requestId: requestId
          }));
          return;
        }

        var ids = [], starts = [], ends = [], strands = '*', dates = [];
        var results = jsondata['query']['results']['quote'];
        var currentPos = start;
        for (var i = results.length - 1; i >= 0; --i) {
          var d = new Date(results[i]['Date']);
          var dIndex = Math.floor(d.getTime() / self._msInDay);

          if (dIndex < currentPos) { continue; }

          for (; currentPos <= dIndex; ++currentPos) {
            ids.push(currentPos - self._aaplStartDate + 1);
            starts.push(currentPos);
            ends.push(currentPos);
            dates.push(results[i]['Date']);
          }
        }

        callback(epiviz.data.Response.fromRawObject({
          data: {
            values: { id: ids, start: starts, end: ends, strand: strands, metadata:{date:dates} },
            globalStartIndex: globalStartIndex,
            useOffset: false
          },
          requestId: requestId
        }));
      });

      return;

    case epiviz.data.Request.Action.GET_VALUES:
      if (seqName != 'stocks') {
        // Nothing to return
        callback(epiviz.data.Response.fromRawObject({
          data: { values: [], globalStartIndex: null },
          requestId: requestId
        }));
        return;
      }

      epiviz.data.WebServerDataProvider.makeGetRequest(query, function(jsondata) {
        if (!jsondata['query'] || !jsondata['query']['count']) {
          // Nothing to return
          callback(epiviz.data.Response.fromRawObject({
            data: { values: [], globalStartIndex: null },
            requestId: requestId
          }));
          return;
        }

        var values = [];
        var results = jsondata['query']['results']['quote'];
        var currentPos = start;
        for (var i = results.length - 1; i >= 0; --i) {
          var d = new Date(results[i]['Date']);
          var dIndex = Math.floor(d.getTime() / self._msInDay);

          if (dIndex < currentPos) { continue; }

          for (; currentPos <= dIndex; ++currentPos) {
            values.push(parseFloat(results[i]['Close']));
          }
        }

        callback(epiviz.data.Response.fromRawObject({
          data: { values: values, globalStartIndex: globalStartIndex },
          requestId: requestId
        }));
      });

      return;

    case epiviz.data.Request.Action.GET_MEASUREMENTS:
      callback(epiviz.data.Response.fromRawObject({
        requestId: request.id(),
        data: {
          id: [this._aaplStock.id(), this._amznStock.id()],
          name: [this._aaplStock.name(), this._amznStock.name()],
          type: [this._aaplStock.type(), this._amznStock.type()],
          datasourceId: [this._aaplStock.datasourceId(), this._amznStock.datasourceId()],
          datasourceGroup: [this._aaplStock.datasourceGroup(), this._amznStock.datasourceGroup()],
          defaultChartType: [this._aaplStock.defaultChartType(), this._amznStock.defaultChartType()],
          annotation: [this._aaplStock.annotation(), this._amznStock.annotation()],
          minValue: [this._aaplStock.minValue(), this._amznStock.minValue()],
          maxValue: [this._aaplStock.maxValue(), this._amznStock.maxValue()],
          metadata: [this._aaplStock.metadata(), this._amznStock.metadata()]
        }
      }));
      return;

    case epiviz.data.Request.Action.GET_SEQINFOS:
      callback(epiviz.data.Response.fromRawObject({
        requestId: request.id(),
        data: [['stocks', this._aaplStartDate, this._aaplEndDate]]
      }));
      return;

    default:
      epiviz.data.DataProvider.prototype.getData.call(this, request, callback);
      break;
  }
};


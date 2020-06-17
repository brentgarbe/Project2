var globalData = {};
var tSymbol;
var data;

function unpack(rows, index) {
  return rows.map(function(row) {
    return row[index];
  });
}


// Event listener, this happens when you click the button.
d3.select("#submit").on("click", function () {
  // prevents it from refreshing the page on submit
  d3.event.preventDefault();

  // creates the variable for ticker symbol, stockInput is the button name from html
  tSymbol = d3.select("#stockInput").node().value;

  // printing the tSymbol so we know what we have
  console.log(tSymbol);
  // pass the information to the plot building function
  buildPlot(tSymbol);
  buildDescription(tSymbol);
  buildEPSPlot(tSymbol);
});

// we are getting amazon for the specific date range
function buildPlot(tSymbol) {
  // Here we are going to insert our API (sourced from FinnHub, but comes from our SQL server)
  // This gives us the value
  var url2 = `api2/${tSymbol}`;
  var url = `api/${tSymbol}`;
  var url3 = `api3/${tSymbol}`;

  // this is what will make the request for the API
  d3.json(url2).then(function (data) {
    // Grab values from the response json object to build the plots
    // console.log(data);
    
    var name = data.Ticker_Symbol;
    
    var dates = data.Date_Time;
    // console.log(dates)

    var openingPrices = data.Open;
    // console.log(openingPrices);
    var highPrices = data.High;
    // console.log(highPrices);
    var lowPrices = data.Low;
    // console.log(lowPrices);
    var closingPrices  = data.Close;
    // console.log(closingPrices);
    var volume = data.Volume;
    // console.log("cnew dates are")
    // console.log(dates[0], dates[-1])

    // // var openingPrices = data.map((d) => d.Open);
    // var highPrices = data.map((d) => d.High);
    // var lowPrices = data.map((d) => d.Low);
    
    // var closingPrices = data.map((d) => d.Close);
    // var volume = data.map((d) => d.Volume);
    
    // console.log(openingPrices)

    var trace1 = {
      type: "scatter",
      mode: "lines",
      name: tSymbol,
      x: dates,
      y: closingPrices,
      line: {
        color: "#17BECF",
      },
    };

    // Candlestick Trace
    var trace2 = {
      type: "candlestick",
      x: dates,
      high: highPrices,
      low: lowPrices,
      open: openingPrices,
      close: closingPrices,
    };

    // customize colors check this website for additional information on candle sticks
    // plotly.com/javascript/candlestick-charts
    var cdata = [trace1, trace2];
    var startDate = dates[0];
    var endDate = dates[-1];

    var layout = {
      title: `${tSymbol} closing prices`,
      xaxis: {
        range: [startDate, endDate],
        type: "time"
        // ,
        // rangeslider: {visible: true}
      },
      yaxis: {
        autorange: true,
        type: "linear"
      }
      // ,
      // showlegend: true,
    };

    Plotly.newPlot("plot", cdata, layout);
  });

  // this is what will make the request for the API
  d3.json(url).then(function (data) {
    // Grab values from the response json object to build the plots
    // console.log(data);

    // build the table
    function buildTable(data) {
      var table = d3.select("#summary-table");
      var tbody = table.select("tbody");
      var trow;
      var dates = data.map((d, i) => d.Date_Time);
      var openingPrices = data.map((d) => d.Open);
      var highPrices = data.map((d) => d.High);
      var lowPrices = data.map((d) => d.Low);
      var closingPrices = data.map((d) => d.Close);
      var volume = data.map((d) => d.Volume);

      // console.log(globalData.dates[1]);

      // This is how we are able to create the datapoints in the table below
      data.forEach((d, i) => {
        trow = tbody.append("tr");
        trow.append("th").text(dates[i]);
        trow.append("th").text(openingPrices[i]);
        trow.append("th").text(highPrices[i]);
        trow.append("th").text(lowPrices[i]);
        trow.append("th").text(closingPrices[i]);
        trow.append("th").text(volume[i]);
      });
    }
    buildTable(data);
  
  
  

  
  
  });

    // this is what will make the request for the API
    d3.json(url3).then(function (data) {
      // Grab values from the response json object to build the plots
      console.log(data);
  
      // build the table
      function buildEPSTable(data) {
        var table = d3.select("#eps-table");
        var tbody = table.select("tbody");
        var trow;
        var year = data.map((d, i) => d.Year);
        var quarter = data.map((d, i) => d.Quarter);
        var epsActual = data.map((d) => d.EPS_Actual);
        var epsEstimate = data.map((d) => d.EPS_Estimate);
        var epsDiff = data.map((d) => d.EPS_Diff);
        var actualRevenue = data.map((d) => d.Actual_Revenue);
        var estimatedRevenue = data.map((d) => d.Estimated_Revenue);
        var diffRevenue = data.map((d) => d.Revenue_Diff);
  
        // console.log(globalData.dates[1]);
  
        // This is how we are able to create the datapoints in the table below
        data.forEach((d, i) => {
          trow = tbody.append("tr");
          trow.append("th").text(year[i]);
          trow.append("th").text(quarter[i]);
          trow.append("th").text(epsActual[i]);
          trow.append("th").text(epsEstimate[i]);
          trow.append("th").text(epsDiff[i]);
          trow.append("th").text(actualRevenue[i]);
          trow.append("th").text(estimatedRevenue[i]);
          trow.append("th").text(diffRevenue[i]);
        });
      }
      buildEPSTable(data);
    
    
    
  
    
    
    });
}

function buildDescription() {
  var url1 = `api1/${tSymbol}`;

  d3.json(url1).then(function (info) {
    // console.log(info);
    var description = info[0].Description;
    // console.log(description);
    d3.select(".company").html(description);
  });
}







// // we are getting amazon for the specific date range
// function buildEPSPlot(tSymbol) {
//   // Here we are going to insert our API (sourced from FinnHub, but comes from our SQL server)
//   // This gives us the value
//   var url3 = `api3/${tSymbol}`;

//   // this is what will make the request for the API
//   d3.json(url3).then(function (data) {
//     // Grab values from the response json object to build the plots
//     console.log("data is")
//     console.log(data);
    
//     var year = data.Year;
//     var quarter = data.Quarter;
//     var epsActual = data.EPS_Actual;
//     var epsEstimate = data.EPS_Estiamte;
//     var actualRevenue = data.Actual_Revenue;
//     var estimatedRevenue = data.Estimated_Revenue;
//     var epsDiff = data.EPS_Diff;
//     var revenueDiff = data.Revenue_Diff;
//     console.log("year is")
//     console.log(year)

//     var table2 = d3.select("#eps-table");
//     var tbody1 = table2.select("tbody1");
//     var trows;

//     for(var i=0; i< year.length; i++){
//       console.log(year[i])
//       trows = tbody1.append("trc");
//       trows.append("th").text(year[i]);
      

//       // trow = tbody1.append("tr");
//       // trow.append("td").text(year[i]);
//     }
    

//     // year.forEach( (i) => {
//     //   trow = tbody1.append("tr");
//     //   trow.append("td").text(year[i]);
//     //   console.log(trow)
//     //   // trow.append("td").text(i.quarter);
//     //   // trow.append("td").text(i.epsActual);
//     //   // trow.append("td").text(i.epsEstimate);
//     //   // trow.append("td").text(i.actualRevenue);
//     //   // trow.append("td").text(i.estimatedRevenue);
//     //   // trow.append("td").text(i.epsDiff);
//     //   // trow.append("td").text(i.revenueDiff);
//     //   });
//     buildEPSPlot(trows);
//     });
// }




// // we are getting amazon for the specific date range
// function buildEPSPlot(tSymbol) {
//   // Here we are going to insert our API (sourced from FinnHub, but comes from our SQL server)
//   // This gives us the value
//   var url3 = `api3/${tSymbol}`;
//   // this is what will make the request for the API
//   d3.json(url3).then(function (data) {
//     // Grab values from the response json object to build the plots
//     console.log(data);
//     var table = d3.select("#eps-table");
//     var tbody1 = table.select("tbody");
//     var trow;
//     var year = data.map((d, i) => data.Year);
//     var quarter = data.map((d, i) => data.Quarter);
//     var epsActual = data.map((d, i) => data.EPS_Actual);
//     var actualRevenue = data.map((d, i) => data.Actual_Revenue);
//     var estimatedRevenue = data.map((d, i) => data.Estimated_Revenue);
//     var epsDiff = data.map((d, i) => data.EPS_Diff);
//     var revenueDiff = data.map((d, i) => data.Revenue_Diff);
//     console.log('Revenue Diff is')
//     console.log(revenueDiff)
//     // This is how we are able to create the datapoints in the table below
//     data.forEach((d, i) => {
//       trow = tbody1.append("tr");
//       trow.append("td").text(year[i]);
//       trow.append("td").text(quarter[i]);
//       trow.append("td").text(epsActual[i]);
//       trow.append("td").text(epsEstimate[i]);
//       trow.append("td").text(actualRevenue[i]);
//       trow.append("td").text(estimatedRevenue[i]);
//       trow.append("td").text(epsDiff[i]);
//       trow.append("td").text(revenueDiff[i]);
//     });
//     buildEPSPlot(data);
//   });
// }
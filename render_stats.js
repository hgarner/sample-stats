var graph = d3.select("#graph");
var data;

var width = 800;
var height = 600;
var date_parser = d3.timeParse("%Y-%m-%d");

function renderGraph(datapoints, study) {
  let x = d3.scaleTime()
    .range([0, width-100]);
  let y = d3.scaleLinear()
    .range([height-100, 0]);

  //parse the date of each entry
  datapoints[study].forEach(function(d) {
    d.date = date_parser(d.date);
  });

  //sort by date
  datapoints[study].sort(function(a, b) {
    if (a.date < b.date) {
      return -1;
    }
    if (a.date > b.date) {
      return 1;
    }
    return 0;
  });


  let val_line = d3.line()
    .x(function(d) { console.log(d.date); return x(d.date); })
    .y(function(d) { console.log(d.mean); return y(d.mean); });

  x.domain(d3.extent(datapoints[study], function(d) { return d.date; }));
  //y.domain([3000, d3.max(datapoints[study], function(d) { return d.mean; })]);
  //set a max y val of the max value of the means or 5000 (whichever is
  //larger)
  //avoids issues with very small changes looking significant on the graph
  y.domain([3000, Math.max(5000, d3.max(datapoints[study], function(d) { return d.mean; }))]);

  //console.log(x.domain());
  //console.log(y.domain());

  let svg = graph.append("svg")
    .attr("width", width)
    .attr("height", height)
    .append("g")
    .attr("transform", "translate(50, 50)");

  //add the path for the values
  let line_string = val_line(datapoints[study]);
  //console.log(line_string);
  svg.append("path")
    .data([datapoints[study]])
    .attr("d", val_line)
    .attr("class", "line");
  //console.log(svg);
  //console.log(graph);

  //add the axes
  svg.append("g")
    .attr("transform", "translate(0, "+(height-100)+")")
    .call(d3.axisBottom(x));
  svg.append("g")
    .call(d3.axisLeft(y));

  //add title
  svg.append("g")
    .attr("transform", "translate(0, -10)")
    .append("text")
    .text("study_no: "+study);
}

function processData(data) {
  //console.log(data);

  let datapoints = {};

  for (let datafile of data) {
    //get metadata
    let filename = datafile.input_filename;
    let date_regex = /([0-9]{4})\-([0-9]{2})\-([0-9]{2})/;
    let data_date_match =  filename.match(date_regex);

    let data_date = data_date_match[1]+'-'+data_date_match[2]+'-'+data_date_match[3];
    //let data_date = date_parser(data_date_match[0]);

    let ignore_keys = ['input_filename'];
    
    for (let study in datafile) {
      //skip if key (study) is in ignore_keys
      if (ignore_keys.indexOf(study) !== -1) {
        continue;
      }

      if (typeof datapoints[study] === "undefined") {
        datapoints[study] = [];
      }

      datapoints[study].push({'date': data_date, 'mean': datafile[study].proc_times.mean});
    }
  }

  console.log(datapoints);

  //now go through the compiled data (by study) and display graphs
  for (let study in datapoints) {
    renderGraph(datapoints, study)
  }
}

function passData(parse_error, node_data) {
  console.log(parse_error);
  processData(node_data);
}

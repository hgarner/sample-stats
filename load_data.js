function loadData() {
  var data_queue = d3.queue();

  data_queue
    .defer(d3.json, './data/all-studies-sample-processing-time-manage-nonunique-ids.2018-01-23-1153_summarystats.23012018.json').defer(d3.json, './data/all-studies-sample-processing-time-manage-nonunique-ids.2017-09-18-10:26_summarystats.18092017.json').defer(d3.json, './data/all-studies-sample-processing-time-manage-nonunique-ids.2018-01-20-1153_summarystats.23012018.json').defer(d3.json, './data/all-studies-sample-processing-time-manage-nonunique-ids.2018-01-10-1153_summarystats.23012018.json').defer(d3.json, './data/all-studies-sample-processing-time-manage-nonunique-ids.2018-01-15-1153_summarystats.23012018.json').defer(d3.json, './data/all-studies-sample-processing-time-manage-nonunique-ids.2018-01-01-1153_summarystats.23012018.json')
    .awaitAll(passData);

}

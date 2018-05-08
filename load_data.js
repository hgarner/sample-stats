function loadData() {
  var data_queue = d3.queue();

  data_queue
    .defer(d3.json, '/projects/alspac_ieu_lab/monitoring/sample_processing_time/data/processed/all-studies-sample-processing-time-manage-nonunique-ids.2018-03-04-0430_summarystats.08032018.json').defer(d3.json, '/projects/alspac_ieu_lab/monitoring/sample_processing_time/data/processed/all-studies-sample-processing-time-manage-nonunique-ids.2018-03-04-0430_summarystats.04032018.json').defer(d3.json, '/projects/alspac_ieu_lab/monitoring/sample_processing_time/data/processed/all-studies-sample-processing-time-manage-nonunique-ids.2018-02-25-0430_summarystats.25022018.json').defer(d3.json, '/projects/alspac_ieu_lab/monitoring/sample_processing_time/data/processed/all-studies-sample-processing-time-manage-nonunique-ids.2018-02-20-1143_summarystats.20022018.json')
    .awaitAll(passData);

}

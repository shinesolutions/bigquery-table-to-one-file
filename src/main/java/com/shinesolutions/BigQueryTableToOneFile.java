package com.shinesolutions;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED;
import static org.apache.beam.sdk.io.FileBasedSink.CompressionType.GZIP;

/**
 * BigQuery -> ParDo -> GCS (one file)
 */
public class BigQueryTableToOneFile {
    private static final String BIGQUERY_TABLE = "bigquery-samples:wikipedia_benchmark.Wiki1M";
    private static final String GCS_OUTPUT_FILE = "gs://bigquery-table-to-one-file/output/wiki_1M.csv";
    private static final String[] TABLE_ROW_FIELDS = {"year", "month", "day", "wikimedia_project", "language", "title", "views"};

    public static void main(String[] args) throws Exception {
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        options.setAutoscalingAlgorithm(THROUGHPUT_BASED);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(BigQueryIO.read().from(BIGQUERY_TABLE))
                .apply(ParDo.of(new DoFn<TableRow, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        String commaSep = Arrays.asList(TABLE_ROW_FIELDS)
                                .stream()
                                .map(field -> c.element().get(field).toString().replaceAll(",", "-"))
                                .collect(Collectors.joining(","));
                        c.output(commaSep);
                    }
                }))
                .apply(TextIO.write().to(GCS_OUTPUT_FILE)
                        .withoutSharding()
                        .withWritableByteChannelFactory(GZIP)
                );
        pipeline.run();
    }
}

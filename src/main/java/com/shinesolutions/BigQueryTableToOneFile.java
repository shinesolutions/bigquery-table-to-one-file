package com.shinesolutions;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED;
import static org.apache.beam.sdk.io.FileBasedSink.CompressionType.GZIP;

/**
 * BigQuery -> ParDo -> GCS (one file)
 */
public class BigQueryTableToOneFile {
    private static final String BIGQUERY_TABLE = "bigquery-samples:wikipedia_benchmark.Wiki1B";
    private static final String GCS_OUTPUT_FILE = "gs://bigquery-table-to-one-file/output/result.csv";

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
                        TableRow row = c.element();
                        c.output(row.toPrettyString()); //e.g. {year=2010, month=1, day=1, wikimedia_project=m, language=commons, title=Image:Coat_of_arms_kautenbach_luxbrg.png, views=1}
                    }
                }))
                .apply(TextIO.write().to(GCS_OUTPUT_FILE)
                        .withoutSharding()
                        .withWritableByteChannelFactory(GZIP)
                );
        pipeline.run();
    }
}

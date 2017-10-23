package com.shinesolutions;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.stream.Collectors;

import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED;
import static org.apache.beam.sdk.io.FileBasedSink.CompressionType.GZIP;

/**
 * BigQuery -> ParDo -> GCS (one file)
 */
public class BigQueryTableToOneFile {
    public static void main(String[] args) throws Exception {
        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(TemplateOptions.class);
        options.setAutoscalingAlgorithm(THROUGHPUT_BASED);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(BigQueryIO.read().from(options.getBigQueryTableName()))
                .apply(ParDo.of(new DoFn<TableRow, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        String commaSep = c.element().values()
                                .stream()
                                .map(cell -> cell.toString().trim())
                                .collect(Collectors.joining("\",\""));
                        c.output(commaSep);
                    }
                }))
                .apply(TextIO.write().to(options.getOutputFile())
                        .withoutSharding()
                        .withWritableByteChannelFactory(GZIP)
                );
        pipeline.run();
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("The BigQuery table to read from in the format project:dataset.table")
        ValueProvider<String> getBigQueryTableName();

        void setBigQueryTableName(ValueProvider<String> value);

        @Description("The name of the output file to produce in the format gs://bucket_name/filname.csv")
        ValueProvider<String> getOutputFile();

        void setOutputFile(ValueProvider<String> value);
    }
}

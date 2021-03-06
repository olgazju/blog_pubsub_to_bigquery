/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package pubsub_to_bigquery;

import java.util.*;

import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.KV;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.FILE_LOADS;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;

//import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

import static pubsub_to_bigquery.TransformToBQ.SUCCESS_TAG;
import static pubsub_to_bigquery.TransformToBQ.FAILURE_TAG;

public class App {

	public interface MyOptions extends DataflowPipelineOptions, DirectOptions {
    @Description("BigQuery project")
    @Default.String("my_bq_project")
    String getBQProject();

    void setBQProject(String value);

    @Description("BigQuery dataset")
    @Default.String("my_bq_dataset")
    String getBQDataset();

    void setBQDataset(String value);

    @Description("Bucket path to collect pipeline errors in json files")
    @Default.String("errors")

    String getErrorsBucket();
    void setErrorsBucket(String value);

    @Description("Bucket path to collect pipeline errors in json files")
    @Default.String("bi_pipeline_dev")
    String getBucket();

    void setBucket(String value);

    @Description("Pubsub project")
    @Default.String("my_pubsub_project")
    String getPubSubProject();

    void setPubSubProject(String value);

    @Description("Pubsub subscription")
    @Default.String("my_pubsub_subscription")
    String getSubscription();

    void setSubscription(String value);
  }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyOptions.class);
  		MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                  .withValidation()
                                                  .as(MyOptions.class);
        Pipeline p = Pipeline.create(options);

        final String PROJECT = options.getProject();
        final String ERRORS_BUCKET = String.format("gs://%s/%s/", options.getBucket(), options.getErrorsBucket());
        final String SUBSCRIPTION = String.format("projects/%s/subscriptions/%s", options.getPubSubProject(), options.getSubscription());
        final int STORAGE_LOAD_INTERVAL = 5; //5 minutes
        final int STORAGE_NUM_SHARDS = 1;

        final String BQ_PROJECT = options.getBQProject();
        final String BQ_DATASET = options.getBQDataset();

        System.out.println(options);

        // 1. Read from PubSub
        PCollection<String> pubsubMessages = p
                .apply("ReadPubSubSubscription", PubsubIO.<String>readStrings().fromSubscription(SUBSCRIPTION));

        // 2. Print elements
        pubsubMessages.apply("PrintElements", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c)  {
                System.out.println(c.element());
            }
        }));

        // 3. Transform element to TableRow
        PCollectionTuple results = pubsubMessages.apply("TransformToBQ", TransformToBQ.run());

        // 4. Write the successful records out to BigQuery

        WriteResult writeResult = results.get(SUCCESS_TAG).apply("WriteSuccessfulRecordsToBQ", BigQueryIO.writeTableRows()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()) //Retry all failures except for known persistent errors.
                .withWriteDisposition(WRITE_APPEND)
                .withCreateDisposition(CREATE_NEVER)
                .withExtendedErrorInfo() //- getFailedInsertsWithErr
                .ignoreUnknownValues()
                .skipInvalidRows()
                .withoutValidation()
                .to((row) -> {
                    String tableName = Objects.requireNonNull(row.getValue()).get("table").toString();
                    return new TableDestination(String.format("%s:%s.%s", BQ_PROJECT, BQ_DATASET, tableName), "Some destination");
                })
        );
        
        // 5. Write rows that failed to GCS using windowing of STORAGE_LOAD_INTERVAL interval
        // Flatten failed rows after TransformToBQ with failed inserts

        PCollection<KV<String, String>> failedInserts = writeResult.getFailedInsertsWithErr()
                .apply("MapFailedInserts", MapElements.via(new SimpleFunction<BigQueryInsertError, KV<String, String>>() {
                                                               @Override
                                                               public KV<String, String> apply(BigQueryInsertError input) {
                                                                   return KV.of("FailedInserts", input.getError().toString() + " for table" + input.getRow().get("table") + ", message: "+ input.getRow().toString());
                                                               }
                                                           }
                ));

        
        failedInserts.apply("LogFailedInserts", ParDo.of(new DoFn<KV<String, String>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c)  {
                System.out.format("%s: %s", c.element().getKey(), c.element().getValue());
            }
        }));


        //! add debug logs instead of prints

        p.run();


    }
}

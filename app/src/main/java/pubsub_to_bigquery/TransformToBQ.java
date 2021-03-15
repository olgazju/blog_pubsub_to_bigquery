package pubsub_to_bigquery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.nio.charset.StandardCharsets;

import com.google.api.services.bigquery.model.TableRow;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.paloaltonetworks.bi_pipeline.tools.JsonMapper;
//import com.paloaltonetworks.bi_pipeline.tools.Metric;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

//import javax.annotation.Nullable;

import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.coders.Coder.Context;

//import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;


public class TransformToBQ {

    public static PTransform<PCollection<String>, PCollectionTuple> run() {
        return new TransformToBQ.JsonToTableRow();
    }

    static final TupleTag<TableRow> SUCCESS_TAG =
            new TupleTag<TableRow>(){};
    static final TupleTag<KV<String, String>> FAILURE_TAG =
            new TupleTag<KV<String, String>>(){};

    private static class JsonToTableRow
            extends PTransform<PCollection<String>, PCollectionTuple> {


        @Override
        public PCollectionTuple expand(PCollection<String> jsonStrings) {
            return jsonStrings
                    .apply(ParDo.of(new DoFn<String, TableRow>() {
                        @ProcessElement
                        public void processElement(ProcessContext context) {
                            String jsonString = context.element();
                            System.out.println("processElement");

                            byte[] message_in_bytes = jsonString.getBytes(StandardCharsets.UTF_8);

                            // checking message size if it's not more than 5 Mb for POST body
                            if (message_in_bytes.length >= 5 * 1024 * 1024) {
                                System.out.printf("Error: too big row of %d bytes\n", message_in_bytes.length);
                                context.output(FAILURE_TAG, KV.of("TooBigRow", jsonString));
                            }

                            TableRow row;
                            // Parse the JSON into a {@link TableRow} object.
                            try (InputStream inputStream = new ByteArrayInputStream(message_in_bytes)) 
                            {
                                row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
                                context.output(row);

                             } catch (IOException e) {
                                //throw new RuntimeException("Failed to serialize json to table row: " + json, e);
                                context.output(FAILURE_TAG, KV.of("JsonParseError", jsonString));
                             }
                             

                        }
                    }).withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));
        }

    }
}

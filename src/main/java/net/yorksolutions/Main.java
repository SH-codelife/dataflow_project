package net.yorksolutions;
import com.google.api.services.bigquery.model.TableFieldSchema;
//import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
//import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

//import java.lang.reflect.Array;
import java.util.Arrays;
//import java.util.List;

//comment to test Github push
//comment to test Github push
public class Main {
    //create main method
    public static void main(String[] args) {
        /*create object to hold Dataflow pipeline options, pass a .class to extend parent object
        PLOF constructs a PLO composable with any other PLO.as() method */
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        //set attributes of the pipeline object
        options.setJobName("sh-dataflow");
        options.setProject("york-cdf-start");
        options.setRegion("us-central1");
        //.class extends the object
        options.setRunner(DataflowRunner.class);
        //options.setGcpTempLocation("gs://sh_temp_test/tmp"); //isn't using this (yet)
        options.setStagingLocation("gs://sh_temp_test/staging"); //is required to run

        //step 1: create pipeline object, pass "options" object to create function
        Pipeline p = Pipeline.create(options);

        //table specs for writing prodcut_views into BigQuery
        TableSchema output_views = new TableSchema()
                .setFields(
                        Arrays.asList(
                                new TableFieldSchema()
                                        .setName("CUST_TIER_CODE")
                                        .setType("STRING")
                                        .setMode("Required"),
                                new TableFieldSchema()
                                        .setName("SKU")
                                        .setType("INT64")
                                        .setMode("Required"),
                                new TableFieldSchema()
                                        .setName("total_no_of_product_views")
                                        .setType("INT64")
                                        .setMode("Required")
                ));
        //table specs for writing sales_views into BigQuery
        TableSchema output_sales = new TableSchema()
                .setFields(
                        Arrays.asList(
                                new TableFieldSchema()
                                        .setName("CUST_TIER_CODE")
                                        .setType("STRING")
                                        .setMode("Required"),
                                new TableFieldSchema()
                                        .setName("SKU")
                                        .setType("INT64")
                                        .setMode("Required"),
                                new TableFieldSchema()
                                        .setName("total_sales_amount")
                                        .setType("FLOAT")
                                        .setMode("Required")
                        ));
        //read data from 2 bigquery tables, join for product_views output table
        PCollection<TableRow> rows = p.apply(
                BigQueryIO.readTableRows()
                        .fromQuery("SELECT c.CUST_TIER_CODE, p.SKU, count(p.SKU) AS `total_no_of_product_views` " +
                                "FROM york-cdf-start.final_input_data.product_views AS p " +
                                        "JOIN york-cdf-start.final_input_data.customers AS c ON p.customer_id=c.CUSTOMER_ID " +
                                        "GROUP BY c.CUST_TIER_CODE, p.SKU")
                        .usingStandardSql()
        );
        //read data from 2 bigquery tables, join for sales_amount output table
        PCollection<TableRow> view_rows = p.apply(
                BigQueryIO.readTableRows()
                        .fromQuery("SELECT c.CUST_TIER_CODE, o.SKU, round(sum(o.order_amt),2) AS total_sales_amount " +
                                "FROM york-cdf-start.final_input_data.customers AS c " +
                                "JOIN york-cdf-start.final_input_data.orders AS o ON o.customer_id=c.CUSTOMER_ID " +
                                "GROUP BY c.CUST_TIER_CODE, o.SKU")
                        .usingStandardSql()
        );
        //write data into product_views table
        rows.apply(BigQueryIO.writeTableRows()
                .to("york-cdf-start:final_sonja_hayden.CUST_tier_code-sku-total_no_of_product_views")
                .withSchema(output_views)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
        );
        //write data into sales_amount table
        view_rows.apply(BigQueryIO.writeTableRows()
                .to("york-cdf-start:final_sonja_hayden.CUST_tier_code-sku-total_sales_amount")
                .withSchema(output_sales)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
        );

        /*Arrays.asList=changes to the list "write through" to the array
        the method acts as bridge between array-based and collection-based APIs */

        //final List<String> input = Arrays.asList("first_word", "second_word", "third", "dataflow");
        /*Create<T> takes a collection of elements <T> from pipeline construct and returns a PColl containing the elements.
         A good use for when a PColl needs to be created w/o dependencies on files, etc.
         so,  Create.of(input) uses "input" and PTransform Text.IO.write is applied and sent to GS bucket*/
        //p.apply(Create.of(input)).apply(TextIO.write().to("gs://sh_temp_test/output").withSuffix(".txt"));
        /*method to run pipeline, waits until run finishes and returns the final status*/

        p.run().waitUntilFinish();
    }
}
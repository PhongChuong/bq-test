package org.chuongphtest;

import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.http.HttpTransportOptions;
import org.threeten.bp.Duration;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String projectId = "TODO";

        // BigQuery bq = BigQueryOptions.getDefaultInstance().getService();

        BigQueryOptions.Builder bigQueryOptions = BigQueryOptions.newBuilder()
            .setProjectId(projectId);
        bigQueryOptions.setRetrySettings(RetrySettings.newBuilder()
                .setMaxAttempts(10)
                .setRetryDelayMultiplier(1.5)
                .setTotalTimeout(Duration.ofMinutes(5))
                .build())
            .setTransportOptions(HttpTransportOptions.newBuilder()
                .setConnectTimeout(300000)//5 minutes
                .setReadTimeout(300000)//5 minutes
                .build());
        bigQueryOptions.setProjectId(projectId);
        BigQuery bq = bigQueryOptions.build().getService();

        // Validate by list dataset.
        Page<Dataset> datasets = bq.listDatasets(projectId);
        if (datasets == null) {
            System.out.println("Dataset does not contain any models");
            return;
        }
        datasets
            .iterateAll()
            .forEach(
                dataset -> System.out.printf("Success! Dataset ID: %s ", dataset.getDatasetId()));
    }
}

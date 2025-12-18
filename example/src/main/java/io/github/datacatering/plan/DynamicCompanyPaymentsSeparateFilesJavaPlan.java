package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder;
import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.TimestampType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Alternative approach: Generate separate files for each company
 * This creates one JSON file per company with exact record counts
 */
public class DynamicCompanyPaymentsSeparateFilesJavaPlan extends PlanRun {
    {
        // Same configuration map
        var companyRecordCounts = new LinkedHashMap<String, Long>();
        companyRecordCounts.put("Acme Corp", 1000L);
        companyRecordCounts.put("TechStart Inc", 500L);
        companyRecordCounts.put("Global Solutions", 750L);
        companyRecordCounts.put("SmallBiz LLC", 250L);

        var conf = configuration()
                .generatedReportsFolderPath("/opt/app/data/report");

        // Dynamically create and execute tasks for each company
        // Note: For simplicity, we're using multiple calls instead of collecting tasks
        // In production, you might want to collect and execute in a loop
        List<ConnectionTaskBuilder<?>> tasks = companyRecordCounts.entrySet().stream().map(stringLongEntry -> {
            var companyName = stringLongEntry.getKey();
            var recordCount = stringLongEntry.getValue();

            // Sanitize company name for file path
            String sanitizedName = companyName.toLowerCase().replaceAll("[^a-z0-9]", "_");

            var companyTask = json("payments_" + sanitizedName, "/opt/app/data/payments/" + sanitizedName)
                    .count(count().records(recordCount))
                    .fields(
                            // Static company name for this file
                            field().name("company_name").sql("'" + companyName + "'"),

                            // Payment ID with company prefix
                            field().name("payment_id")
                                    .sql("CONCAT('" + sanitizedName.toUpperCase() +
                                            "', '-', LPAD(CAST(RAND() * 1000000000 AS BIGINT), 10, '0'))"),

                            field().name("transaction_id").expression("#{Internet.uuid}"),

                            field().name("amount").type(DoubleType.instance()).min(10.0).max(10000.0),

                            field().name("payment_timestamp").type(TimestampType.instance())
                                    .min(java.sql.Date.valueOf("2024-01-01")),

                            field().name("payment_method")
                                    .oneOf("CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "PAYPAL"),

                            // Status - use SQL for weighting
                            field().name("status_random").type(DoubleType.instance()).min(0.0).max(1.0).omit(true),
                            field().name("status")
                                    .sql("""
                                            CASE
                                                WHEN status_random < 0.85 THEN 'SUCCESS'
                                                WHEN status_random < 0.95 THEN 'PENDING'
                                                ELSE 'FAILED'
                                            END
                                            """)
                    );

            return companyTask;
        }).collect(Collectors.toList());

        // Get the tail of the tasks list
        List<ConnectionTaskBuilder<?>> tasksTail = tasks.subList(1, tasks.size());
        execute(conf, tasks.get(0), tasksTail.toArray(new ConnectionTaskBuilder[0]));
    }
}

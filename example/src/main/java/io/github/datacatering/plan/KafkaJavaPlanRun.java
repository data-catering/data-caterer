package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder;
import io.github.datacatering.datacaterer.api.connection.KafkaBuilder;
import io.github.datacatering.datacaterer.api.model.ArrayType;
import io.github.datacatering.datacaterer.api.model.DateType;
import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.IntegerType;
import io.github.datacatering.datacaterer.api.model.TimestampType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.sql.Date;

public class KafkaJavaPlanRun extends PlanRun {
    {
        var kafkaTask = getKafkaTask();
        var conf = configuration().generatedReportsFolderPath("/opt/app/data/report");
        execute(conf, kafkaTask);
    }

    public ConnectionTaskBuilder<KafkaBuilder> getKafkaTask() {
        return kafka("my_kafka", "kafkaserver:29092")
                .topic("account-topic")
                .fields(
                        field().name("key").sql("body.account_id"),
                        //field().name("partition").type(IntegerType.instance()),   //can define message partition here
                        field().messageHeaders(
                                field().messageHeader("account-id", "body.account_id"),
                                field().messageHeader("updated", "body.details.updated_by-time")
                        )
                ).fields(
                        field().messageBody(
                                field().name("account_id").regex("ACC[0-9]{8}"),
                                field().name("year").type(IntegerType.instance()).min(2021).max(2023),
                                field().name("amount").type(DoubleType.instance()),
                                field().name("details")
                                        .fields(
                                                field().name("name").expression("#{Name.name}"),
                                                field().name("first_txn_date").type(DateType.instance()).sql("ELEMENT_AT(SORT_ARRAY(body.transactions.txn_date), 1)"),
                                                field().name("updated_by")
                                                        .fields(
                                                                field().name("user"),
                                                                field().name("time").type(TimestampType.instance())
                                                        )
                                        ),
                                field().name("transactions").type(ArrayType.instance())
                                        .fields(
                                                field().name("txn_date").type(DateType.instance()).min(Date.valueOf("2021-01-01")).max("2021-12-31"),
                                                field().name("amount").type(DoubleType.instance())
                                        )
                        )
                )
                .count(count().records(100));
    }
}

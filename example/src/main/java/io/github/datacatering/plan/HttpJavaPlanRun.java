package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.HttpMethodEnum;
import io.github.datacatering.datacaterer.api.model.Constants;
import io.github.datacatering.datacaterer.api.model.IntegerType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.List;
import java.util.Map;

public class HttpJavaPlanRun extends PlanRun {
    {
        var httpTask = http("my_http", Map.of(Constants.ROWS_PER_SECOND(), "1", Constants.VALIDATION_IDENTIFIER(), "POST/pets"))
                .fields(
                        field().httpHeader("Content-Type").staticValue("application/json"),
                        field().httpHeader("Content-Length"),
                        field().httpHeader("X-Account-Id").sql("body.account_id")
                )
                .fields(
                        field().httpUrl(
                                "http://host.docker.internal:80/anything/pets/{id}",    //url
                                HttpMethodEnum.GET(),                                       //method
                                List.of(field().name("id")),                                //path parameter
                                List.of(field().name("limit").type(IntegerType.instance()).min(1).max(10))  //query parameter
                        )
                )
                .fields(
                        field().httpBody(
                                field().name("account_id").regex("ACC[0-9]{8}"),
                                field().name("details").fields(
                                        field().name("name").expression("#{Name.name}"),
                                        field().name("age").type(IntegerType.instance()).max(100)
                                )
                        )
                )
                .validations(
                        validation().field("request.method").isEqual("POST"),
                        validation().field("response.statusCode").isEqual(200),
                        validation().field("response.timeTaken").lessThan(100),
                        validation().field("response.headers.Content-Length").greaterThan(0),
                        validation().field("response.headers.Content-Type").isEqual("application/json")
                )
                .count(count().records(2));

        var conf = configuration()
                .generatedReportsFolderPath("/opt/app/data/report");

        execute(conf, httpTask);
    }
}

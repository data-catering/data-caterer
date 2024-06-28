FROM amazoncorretto:22-alpine

USER root
RUN addgroup -S app \
    && adduser -S app -G app \
    && mkdir -p /opt/app /opt/DataCaterer/connection /opt/DataCaterer/plan /opt/DataCaterer/execution /opt/DataCaterer/report \
    && chown -R app:app /opt/app /opt/DataCaterer/connection /opt/DataCaterer/plan /opt/DataCaterer/execution /opt/DataCaterer/report \
    && apk add --no-cache bash
COPY --chown=app:app script app/src/main/resources app/build/libs /opt/app/

USER app
WORKDIR /opt/app
ENV APPLICATION_CONFIG_PATH=/opt/app/application.conf
EXPOSE 9898

ENTRYPOINT ["/opt/app/run-data-caterer.sh"]

FROM alpine:latest

WORKDIR /opt/
COPY ./dist/optimus/linux-amd64/optimus .
RUN chmod +x ./optimus

ARG PLUGIN_BQ
ARG PLUGIN_PREDATOR
ARG PLUGIN_TRANSPORTER
ARG PLUGIN_NEO

ADD ${PLUGIN_BQ} ./optimus-task-bq2bq_dev_linux_amd64
RUN chmod +x ./optimus-task-bq2bq_dev_linux_amd64
ADD ${PLUGIN_PREDATOR} ./optimus-hook-predator_dev_linux_amd64
RUN chmod +x ./optimus-hook-predator_dev_linux_amd64
ADD ${PLUGIN_TRANSPORTER} ./optimus-hook-transporter_dev_linux_amd64
RUN chmod +x ./optimus-hook-transporter_dev_linux_amd64
ADD ${PLUGIN_NEO} ./optimus-task-neo_dev_linux_amd64
RUN chmod +x ./optimus-task-neo_dev_linux_amd64


EXPOSE 8080
ENTRYPOINT ["/opt/optimus"]

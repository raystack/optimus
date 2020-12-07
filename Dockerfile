FROM alpine:latest
RUN ["apk", "update"]
RUN ["apk", "add", "libc6-compat"] 

WORKDIR /opt/
COPY ./dist/optimus/linux-amd64/optimus .
RUN chmod +x ./optimus

EXPOSE 8080
ENTRYPOINT ["/opt/optimus"]

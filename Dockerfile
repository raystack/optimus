FROM alpine:3.13

COPY optimus /usr/bin/optimus

COPY ./entrypoint_init_container.sh /opt/entrypoint_init_container.sh
RUN chmod +x /opt/entrypoint_init_container.sh

EXPOSE 8080
CMD ["optimus"]
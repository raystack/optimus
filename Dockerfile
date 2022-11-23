FROM alpine:3.13
ARG USER=optimus

COPY optimus /usr/bin/optimus
WORKDIR /app 

RUN adduser -D $USER 
RUN chown -R $USER:$USER /app

# use this part on airflow task to fetch and compile assets by optimus client
COPY ./entrypoint_init_container.sh /opt/entrypoint_init_container.sh
RUN chmod +x /opt/entrypoint_init_container.sh

USER $USER

EXPOSE 8080
CMD ["optimus"]
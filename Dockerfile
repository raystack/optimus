FROM alpine:3.13
ARG USER=optimus

COPY optimus /usr/bin/optimus
WORKDIR /app 

RUN adduser -D $USER 
RUN chown -R $USER:$USER /app

USER $USER
EXPOSE 8080
CMD ["optimus"]
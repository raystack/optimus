FROM alpine:3.13

COPY optimus /usr/bin/optimus

EXPOSE 8080
CMD ["optimus"]
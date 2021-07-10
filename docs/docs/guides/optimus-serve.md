---
id: optimus-serve
title: Starting Optimus Server
---

Once the optimus binary is installed, it can be started in serve mode using
```shell
optimus serve
```
It needs few [configurations](../getting-started/configuration.md) as prerequisites, create a `.optimus.yaml` file with
```yaml
version: 1
host: localhost:9100
serve:
  port: 9100
  host: localhost
  ingress_host: optimus.example.io:80
  app_key: 32charrandomhash32charrandomhash
  db:
    dsn: postgres://user:password@localhost:5432/optimus?sslmode=disable
```
You will need to change `dsn` and `app_key` according to your installation.

Once the server is up and running, before it is ready to deploy `jobs` we need to
- Register an optimus project
- Register a namespace under project
- Register required secrets under project

This needs to be done in order using REST/GRPC endpoints provided by the server.
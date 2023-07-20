# Managing Project & Namespace

Optimus provides a command to register a new project specified in the client configuration or update if it exists:
```shell
$ optimus project register
```

You are also allowed to register the namespace by using the with-namespaces flag:
```shell
$ optimus project register --with-namespaces
```

You can also check the project configuration that has been registered in your server using:
```shell
$ optimus project describe
```

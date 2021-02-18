
Starting Optimus server:

```bash
make && export $(cat development.env | xargs) && ./optimus
```


Instance Register Command:

```bash
OPTIMUS_ADMIN=1 ../../asgard/optimus-v2/opctl admin create instance pilotdata-integration.playground.characters  \
--host localhost:6666 --name transporter --project g-pilotdata-gl --scheduled-at "2021-01-14T02:00:00+00:00" \
--type hook --output-dir ./container
```

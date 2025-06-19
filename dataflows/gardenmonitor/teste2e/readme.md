## Isolated e2e tests

we initially test 2 data flows:

1) mqtt to [bigquery](pubsubbigquery)
2) mqtt to [icestore](pubsubicestore)

both follow a similar structure and are envisioned to be combined in a
real world microservice deployment 
# Cosmos Garbage Collector

## Running

```bash
go run main.go PATH_TO_APP_HOME # eg. ~/.terra
```

## Notes

```bash
# Data held in our 10TB archival full node:
du -sh ./*
5.6T    ./application.db
813G    ./blockstore.db
1022M   ./cs.wal
104K    ./evidence.db
4.0K    ./priv_validator_state.json
3.0G    ./snapshots
1.5T    ./state.db
3.3T    ./tx_index.db
4.0K    ./upgrade-info.json
```

- Latest block height must be read from each database (ie. cannot read from one db and assume it's the same for all)
- `state.db` must contain all data with the `validatorsKey:` prefix, else starting the daemon will result in consensus failure

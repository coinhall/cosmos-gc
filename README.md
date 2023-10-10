# Cosmos Garbage Collector

CLI tool to *really* prune a Cosmos SDK full node.

## Assumptions

- Target chain is running Cosmos SDK version v0.46 or v0.47 (does NOT work on v0.45)
- Full node uses LevelDB (support for other DBs can be added if need be)

## Installing

Using Go:

```bash
go install github.com/coinhall/cosmos-gc/cmd/cosmos-gc
```

OR, from source:

```bash
git clone https://github.com/coinhall/cosmos-gc
cd cosmos-gc
make install
```

## Running

Make sure to stop the full node first (if it is running). Then, run the following command:

```bash
cosmos-gc PATH_TO_APP_HOME # eg. ~/.terra
```

You may proceed to start the full node after the above command has finished.

## Implementation Notes

- A new db is created to replace the old db as opposed to pruning the old db as deletions are extremely slow
- Latest block height must be read from each database (ie. cannot read from one db and assume it's the same for all)
- `state.db` must contain `validatorsKey` for last valset changed height, latest height, latest height + 1, and latest height + 2; else, starting the daemon will result in consensus failure
- `application.db` holds the IAVL tree for Cosmos SDK, iteration itself is already very expensive (worse still for reads and writes)

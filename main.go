package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func pruneBlockstoreState(dataDir string) {
	db, err := leveldb.OpenFile(filepath.Join(dataDir, "blockstore.db"), nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Get latest height
	fmt.Printf("Finding latest block height...\n")
	prefix := []byte("H:")
	latestHeight := uint64(0)
	iter := db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		height, err := strconv.ParseUint(string(iter.Key())[len(prefix):], 10, 64)
		if err != nil {
			panic(err)
		}
		if height > latestHeight {
			latestHeight = height
		}
	}
	iter.Release()
	fmt.Printf("Latest block height [%v]\n", latestHeight)

	// Get blockhash of latest height
	fmt.Printf("Finding latest block hash...\n")
	value, err := db.Get([]byte("H:"+fmt.Sprint(latestHeight)), nil)
	if err != nil {
		panic(err)
	}
	protoBlockMeta := new(cmtproto.BlockMeta)
	if err = proto.Unmarshal(value, protoBlockMeta); err != nil {
		panic(err)
	}
	blockMeta, err := types.BlockMetaFromProto(protoBlockMeta)
	if err != nil {
		panic(err)
	}
	latestHash := strings.ToLower(blockMeta.BlockID.Hash.String())
	fmt.Printf("Latest block hash [%v]\n", latestHash)

	// Delete all keys except for the latest heights
	fmt.Printf("Deleting blockstore.db state till latest height and hash...\n")
	iter = db.NewIterator(nil, nil)
	maxHeightString := fmt.Sprint(latestHeight)
	batch := new(leveldb.Batch)
	for iter.Next() {
		key := string(iter.Key())
		if key != "H:"+maxHeightString &&
			key != "C:"+maxHeightString &&
			key != "P:"+maxHeightString+":0" &&
			key != "SC:"+maxHeightString &&
			key != "BH:"+latestHash &&
			key != "blockStore" {
			batch.Delete([]byte(key))
		}
	}
	iter.Release()
	opts := opt.WriteOptions{
		Sync: true,
	}
	if err := db.Write(batch, &opts); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully pruned blockstore.db till height [%v]\n", latestHeight)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: go run main.go <app_home>\n")
		os.Exit(1)
	}
	appHome := os.Args[1]
	dataDir := filepath.Join(appHome, "data")
	fmt.Printf("Using app data dir at [%v]\n", dataDir)

	pruneBlockstoreState(dataDir)
}

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
	"github.com/syndtr/goleveldb/leveldb/util"
)

func pruneBlockstoreState(dataDir string) {
	dbCurrent, err := leveldb.OpenFile(filepath.Join(dataDir, "blockstore.db"), nil)
	if err != nil {
		panic(err)
	}

	// Get latest height
	fmt.Printf("Finding latest block height...\n")
	prefix := []byte("H:")
	latestHeight := uint64(0)
	iter := dbCurrent.NewIterator(util.BytesPrefix(prefix), nil)
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
	value, err := dbCurrent.Get([]byte("H:"+fmt.Sprint(latestHeight)), nil)
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

	// Create new blockstore.db and populate latest height and hash
	fmt.Printf("Creating new db and adding latest info from old db...\n")
	if err := os.RemoveAll(filepath.Join(dataDir, "blockstore.new.db")); err != nil {
		panic(err)
	}
	dbNew, err := leveldb.OpenFile(filepath.Join(dataDir, "blockstore.new.db"), nil)
	if err != nil {
		panic(err)
	}
	var (
		hKey          []byte = []byte("H:" + fmt.Sprint(latestHeight))
		hVal          []byte
		cKey          []byte = []byte("C:" + fmt.Sprint(latestHeight-1))
		cVal          []byte
		pKey          []byte = []byte("P:" + fmt.Sprint(latestHeight) + ":0")
		pVal          []byte
		scKey         []byte = []byte("SC:" + fmt.Sprint(latestHeight))
		scVal         []byte
		bhKey         []byte = []byte("BH:" + latestHash)
		bhVal         []byte
		blockstoreKey []byte = []byte("blockStore")
		blockstoreVal []byte
	)
	hVal, err = dbCurrent.Get(hKey, nil)
	if err != nil {
		panic(err)
	}
	cVal, err = dbCurrent.Get(cKey, nil)
	if err != nil {
		panic(err)
	}
	pVal, err = dbCurrent.Get(pKey, nil)
	if err != nil {
		panic(err)
	}
	scVal, err = dbCurrent.Get(scKey, nil)
	if err != nil {
		panic(err)
	}
	bhVal, err = dbCurrent.Get(bhKey, nil)
	if err != nil {
		panic(err)
	}
	blockstoreVal, err = dbCurrent.Get(blockstoreKey, nil)
	if err != nil {
		panic(err)
	}
	batch := new(leveldb.Batch)
	batch.Put(hKey, hVal)
	batch.Put(cKey, cVal)
	batch.Put(pKey, pVal)
	batch.Put(scKey, scVal)
	batch.Put(bhKey, bhVal)
	batch.Put(blockstoreKey, blockstoreVal)
	if err := dbNew.Write(batch, nil); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully added latest info to new blockstore.db\n")

	// Remove old db and rename new db
	fmt.Printf("Removing old db and renaming new db...\n")
	dbNew.Close()
	dbCurrent.Close()
	if err := os.RemoveAll(filepath.Join(dataDir, "blockstore.db")); err != nil {
		panic(err)
	}
	if err := os.Rename(filepath.Join(dataDir, "blockstore.new.db"), filepath.Join(dataDir, "blockstore.db")); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully pruned blockstore.db!\n")
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

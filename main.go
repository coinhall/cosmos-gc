package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	cosmosdb "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/libs/log"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	storeiavl "github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/store/rootmulti"
	sdk "github.com/cosmos/cosmos-sdk/store/types"
	"github.com/cosmos/gogoproto/proto"
	gogotypes "github.com/cosmos/gogoproto/types"
	"github.com/cosmos/iavl"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func PruneBlockstoreDB(dataDir string) {
	fmt.Printf("=== Pruning blockstore.db ===\n")

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
	if err := dbCurrent.Close(); err != nil {
		panic(err)
	}
	if err := dbNew.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "blockstore.db")); err != nil {
		panic(err)
	}
	if err := os.Rename(filepath.Join(dataDir, "blockstore.new.db"), filepath.Join(dataDir, "blockstore.db")); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully pruned blockstore.db!\n\n")
}

func PruneStateDB(dataDir string) {
	fmt.Printf("=== Pruning state.db ===\n")

	dbCurrent, err := leveldb.OpenFile(filepath.Join(dataDir, "state.db"), nil)
	if err != nil {
		panic(err)
	}

	// Get latest height
	fmt.Printf("Finding latest block height...\n")
	prefix := []byte("validatorsKey:")
	lowestHeight := uint64(math.MaxUint64)
	latestHeight := uint64(0)
	iter := dbCurrent.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		height, err := strconv.ParseUint(string(iter.Key())[len(prefix):], 10, 64)
		if err != nil {
			panic(err)
		}
		if height < lowestHeight {
			lowestHeight = height
		}
		if height > latestHeight {
			latestHeight = height
		}
	}
	iter.Release()
	fmt.Printf("Latest block height [%v]\n", latestHeight)

	// Create new db and populate latest info
	fmt.Printf("Creating new db and adding latest info from old db...\n")
	if err := os.RemoveAll(filepath.Join(dataDir, "state.new.db")); err != nil {
		panic(err)
	}
	dbNew, err := leveldb.OpenFile(filepath.Join(dataDir, "state.new.db"), nil)
	if err != nil {
		panic(err)
	}
	var (
		abciResponsesKey    []byte = []byte("abciResponsesKey:" + fmt.Sprint(latestHeight-2))
		abciResponsesVal    []byte
		consensusParamsKey  []byte = []byte("consensusParamsKey:" + fmt.Sprint(latestHeight-1))
		consensusParamsVal  []byte
		genesisDocKey       []byte = []byte("genesisDoc")
		genesisDocVal       []byte
		lastABCIResponseKey []byte = []byte("lastABCIResponseKey")
		lastABCIResponseVal []byte
		stateKey            []byte = []byte("stateKey")
		stateVal            []byte
	)
	abciResponsesVal, err = dbCurrent.Get(abciResponsesKey, nil)
	if err != nil {
		panic(err)
	}
	consensusParamsVal, err = dbCurrent.Get(consensusParamsKey, nil)
	if err != nil {
		panic(err)
	}
	genesisDocVal, err = dbCurrent.Get(genesisDocKey, nil)
	if err != nil {
		panic(err)
	}
	lastABCIResponseVal, err = dbCurrent.Get(lastABCIResponseKey, nil)
	if err != nil {
		panic(err)
	}
	stateVal, err = dbCurrent.Get(stateKey, nil)
	if err != nil {
		panic(err)
	}
	// ! we need to get the first height of a hard fork
	// ! the first ever key found in the db might be wrong
	firstValidatorsVal, err := dbCurrent.Get([]byte("validatorsKey:"+fmt.Sprint(lowestHeight)), nil)
	if err != nil {
		panic(err)
	}
	batch := new(leveldb.Batch)
	batch.Put(abciResponsesKey, abciResponsesVal)
	batch.Put(consensusParamsKey, consensusParamsVal)
	batch.Put(genesisDocKey, genesisDocVal)
	batch.Put(lastABCIResponseKey, lastABCIResponseVal)
	batch.Put(stateKey, stateVal)
	batch.Put([]byte("validatorsKey:"+fmt.Sprint(lowestHeight)), firstValidatorsVal)
	for i := 0; i <= 2; i++ {
		key := []byte("validatorsKey:" + fmt.Sprint(latestHeight-uint64(i)))
		val, err := dbCurrent.Get(key, nil)
		if err != nil {
			panic(err)
		}
		batch.Put(key, val)
	}
	if err := dbNew.Write(batch, nil); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully added latest info to new state.db\n")

	// Remove old db and rename new db
	if err := dbCurrent.Close(); err != nil {
		panic(err)
	}
	if err := dbNew.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "state.db")); err != nil {
		panic(err)
	}
	if err := os.Rename(filepath.Join(dataDir, "state.new.db"), filepath.Join(dataDir, "state.db")); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully pruned state.db!\n\n")
}

func PruneApplicationDB(dataDir string) {
	fmt.Printf("=== Pruning application.db ===\n")

	// Get all iavl keys by iterating through the whole db
	fmt.Printf("Finding all module keys...\n")
	ldb, err := leveldb.OpenFile(filepath.Join(dataDir, "application.db"), nil)
	if err != nil {
		panic(err)
	}
	iter := ldb.NewIterator(nil, nil)
	set := make(map[string]struct{})
	// TODO: can be alot more efficient instead of looping through everything
	for iter.Next() {
		key := string(iter.Key())
		if strings.HasPrefix(key, "s/k:") {
			key = strings.Replace(key, "s/k:", "", 1)
			split := strings.Split(key, "/")
			key = split[0]
			set[key] = struct{}{}
		}
	}
	keys := []string{}
	for key := range set {
		keys = append(keys, key)
	}
	iter.Release()
	ldb.Close()
	fmt.Printf("Found [%v] module keys\n", len(keys))

	// Get latest height
	fmt.Printf("Finding latest block height...\n")
	cdbOld, err := cosmosdb.NewGoLevelDB("application", dataDir)
	if err != nil {
		panic(err)
	}
	val, err := cdbOld.Get([]byte("s/latest"))
	if err != nil {
		panic(err)
	}
	latestHeight := int64(0)
	if val != nil {
		if err := gogotypes.StdInt64Unmarshal(&latestHeight, val); err != nil {
			panic(err)
		}
	}
	fmt.Printf("Latest block height [%v]\n", latestHeight)

	// Create new db and import latest version from old db
	fmt.Printf("Creating new db and adding latest info from old db...\n")
	if err := os.RemoveAll(filepath.Join(dataDir, "application.new.db")); err != nil {
		panic(err)
	}
	cdbNew, err := cosmosdb.NewGoLevelDB("application.new", dataDir)
	if err != nil {
		panic(err)
	}

	storeOld := rootmulti.NewStore(cdbOld, log.NewNopLogger())
	storeNew := rootmulti.NewStore(cdbNew, log.NewNopLogger())
	if err != nil {
		panic(err)
	}
	storeKeys := []*sdk.KVStoreKey{}
	for _, key := range keys {
		storeKeys = append(storeKeys, sdk.NewKVStoreKey(key))
	}
	for _, storeKey := range storeKeys {
		storeOld.MountStoreWithDB(storeKey, sdk.StoreTypeIAVL, nil)
	}
	if err := storeOld.LoadLatestVersion(); err != nil {
		panic(err)
	}
	for _, storeKey := range storeKeys {
		storeNew.MountStoreWithDB(storeKey, sdk.StoreTypeIAVL, nil)
	}
	if err := storeNew.LoadLatestVersion(); err != nil {
		panic(err)
	}
	for _, storeKey := range storeKeys {
		fmt.Printf("  Restoring module [%v]...\n", string(storeKey.Name()))
		kvStoreOld := storeOld.GetCommitKVStore(storeKey)
		exp, err := kvStoreOld.(*storeiavl.Store).Export(latestHeight)
		if err != nil {
			// error will be thrown when tree root is null
			// we should set it back to null in the new db
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(latestHeight))
			key := append([]byte("s/k:"+storeKey.Name()+"/r"), b...)
			if err := cdbNew.SetSync(key, []byte{}); err != nil {
				panic(err)
			}
			continue
		}
		kvStoreNew := storeNew.GetCommitKVStore(storeKey)
		inp, err := kvStoreNew.(*storeiavl.Store).Import(latestHeight)
		if err != nil {
			panic(err)
		}
		for {
			node, err := exp.Next()
			if err == iavl.ErrorExportDone {
				break
			}
			if err := inp.Add(node); err != nil {
				panic(err)
			}
		}
		if err := inp.Commit(); err != nil {
			panic(err)
		}
	}

	// Copy latest height and commit info
	cdbNew.SetSync([]byte("s/latest"), val)
	for i := 1; i <= int(latestHeight); i++ {
		commitInfo, _ := cdbOld.Get([]byte("s/" + fmt.Sprint(i)))
		cdbNew.SetSync([]byte("s/"+fmt.Sprint(i)), commitInfo)
	}
	fmt.Printf("Successfully added latest info to new application.db\n")

	// Remove old db and rename new db
	fmt.Printf("Removing old db and renaming new db...\n")
	if err := cdbOld.Close(); err != nil {
		panic(err)
	}
	if err := cdbNew.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "application.db")); err != nil {
		panic(err)
	}
	if err := os.Rename(filepath.Join(dataDir, "application.new.db"), filepath.Join(dataDir, "application.db")); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully pruned application.db!\n\n")
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: go run main.go <app_home>\n")
		os.Exit(1)
	}
	appHome := os.Args[1]
	dataDir := filepath.Join(appHome, "data")
	fmt.Printf("Using app data dir at [%v]\n\n", dataDir)

	PruneBlockstoreDB(dataDir)
	PruneStateDB(dataDir)
	PruneApplicationDB(dataDir)
}

package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cosmosdb "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	storeiavl "github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/store/rootmulti"
	sdk "github.com/cosmos/cosmos-sdk/store/types"
	gogotypes "github.com/cosmos/gogoproto/types"
	"github.com/cosmos/iavl"
	"github.com/syndtr/goleveldb/leveldb"
)

func PruneBlockstoreDB(dataDir string) {
	fmt.Printf("=== Pruning blockstore.db ===\n")

	dbOld, err := cosmosdb.NewGoLevelDB("blockstore", dataDir)
	if err != nil {
		panic(err)
	}

	// Get latest height
	fmt.Printf("Finding latest block height...\n")
	blockStore := store.NewBlockStore(dbOld)
	latestHeight := blockStore.Height()
	fmt.Printf("Latest block height [%v]\n", latestHeight)

	// Get blockhash of latest height
	fmt.Printf("Finding latest block hash...\n")
	meta := blockStore.LoadBlockMeta(latestHeight)
	latestHash := strings.ToLower(meta.BlockID.Hash.String())
	fmt.Printf("Latest block hash [%v]\n", latestHash)

	// Create new db and populate latest info
	fmt.Printf("Creating new db and adding latest info from old db...\n")
	if err := os.RemoveAll(filepath.Join(dataDir, "blockstore.new.db")); err != nil {
		panic(err)
	}
	dbNew, err := cosmosdb.NewGoLevelDB("blockstore.new", dataDir)
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
	hVal, err = dbOld.Get(hKey)
	if err != nil {
		panic(err)
	}
	cVal, err = dbOld.Get(cKey)
	if err != nil {
		panic(err)
	}
	pVal, err = dbOld.Get(pKey)
	if err != nil {
		panic(err)
	}
	scVal, err = dbOld.Get(scKey)
	if err != nil {
		panic(err)
	}
	bhVal, err = dbOld.Get(bhKey)
	if err != nil {
		panic(err)
	}
	blockstoreVal, err = dbOld.Get(blockstoreKey)
	if err != nil {
		panic(err)
	}
	batch := dbNew.NewBatch()
	batch.Set(hKey, hVal)
	batch.Set(cKey, cVal)
	batch.Set(pKey, pVal)
	batch.Set(scKey, scVal)
	batch.Set(bhKey, bhVal)
	batch.Set(blockstoreKey, blockstoreVal)
	if err := batch.WriteSync(); err != nil {
		panic(err)
	}
	if err := batch.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully added latest info to new db\n")

	// Remove old db and rename new db
	fmt.Printf("Removing old db and renaming new db...\n")
	if err := dbOld.Close(); err != nil {
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

	dbOld, err := cosmosdb.NewGoLevelDB("state", dataDir)
	if err != nil {
		panic(err)
	}

	// Get latest height
	fmt.Printf("Finding latest and initial block heights...\n")
	storeOld := state.NewStore(dbOld, state.StoreOptions{})
	storeStateOld, err := storeOld.Load()
	if err != nil {
		panic(err)
	}
	initialheight := storeStateOld.InitialHeight
	latestHeight := storeStateOld.LastBlockHeight
	fmt.Printf("Initial block height [%v]\n", initialheight)
	fmt.Printf("Latest block height [%v]\n", latestHeight)

	// Create new db and populate latest info
	fmt.Printf("Creating new db and adding latest info from old db...\n")
	if err := os.RemoveAll(filepath.Join(dataDir, "state.new.db")); err != nil {
		panic(err)
	}
	dbNew, err := cosmosdb.NewGoLevelDB("state.new", dataDir)
	if err != nil {
		panic(err)
	}
	storeNew := state.NewStore(dbNew, state.StoreOptions{})
	if err := storeNew.Save(storeStateOld); err != nil {
		panic(err)
	}
	var (
		validatorsKey0 []byte = []byte("validatorsKey:" + fmt.Sprint(initialheight))
		validatorsVal0 []byte
		validatorsKey1 []byte = []byte("validatorsKey:" + fmt.Sprint(latestHeight))
		validatorsVal1 []byte
		validatorsKey2 []byte = []byte("validatorsKey:" + fmt.Sprint(latestHeight+1))
		validatorsVal2 []byte
	)
	validatorsVal0, err = dbOld.Get(validatorsKey0)
	if err != nil {
		panic(err)
	}
	validatorsVal1, err = dbOld.Get(validatorsKey1)
	if err != nil {
		panic(err)
	}
	validatorsVal2, err = dbOld.Get(validatorsKey2)
	if err != nil {
		panic(err)
	}
	batch := dbNew.NewBatch()
	batch.Set(validatorsKey0, validatorsVal0)
	batch.Set(validatorsKey1, validatorsVal1)
	batch.Set(validatorsKey2, validatorsVal2)
	if err := batch.WriteSync(); err != nil {
		panic(err)
	}
	if err := batch.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully added latest info to new db\n")

	// Remove old db and rename new db
	fmt.Printf("Removing old db and renaming new db...\n")
	if err := dbOld.Close(); err != nil {
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

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
	"github.com/cosmos/iavl"
)

func PruneBlockstoreDB(dataDir string) {
	fmt.Printf("\n========= Pruning blockstore.db =========\n")

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
	fmt.Printf("Successfully pruned blockstore.db!\n")
}

func PruneStateDB(dataDir string) {
	fmt.Printf("\n========= Pruning state.db =========\n")

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
	fmt.Printf("Successfully pruned state.db!\n")
}

func PruneApplicationDB(dataDir string) {
	fmt.Printf("\n========= Pruning application.db =========\n")

	cdbOld, err := cosmosdb.NewGoLevelDB("application", dataDir)
	if err != nil {
		panic(err)
	}

	// Get latest height
	fmt.Printf("Finding latest block height...\n")
	latestHeight := rootmulti.GetLatestVersion(cdbOld)
	fmt.Printf("Latest block height [%v]\n", latestHeight)

	// Get all module keys
	fmt.Printf("Finding all module keys...\n")
	storeOld := rootmulti.NewStore(cdbOld, log.NewNopLogger())
	commitInfo, err := storeOld.GetCommitInfo(latestHeight)
	if err != nil {
		panic(err)
	}
	storeKeys := make([]*sdk.KVStoreKey, len(commitInfo.StoreInfos))
	for i, info := range commitInfo.StoreInfos {
		storeKeys[i] = sdk.NewKVStoreKey(info.Name)
	}
	fmt.Printf("Found [%v] module keys\n", len(storeKeys))

	// Initialise old store
	fmt.Printf("Initialising old store with module keys...\n")
	for _, storeKey := range storeKeys {
		storeOld.MountStoreWithDB(storeKey, sdk.StoreTypeIAVL, nil)
	}
	if err := storeOld.LoadLatestVersion(); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully initialised old store...\n")

	// Create new db and import latest version from old db
	fmt.Printf("Creating new db and adding latest info from old db...\n")
	if err := os.RemoveAll(filepath.Join(dataDir, "application.new.db")); err != nil {
		panic(err)
	}
	cdbNew, err := cosmosdb.NewGoLevelDB("application.new", dataDir)
	if err != nil {
		panic(err)
	}
	storeNew := rootmulti.NewStore(cdbNew, log.NewNopLogger())
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
			// error will be thrown when tree root is null for a given height
			// we should set it back to empty bytes in the new store
			binaryHeight := make([]byte, 8)
			binary.BigEndian.PutUint64(binaryHeight, uint64(latestHeight))
			key := append([]byte("s/k:"+storeKey.Name()+"/r"), binaryHeight...)
			if err := cdbNew.Set(key, []byte{}); err != nil {
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
	val, err := cdbOld.Get([]byte("s/latest"))
	if err != nil {
		panic(err)
	}
	cdbNew.SetSync([]byte("s/latest"), val)
	val, err = commitInfo.Marshal()
	if err != nil {
		panic(err)
	}
	cdbNew.SetSync([]byte("s/"+fmt.Sprint(latestHeight)), val)
	fmt.Printf("Successfully added latest info to new db\n")

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
	fmt.Printf("Successfully pruned application.db!\n")
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: go run main.go <app_home>\n")
		os.Exit(1)
	}
	appHome := os.Args[1]
	dataDir := filepath.Join(appHome, "data")
	fmt.Printf("Using app data dir at [%v]\n", dataDir)

	PruneBlockstoreDB(dataDir)
	PruneStateDB(dataDir)
	PruneApplicationDB(dataDir)
}

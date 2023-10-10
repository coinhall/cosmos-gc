package pruner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cdb "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/store"
)

func PruneBlockstoreDB(dataDir string) error {
	// Open old db (if it exists)
	if _, err := os.Stat(filepath.Join(dataDir, "blockstore.db")); os.IsNotExist(err) {
		return fmt.Errorf("blockstore.db does not exist in %s", dataDir)
	}
	dbOld, err := cdb.NewGoLevelDB("blockstore", dataDir)
	if err != nil {
		return err
	}

	// Get latest height
	blockStore := store.NewBlockStore(dbOld)
	latestHeight := blockStore.Height()

	// Get blockhash of latest height
	meta := blockStore.LoadBlockMeta(latestHeight)
	latestHash := strings.ToLower(meta.BlockID.Hash.String())

	// Create new db and populate latest info
	if err := os.RemoveAll(filepath.Join(dataDir, "blockstore.new.db")); err != nil {
		return err
	}
	dbNew, err := cdb.NewGoLevelDB("blockstore.new", dataDir)
	if err != nil {
		return err
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
		return err
	}
	cVal, err = dbOld.Get(cKey)
	if err != nil {
		return err
	}
	pVal, err = dbOld.Get(pKey)
	if err != nil {
		return err
	}
	scVal, err = dbOld.Get(scKey)
	if err != nil {
		return err
	}
	bhVal, err = dbOld.Get(bhKey)
	if err != nil {
		return err
	}
	blockstoreVal, err = dbOld.Get(blockstoreKey)
	if err != nil {
		return err
	}
	batch := dbNew.NewBatch()
	batch.Set(hKey, hVal)
	batch.Set(cKey, cVal)
	batch.Set(pKey, pVal)
	batch.Set(scKey, scVal)
	batch.Set(bhKey, bhVal)
	batch.Set(blockstoreKey, blockstoreVal)
	if err := batch.WriteSync(); err != nil {
		return err
	}
	if err := batch.Close(); err != nil {
		return err
	}

	// Remove old db and rename new db
	if err := dbOld.Close(); err != nil {
		return err
	}
	if err := dbNew.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(dataDir, "blockstore.db")); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(dataDir, "blockstore.new.db"), filepath.Join(dataDir, "blockstore.db")); err != nil {
		return err
	}

	return nil
}

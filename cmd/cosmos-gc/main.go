package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/coinhall/cosmos-gc/pkg/pruner"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: cosmos-gc <app_home>\n")
		os.Exit(1)
	}
	appHome := os.Args[1]
	dataDir := filepath.Join(appHome, "data")
	fmt.Printf("Using app data dir at [%v]\n", dataDir)

	wg := sync.WaitGroup{}
	wg.Add(3)

	go func() {
		defer wg.Done()
		fmt.Println("[blockstore] starting to prune...")
		if err := pruner.PruneBlockstoreDB(dataDir); err != nil {
			fmt.Printf("[blockstore] pruning failed: %v\n", err)
			return
		}
		fmt.Println("[blockstore] pruned successfully!")
	}()

	go func() {
		defer wg.Done()
		fmt.Println("[state] starting to prune...")
		if err := pruner.PruneStateDB(dataDir); err != nil {
			fmt.Printf("[state] pruning failed: %v\n", err)
			return
		}
		fmt.Println("[state] pruned successfully!")
	}()

	go func() {
		defer wg.Done()
		fmt.Println("[application] starting to prune...")
		fmt.Println("[application] WARNING: THIS IS KNOWN TO TAKE A VERY LONG TIME!")
		if err := pruner.PruneApplicationDB(dataDir); err != nil {
			fmt.Printf("[application] pruning failed: %v\n", err)
			return
		}
		fmt.Println("[application] pruned successfully!")
	}()

	wg.Wait()
}

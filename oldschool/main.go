package main

import "flag"
import "log"
import "time"
import "strings"
import "github.com/coreos/go-etcd/etcd"
import "github.com/tobz/oldschool"

var (
	executorName = flag.String("name", "", "the name of this agent (must be unique)")
	joinNode     = flag.String("join-node", "127.0.0.1:4001", "the address of one of the etcd nodes in the cluster")
	baseDir      = flag.String("base-dir", "", "the base directory to synchronize files within")
	baseEtcdDir  = flag.String("base-etcd-dir", "", "the base directory to watch in etcd")
)

func init() {
	flag.Parse()

	if *executorName == "" {
		log.Fatal("[main] You must specify a name for this agent and it must be unique.  If you try and pass in a non-unique name, we'll find out, and simply exit.  Make your move.")
	}

	if *baseDir == "" || *baseEtcdDir == "" {
		log.Fatal("[main] You must specify both a base etcd directory to synchronize and the base directory on the filesystem to synchronize it to!")
	}
}

func main() {
	// Set etcd to debug mode for all da info.
	etcd.OpenDebug()

	// Create our etcd client and make sure we can sync to the cluster.
	etcdClient := etcd.NewClient(strings.Split(*joinNode, ","))
	if !etcdClient.SyncCluster() {
		log.Fatal("[main] Couldn't synchronize with the etcd cluster! (Is it down? Is your join node accurate?)")
	}

	log.Printf("[main] etcd cluster appears operational, found these nodes: %s", strings.Join(etcdClient.GetCluster(), ", "))

	// Now create our change executor.  This guy keeps track of and waits for changes, and then executes them
	// by turning the key into a file path, mirroring the keys on disk.
	executor := oldschool.NewExecutor(*executorName, etcdClient, *baseDir, *baseEtcdDir)
	go func() {
		log.Fatal(executor.Run())
	}()

	// Spin around and spit out some statistics and such.
	statsTick := time.Tick(time.Second * 2)
	for _ = range statsTick {
		log.Printf("[main] dc: %d, fw: %d, sets: %d/%d, deletes: %d/%d", executor.Statistics.DirectoriesCreated, executor.Statistics.FilesWritten,
			executor.Statistics.SetsProcessed, executor.Statistics.SetsReceived, executor.Statistics.DeletesProcessed, executor.Statistics.DeletesReceived)
	}
}

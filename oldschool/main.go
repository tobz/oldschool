package main

import "os"
import "flag"
import "log"
import "strings"
import "os/signal"
import "github.com/coreos/go-etcd/etcd"
import "github.com/tobz/oldschool"

var (
	joinNode    = flag.String("join-node", "127.0.0.1:4001", "the address of one of the etcd nodes in the cluster")
	baseDir     = flag.String("base-dir", "", "the base directory to synchronize files within")
	baseEtcdDir = flag.String("base-etcd-dir", "", "the base directory to watch in etcd")
)

func init() {
	flag.Parse()

	if *baseDir == "" || *baseEtcdDir == "" {
		log.Fatal("You must specify both a base etcd directory to synchronize and the base directory on the filesystem to synchronize it to!")
	}
}

func main() {
	// Create our etcd client and make sure we can sync to the cluster.
	etcdClient := etcd.NewClient(strings.Split(*joinNode, ","))
	if !etcdClient.SyncCluster() {
		log.Fatal("Couldn't synchronize with the etcd cluster! (Is it down? Is your join node accurate?)")
	}

	// Now create our change executor.  This guy keeps track of and waits for changes, and then executes them
	// by turning the key into a file path, mirroring the keys on disk.
	executor := oldschool.NewExecutor(etcdClient, *baseDir, *baseEtcdDir)
	executor.Start()

	// Wait to be closed or until we close ourselves.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for {
		select {
		case <-c:
			os.Exit(1)
		}
	}
}

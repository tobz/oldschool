package oldschool

import "github.com/coreos/go-etcd/etcd"

type Executor struct {
	etcdClient  *etcd.Client
	baseDir     string
	baseEtcdDir string
}

func NewExecutor(etcdClient *etcd.Client, baseDir string, baseEtcdDir string) *Executor {
	return &Executor{
		etcdClient:  etcdClient,
		baseDir:     baseDir,
		baseEtcdDir: baseEtcdDir,
	}
}

func (me *Executor) Run() error {
	// Figure out if we've done any work in the past, and if we can pick it back up.
	lastProcessedIndex := me.getLastProcessedIndex()
	if lastProcessedIndex != 0 {
		// See if our last processed index is still in etcd's history.
		presentInHistory := me.checkHistoryForIndex(lastProcessedIndex)
		if presentInHistory {
			// We should still be able to catch up, so start from our forward position.
			return me.startFromLastIndex()
		}
	}

	// Either we've never done anything or we're too far out of sync, so start from scratch.
	return me.startFromScratch()
}

func (me *Executor) startFromScratch() error {
	// Set up our channels and guards and what not.
	events := make(chan *etcd.Response, 128)
	errors := make(chan error, 1)
	startProcessing := false

	// First, set up a routine that handles our watcher.
	go func() {
		_, err := me.etcdClient.Watch(me.baseEtcdDir, 0, true, events, nil)
		if err != nil {
			errors <- err
		}
	}()

	// Now set up a routine that processes events once we've bulk loaded.
	go func() {
		// Block until we're allowed to process.
		for {
			if startProcessing {
				break
			}
		}

		// Now listen for and process all events that we get.
		for {
			select {
			case event := <-events:
				err := me.processEvent(event)
				if err != nil {
					errors <- err
					break
				}
			}
		}
	}()

	return nil
}

func (me *Executor) startFromLastIndex() error {
	return nil
}

func (me *Executor) processEvent(event *etcd.Response) error {
	return nil
}

func (me *Executor) checkHistoryForIndex(index uint64) bool {
	return false
}

func (me *Executor) getLastProcessedIndex() uint64 {
	return 0
}

func (me *Executor) setLastProcessedIndex(index uint64) {
}

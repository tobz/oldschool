package oldschool

import "github.com/coreos/go-etcd/etcd"
import "strconv"
import "log"

const (
	EtcdErrEventIndexCleared = 401
	EtcdErrKeyNotFound       = 100
)

type Executor struct {
	executorName string
	etcdClient   *etcd.Client
	baseDir      string
	baseEtcdDir  string
}

func NewExecutor(executorName string, etcdClient *etcd.Client, baseDir string, baseEtcdDir string) *Executor {
	return &Executor{
		executorName: executorName,
		etcdClient:   etcdClient,
		baseDir:      baseDir,
		baseEtcdDir:  baseEtcdDir,
	}
}

func (me *Executor) Run() error {
	// Figure out if we've done any work in the past, and if we can pick it back up.
	lastProcessedIndex, err := me.getLastProcessedIndex()
	if err != nil {
		return err
	}

	if lastProcessedIndex != 0 {
		log.Printf("[runner] Found indicator of previous work: last processed index set as '%d'", lastProcessedIndex)

		// See if our last processed index is still in etcd's history.
		presentInHistory := me.checkHistoryForIndex(lastProcessedIndex)
		if presentInHistory {
			log.Print("[runner] Found last processed index still in etcd history.  Trying to start from last index...")

			// We should still be able to catch up, so start from our forward position.
			return me.startFromLastIndex()
		}
	}

	log.Print("[runner] No previous indicator of work found.  Starting this node from scratch...")

	// Either we've never done anything or we're too far out of sync, so start from scratch.
	return me.startFromScratch()
}

func (me *Executor) startFromScratch() error {
	// Set up our channels and guards and what not.
	events := make(chan *etcd.Response, 128)
	errors := make(chan error, 1)

	// First, set up a routine that handles our watcher.
	go func() {
		log.Print("[watcher] Starting watch for latest changes...")

		_, err := me.etcdClient.Watch(me.baseEtcdDir, 0, true, events, nil)
		if err != nil {
			log.Printf("[watcher] Caught error while watching: %s", err)

			errors <- err
		}
	}()

	// Now set up our routine that processes events as we receive them.
	eventProcessor := func() {
		log.Print("[processor] Starting to process event backlog...")

		// Now listen for and process all events that we get.
		for {
			select {
			case event := <-events:
				log.Printf("[processor] Received event: %s -> %s (%d / %d / %d)",
					event.Action, event.Node.Key, event.EtcdIndex, event.RaftIndex, event.RaftTerm)

				err := me.processEvent(event)
				if err != nil {
					log.Printf("[processor] Caught error while processing event: %s", err)

					errors <- err
					break
				}
			}
		}
	}

	log.Print("[bootstrap] Starting bulk load of universe from etcd...")

	// Now we need to bulk load the data and process it.
	response, err := me.etcdClient.Get(me.baseEtcdDir, false, true)
	if err != nil {
		log.Printf("[bootstrap] Caught error while trying to get the universe: %s", err)
		return err
	}

	err = me.processEvent(response)
	if err != nil {
		log.Printf("[bootstrap] Caught an error while trying to process our bootstrap get: %s", err)
		return err
	}

	log.Print("[bootstrap] Starting processor...")

	go eventProcessor()

	// Now just watch for any errors that our routines throw, and spit them back up the chain if we get one.
	log.Print("[bootstrap] Spinning while we wait for any potential error.  We are now operational and in cruise control mode.")

	for {
		select {
		case err = <-errors:
			log.Printf("[bootstrap] Caught an error from worker routine: %s", err)
			return err
		}
	}

	return nil
}

func (me *Executor) startFromLastIndex() error {
	log.Print("[warm restart] Not implemented.")

	return nil
}

func (me *Executor) processEvent(event *etcd.Response) error {
	log.Printf("[handler] Processing event: %s -> %s (%d / %d / %d)",
		event.Action, event.Node.Key, event.EtcdIndex, event.RaftIndex, event.RaftTerm)

    // Handle the event according to action.
    switch event.Action {
    case "get":
        return me.handleGet(event)
    case "set":
        return me.handleSet(event)
    case "delete":
        return me.handleDelete(event)
    }

	return nil
}

func (me *Executor) handleGet(event *etcd.Response) error {
    return nil
}

func (me *Executor) handleSet(event *etcd.Response) error {
    return nil
}

func (me *Executor) handleDelete(event *etcd.Response) error {
    return nil
}

func (me *Executor) checkHistoryForIndex(index uint64) bool {
	_, err := me.etcdClient.Watch(me.baseEtcdDir, index, true, nil, nil)
	return errorIsEtcdError(err, EtcdErrEventIndexCleared)
}

func (me *Executor) getLastProcessedIndex() (uint64, error) {
	// Try and grab whatever we may have put there last.
	response, err := me.etcdClient.Get(me.baseEtcdDir+"/_executors/_"+me.executorName, false, false)
	if err != nil {
		// If we simply couldn't find the key - that's not an issue, per se.  We only care about
		// failures of the cluster or something.
		if errorIsEtcdError(err, EtcdErrKeyNotFound) {
			return 0, nil
		}

		// Couldn't find anything.  Started from the bottom, now we here.
		return 0, err
	}

	// This is the simplest way to convert a string to a uint64.  Ugh.
	index, err := strconv.ParseUint(response.Node.Value, 10, 64)
	if err != nil {
		// I like clearly defined error handling situations, but fuck it.  I'm returning a
		// zero if something stupid happened and we set this to a non-numeric value.
		return 0, err
	}

	return index, nil
}

func (me *Executor) setLastProcessedIndex(index uint64) error {
	_, err := me.etcdClient.Set(me.baseEtcdDir+"/_executors/_"+me.executorName, strconv.FormatUint(index, 10), 0)
	return err
}

func errorIsEtcdError(err error, errCode int) bool {
	if err == nil {
		return false
	}

	// If the error code we're checking for is -1, we're just trying to match an EtcdError, but not a specific one.
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && (etcdErr.ErrorCode == errCode || errCode == -1)
}

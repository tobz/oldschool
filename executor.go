package oldschool

import "github.com/coreos/go-etcd/etcd"
import "github.com/rcrowley/go-metrics"
import "path/filepath"
import "strconv"
import "strings"
import "time"
import "fmt"
import "log"
import "os"

const (
	etcdErrEventIndexCleared = 401
	etcdErrKeyNotFound       = 100
	defaultDirectoryMode     = os.ModeDir | os.ModeSetgid | 0775
	defaultFileMode          = 0644
	defaultFileFlags         = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
)

// Executor is the main encapsulation of business logic.  It handles bootstrapping, watching for changes and executing all
// of those changes.  It handles metric collection, for things like sets or deletes processed, etc, as well.
type Executor struct {
	executorName    string
	etcdClient      *etcd.Client
	baseDir         string
	baseEtcdDir     string
	currentIndex    uint64
	metricsRegistry metrics.Registry
	stats           *executorStatistics
}

type executorStatistics struct {
	DirectoriesCreated metrics.Counter
	DirectoriesDeleted metrics.Counter
	FilesWritten       metrics.Counter
	FilesDeleted       metrics.Counter
	SetsReceived       metrics.Counter
	SetsProcessed      metrics.Counter
	DeletesReceived    metrics.Counter
	DeletesProcessed   metrics.Counter
}

// NewExecutor returns a properly initialized executor object, with metrics defined and registered.
func NewExecutor(executorName string, etcdClient *etcd.Client, baseDir string, baseEtcdDir string) *Executor {
	metricsRegistry := metrics.NewRegistry()

	stats := &executorStatistics{
		DirectoriesCreated: metrics.NewCounter(),
		DirectoriesDeleted: metrics.NewCounter(),
		FilesWritten:       metrics.NewCounter(),
		FilesDeleted:       metrics.NewCounter(),
		SetsReceived:       metrics.NewCounter(),
		SetsProcessed:      metrics.NewCounter(),
		DeletesReceived:    metrics.NewCounter(),
		DeletesProcessed:   metrics.NewCounter(),
	}

	metricsRegistry.Register("directoriesCreated", stats.DirectoriesCreated)
	metricsRegistry.Register("directoriesDeleted", stats.DirectoriesDeleted)
	metricsRegistry.Register("filesWritten", stats.FilesWritten)
	metricsRegistry.Register("filesDeleted", stats.FilesDeleted)
	metricsRegistry.Register("setsReceived", stats.SetsReceived)
	metricsRegistry.Register("setsProcessed", stats.SetsProcessed)
	metricsRegistry.Register("deletesReceived", stats.DeletesReceived)
	metricsRegistry.Register("deletesProcessed", stats.DeletesProcessed)

	metrics.RegisterRuntimeMemStats(metricsRegistry)

	return &Executor{
		executorName:    executorName,
		etcdClient:      etcdClient,
		baseDir:         baseDir,
		baseEtcdDir:     baseEtcdDir,
		currentIndex:    0,
		metricsRegistry: metricsRegistry,
		stats:           stats,
	}
}

// Run starts the executor.  It starts metric collection and logging, and kicks off the discovery process
// to figure out whether the node needs to be bootstrapped or whether it can pick up from where it left
// off.  In both cases, it triggers the subsequent background routines that handle ongoing processing.
func (e *Executor) Run() error {
	// Start our metric collection.
	go metrics.CaptureRuntimeMemStats(e.metricsRegistry, time.Second*10)
	go metrics.Log(e.metricsRegistry, 2e9, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	// Figure out if we've done any work in the past, and if we can pick it back up.
	lastProcessedIndex, err := e.getLastProcessedIndex()
	if err != nil {
		return err
	}

	if lastProcessedIndex != 0 {
		log.Printf("[runner] Found potential indicator of previous work: last processed index set as '%d'", lastProcessedIndex)

		// See if our last processed index is still in etcd's history.
		presentInHistory := e.checkHistoryForIndex(lastProcessedIndex)
		if presentInHistory {
			log.Print("[runner] Found last processed index still in etcd history.  Trying to start from last index...")

			// We should still be able to catch up, so start from our forward position.
			return e.startFromLastIndex()
		}
	}

	log.Print("[runner] No previous work found.  Starting this node from scratch...")

	// Either we've never done anything or we're too far out of sync, so start from scratch.
	return e.startFromScratch()
}

func (e *Executor) startFromScratch() error {
	// Set up our channels and guards and what not.
	events := make(chan *etcd.Response, 128)
	errors := make(chan error, 1)

	// First, set up a routine that handles our watcher.
	go func() {
		log.Print("[watcher] Starting watch for latest changes...")

		_, err := e.etcdClient.Watch(e.baseEtcdDir, 0, true, events, nil)
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
				// Make sure this isn't a hidden node, unless it's a get, in which case it's fine. We just don't
				// want to bother handling the node metadata that gets set.
				if isHiddenNode(event.Node.Key) && event.Action != "get" {
					continue
				}

				log.Printf("[processor] Received event: %s -> %s (%d / %d / %d)",
					event.Action, event.Node.Key, event.EtcdIndex, event.RaftIndex, event.RaftTerm)

				err := e.processEvent(event)
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
	response, err := e.etcdClient.Get(e.baseEtcdDir, false, true)
	if err != nil {
		log.Printf("[bootstrap] Caught error while trying to get the universe: %s", err)
		return err
	}

	err = e.processEvent(response)
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

func (e *Executor) startFromLastIndex() error {
	log.Print("[warm restart] Not implemented.")

	return nil
}

func (e *Executor) processEvent(event *etcd.Response) error {
	var highestIndex uint64
	var err error

	log.Printf("[handler] Processing event: %s -> %s (%d / %d / %d)",
		event.Action, event.Node.Key, event.EtcdIndex, event.RaftIndex, event.RaftTerm)

	// Handle the event according to action.
	switch event.Action {
	case "get":
		highestIndex, err = e.handleGet(event)
	case "set":
		e.stats.SetsReceived.Inc(1)
		highestIndex, err = e.handleSet(event)
	case "delete":
		e.stats.DeletesReceived.Inc(1)
		highestIndex, err = e.handleDelete(event)
	}

	if err != nil {
		return err
	}

	// If we actually processed something, update our processed index.
	if highestIndex != 0 {
		log.Printf("[handler] Successfully processed %s -> %s, with a high index of %d", event.Action, event.Node.Key, highestIndex)

		switch event.Action {
		case "set":
			e.stats.SetsProcessed.Inc(1)
		case "delete":
			e.stats.DeletesProcessed.Inc(1)
		}

		// Set the highest index we processed.  This might be lower than our previous highest, in which case
		// the overall highest won't change.
		return e.setLastProcessedIndex(highestIndex)
	}

	log.Printf("[handler] Successfully processed %s -> %s, but index reported as 0", event.Action, event.Node.Key)

	return nil
}

func (e *Executor) handleGet(event *etcd.Response) (uint64, error) {
	// Pass this off to our subordinate, since we are recursively challenged.
	return e.handleGetImpl(event.Node)
}

func (e *Executor) handleGetImpl(node *etcd.Node) (uint64, error) {
	// Set the highest index as the node we got.  Child nodes might usurp this.
	highestIndex := node.ModifiedIndex

	// If we have a directory, just try and parse its children nodes.  If it has none, just
	// create the directory.
	if node.Dir {
		// Create the directory it represents first.
		err := e.createDirectory(node.Key)
		if err != nil {
			return 0, err
		}

		e.stats.DirectoriesCreated.Inc(1)

		// Now go through any children nodes this node may have, and process them all.
		for _, childNode := range node.Nodes {
			index, err := e.handleGetImpl(&childNode)
			if err != nil {
				return 0, err
			}

			if index > highestIndex {
				highestIndex = index
			}
		}
	} else {
		// Seems that we just have a value.  Write it.
		err := e.writeFile(node.Key, node.Value)
		if err != nil {
			return 0, err
		}

		e.stats.FilesWritten.Inc(1)
	}

	return highestIndex, nil
}

func (e *Executor) handleSet(event *etcd.Response) (uint64, error) {
	var err error

	if event.Node.Dir {
		// This is just a directory.
		err = e.createDirectory(event.Node.Key)

		if err == nil {
			e.stats.DirectoriesCreated.Inc(1)
		}
	} else {
		// Looks like we have a file, weee!
		err = e.writeFile(event.Node.Key, event.Node.Value)

		if err == nil {
			e.stats.FilesWritten.Inc(1)
		}
	}

	return event.Node.ModifiedIndex, err
}

func (e *Executor) handleDelete(event *etcd.Response) (uint64, error) {
	err := e.unlinkPath(event.Node.Key)
	if err == nil {
		if event.Node.Dir {
			// This is just a directory.
			e.stats.DirectoriesDeleted.Inc(1)
		} else {
			e.stats.FilesDeleted.Inc(1)
		}
	}

	return event.Node.ModifiedIndex, err
}

func (e *Executor) createDirectory(directoryName string) error {
	absoluteDir := e.getOnDiskPath(directoryName)
	return os.MkdirAll(absoluteDir, defaultDirectoryMode)
}

func (e *Executor) writeFile(fileName string, value string) error {
	absoluteFile := e.getOnDiskPath(fileName)
	absoluteDir := filepath.Dir(absoluteFile)

	if !doesPathExist(absoluteDir) {
		// Create the directory it represents first.
		err := os.MkdirAll(absoluteDir, defaultDirectoryMode)
		if err != nil {
			return err
		}

		e.stats.DirectoriesCreated.Inc(1)
	}

	// Try and chmod the path to our default file mode.  The file might not actually exist yet.
	// If it does, and it's not the right mode, it's most likely not writable by us anyways and
	// our subsequent open will fail.  We tried.
	os.Chmod(absoluteFile, defaultFileMode)

	// Now let's actually try and write to the file.
	file, err := os.OpenFile(absoluteFile, defaultFileFlags, defaultFileMode)
	if err != nil {
		return err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			log.Fatalf("got error when closing file: %s", err)
		}
	}()

	n, err := file.WriteString(value)
	if err != nil {
		return err
	}

	if n != len(value) {
		return fmt.Errorf("tried to write value of len %d to %s; only wrote %d", len(value), absoluteFile, n)
	}

	// Lastly, sync it so it gets persisted to disk like... right meow.
	return file.Sync()
}

func (e *Executor) unlinkPath(key string) error {
	absolutePath := e.getOnDiskPath(key)
	return os.Remove(absolutePath)
}

func (e *Executor) getOnDiskPath(key string) string {
	trimmedPath := strings.Trim(key, "/ ")
	unprefixedPath := strings.TrimPrefix(trimmedPath, e.baseEtcdDir)
	return filepath.Join(e.baseDir, unprefixedPath)
}

func (e *Executor) checkHistoryForIndex(index uint64) bool {
	_, err := e.etcdClient.Watch(e.baseEtcdDir, index, true, nil, nil)
	return errorIsetcdError(err, etcdErrEventIndexCleared)
}

func (e *Executor) getLastProcessedIndex() (uint64, error) {
	// Try and grab whatever we may have put there last.
	response, err := e.etcdClient.Get(e.baseEtcdDir+"/_executors/_"+e.executorName, false, false)
	if err != nil {
		// If we simply couldn't find the key - that's not an issue, per se.  We only care about
		// failures of the cluster or something. Started from the bottom, now we here.
		if errorIsetcdError(err, etcdErrKeyNotFound) {
			return 0, nil
		}

		return 0, err
	}

	// This is the simplest way to convert a string to a uint64.  Ugh.
	index, err := strconv.ParseUint(response.Node.Value, 10, 64)
	if err != nil {
		return 0, err
	}

	return index, nil
}

func (e *Executor) setLastProcessedIndex(index uint64) error {
	// If we don't have a cached value, try and cache it.
	if e.currentIndex == 0 {
		currentIndex, err := e.getLastProcessedIndex()
		if err != nil {
			return err
		}

		e.currentIndex = currentIndex
	}

	// If we're already past this point, don't try and update anything.
	if index <= e.currentIndex {
		return nil
	}

	// Now set the current index locally and in etcd.
	e.currentIndex = index

	_, err := e.etcdClient.Set(e.baseEtcdDir+"/_executors/_"+e.executorName, strconv.FormatUint(index, 10), 0)
	return err
}

func doesPathExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func errorIsetcdError(err error, errCode int) bool {
	if err == nil {
		return false
	}

	// If the error code we're checking for is -1, we're just trying to match an etcdError, but not a specific one.
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && (etcdErr.ErrorCode == errCode || errCode == -1)
}

func isHiddenNode(key string) bool {
	baseKey := filepath.Base(key)
	return strings.HasPrefix(baseKey, "_")
}

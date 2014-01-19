package oldschool

import "github.com/coreos/go-etcd/etcd"
import "path/filepath"
import "strconv"
import "strings"
import "fmt"
import "log"
import "os"

const (
	EtcdErrEventIndexCleared = 401
	EtcdErrKeyNotFound       = 100
	DefaultDirectoryMode     = os.ModeDir | os.ModeSetgid | 0775
	DefaultFileMode          = 0644
	DefaultFileFlags         = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
)

type Executor struct {
	executorName string
	etcdClient   *etcd.Client
	baseDir      string
	baseEtcdDir  string
	currentIndex uint64
	Statistics   *ExecutorStatistics
}

type ExecutorStatistics struct {
	DirectoriesCreated uint64
	DirectoriesDeleted uint64
	FilesWritten       uint64
	FilesDeleted       uint64
	SetsReceived       uint64
	SetsProcessed      uint64
	DeletesReceived    uint64
	DeletesProcessed   uint64
}

func NewExecutor(executorName string, etcdClient *etcd.Client, baseDir string, baseEtcdDir string) *Executor {
	return &Executor{
		executorName: executorName,
		etcdClient:   etcdClient,
		baseDir:      baseDir,
		baseEtcdDir:  baseEtcdDir,
		currentIndex: 0,
		Statistics:   &ExecutorStatistics{},
	}
}

func (me *Executor) Run() error {
	// Figure out if we've done any work in the past, and if we can pick it back up.
	lastProcessedIndex, err := me.getLastProcessedIndex()
	if err != nil {
		return err
	}

	if lastProcessedIndex != 0 {
		log.Printf("[runner] Found potential indicator of previous work: last processed index set as '%d'", lastProcessedIndex)

		// See if our last processed index is still in etcd's history.
		presentInHistory := me.checkHistoryForIndex(lastProcessedIndex)
		if presentInHistory {
			log.Print("[runner] Found last processed index still in etcd history.  Trying to start from last index...")

			// We should still be able to catch up, so start from our forward position.
			return me.startFromLastIndex()
		}
	}

	log.Print("[runner] No previous work found.  Starting this node from scratch...")

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
				// Make sure this isn't a hidden node, unless it's a get, in which case it's fine. We just don't
				// want to bother handling the node metadata that gets set.
				if isHiddenNode(event.Node.Key) && event.Action != "get" {
					continue
				}

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
	var highestIndex uint64
	var err error

	log.Printf("[handler] Processing event: %s -> %s (%d / %d / %d)",
		event.Action, event.Node.Key, event.EtcdIndex, event.RaftIndex, event.RaftTerm)

	// Handle the event according to action.
	switch event.Action {
	case "get":
		highestIndex, err = me.handleGet(event)
	case "set":
		me.Statistics.SetsReceived++
		highestIndex, err = me.handleSet(event)
	case "delete":
		me.Statistics.DeletesReceived++
		highestIndex, err = me.handleDelete(event)
	}

	if err != nil {
		return err
	}

	// If we actually processed something, update our processed index.
	if highestIndex != 0 {
		log.Printf("[handler] Successfully processed %s -> %s, with a high index of %d", event.Action, event.Node.Key, highestIndex)

		switch event.Action {
		case "set":
			me.Statistics.SetsProcessed++
		case "delete":
			me.Statistics.DeletesProcessed++
		}

		// Set the highest index we processed.  This might be lower than our previous highest, in which case
		// the overall highest won't change.
		return me.setLastProcessedIndex(highestIndex)
	} else {
		log.Printf("[handler] Successfully processed %s -> %s, but index reported as 0", event.Action, event.Node.Key)
	}

	return nil
}

func (me *Executor) handleGet(event *etcd.Response) (uint64, error) {
	// Pass this off to our subordinate, since we are recursively challenged.
	return me.handleGetImpl(event.Node)
}

func (me *Executor) handleGetImpl(node *etcd.Node) (uint64, error) {
	// Set the highest index as the node we got.  Child nodes might usurp this.
	highestIndex := node.ModifiedIndex

	// If we have a directory, just try and parse its children nodes.  If it has none, just
	// create the directory.
	if node.Dir {
		// Create the directory it represents first.
		err := me.createDirectory(node.Key)
		if err != nil {
			return 0, err
		}

		me.Statistics.DirectoriesCreated++

		// Now go through any children nodes this node may have, and process them all.
		for _, childNode := range node.Nodes {
			index, err := me.handleGetImpl(&childNode)
			if err != nil {
				return 0, err
			}

			if index > highestIndex {
				highestIndex = index
			}
		}
	} else {
		// Seems that we just have a value.  Write it.
		err := me.writeFile(node.Key, node.Value)
		if err != nil {
			return 0, err
		}

		me.Statistics.FilesWritten++
	}

	return highestIndex, nil
}

func (me *Executor) handleSet(event *etcd.Response) (uint64, error) {
	var err error

	if event.Node.Dir {
		// This is just a directory.
		err = me.createDirectory(event.Node.Key)

		if err == nil {
			me.Statistics.DirectoriesCreated++
		}
	} else {
		// Looks like we have a file, weee!
		err = me.writeFile(event.Node.Key, event.Node.Value)

		if err == nil {
			me.Statistics.FilesWritten++
		}
	}

	return event.Node.ModifiedIndex, err
}

func (me *Executor) handleDelete(event *etcd.Response) (uint64, error) {
	err := me.unlinkPath(event.Node.Key)
	if err == nil {
		if event.Node.Dir {
			// This is just a directory.
			me.Statistics.DirectoriesDeleted++
		} else {
			me.Statistics.FilesDeleted++
		}
	}

	return event.Node.ModifiedIndex, err
}

func (me *Executor) createDirectory(directoryName string) error {
	absoluteDir := me.getOnDiskPath(directoryName)
	return os.MkdirAll(absoluteDir, DefaultDirectoryMode)
}

func (me *Executor) writeFile(fileName string, value string) error {
	absoluteFile := me.getOnDiskPath(fileName)
	absoluteDir := filepath.Dir(absoluteFile)

	if !doesPathExist(absoluteDir) {
		// Create the directory it represents first.
		err := os.MkdirAll(absoluteDir, DefaultDirectoryMode)
		if err != nil {
			return err
		}

		me.Statistics.DirectoriesCreated++
	}

	// Try and chmod the path to our default file mode.  The file might not actually exist yet.
	// If it does, and it's not the right mode, it's most likely not writable by us anyways and
	// our subsequent open will fail.  We tried.
	os.Chmod(absoluteFile, DefaultFileMode)

	// Now let's actually try and write to the file.
	file, err := os.OpenFile(absoluteFile, DefaultFileFlags, DefaultFileMode)
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

func (me *Executor) unlinkPath(key string) error {
	absolutePath := me.getOnDiskPath(key)
	return os.Remove(absolutePath)
}

func (me *Executor) getOnDiskPath(key string) string {
	trimmedPath := strings.Trim(key, "/ ")
	unprefixedPath := strings.TrimPrefix(trimmedPath, me.baseEtcdDir)
	return filepath.Join(me.baseDir, unprefixedPath)
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
		// failures of the cluster or something. Started from the bottom, now we here.
		if errorIsEtcdError(err, EtcdErrKeyNotFound) {
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

func (me *Executor) setLastProcessedIndex(index uint64) error {
	// If we don't have a cached value, try and cache it.
	if me.currentIndex == 0 {
		currentIndex, err := me.getLastProcessedIndex()
		if err != nil {
			return err
		}

		me.currentIndex = currentIndex
	}

	// If we're already past this point, don't try and update anything.
	if index <= me.currentIndex {
		return nil
	}

	// Now set the current index locally and in etcd.
	me.currentIndex = index

	_, err := me.etcdClient.Set(me.baseEtcdDir+"/_executors/_"+me.executorName, strconv.FormatUint(index, 10), 0)
	return err
}

func doesPathExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func errorIsEtcdError(err error, errCode int) bool {
	if err == nil {
		return false
	}

	// If the error code we're checking for is -1, we're just trying to match an EtcdError, but not a specific one.
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && (etcdErr.ErrorCode == errCode || errCode == -1)
}

func isHiddenNode(key string) bool {
	baseKey := filepath.Base(key)
	return strings.HasPrefix(baseKey, "_")
}

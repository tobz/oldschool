package oldschool

import "strings"
import "os"
import "io/ioutil"
import "path/filepath"
import "log"
import "fmt"
import "github.com/coreos/go-etcd/etcd"

type Executor struct {
	etcdClient  *etcd.Client
	baseDir     string
	baseEtcdDir string
	responses   chan *etcd.Response
	running     bool
	stop        chan bool
}

func NewExecutor(etcdClient *etcd.Client, baseDir string, baseEtcdDir string) *Executor {
	return &Executor{
		etcdClient:  etcdClient,
		baseDir:     baseDir,
		baseEtcdDir: baseEtcdDir,
		responses:   make(chan *etcd.Response, 1),
		running:     false,
		stop:        make(chan bool, 1),
	}
}

func (me *Executor) Start() {
	// If we're currently watching, bail out.
	if me.running {
		return
	}

	// Set ourselves to running to block out any other attempts.
	me.running = true

	// Start running our file persister.
	go me.runPersister()

	// Start watching, starting from the beginning to bring everything into sync.
	// Let the persister handle figuring out where to pick up from.
	go me.resynchronize()
}

func (me *Executor) resynchronize() {
	// First, make sure our prefix exists.
	response, err := me.etcdClient.Get(me.baseEtcdDir, false, false)
	if err != nil {
		log.Fatalf("caught an error while finding start index: %s", err)
	}

	if response == nil {
		log.Fatal("initial get of prefix returned empty!")
	}

	// Now, record where it started.
	startIndex := response.Node.CreatedIndex
	log.Printf("got start index of '%s' as %d", me.baseEtcdDir, startIndex)

	// Start watching from when everything was created.
	_, err = me.etcdClient.Watch(me.baseEtcdDir, startIndex, true, me.responses, me.stop)
	if err != nil {
		log.Fatalf("caught an error while watching etcd: %s", err)
	}
}

func (me *Executor) Stop() {
	me.stop <- true
	me.running = false
}

func (me *Executor) runPersister() {
	for {
		select {
		case response := <-me.responses:
			err := me.handleResponse(response)
			if err != nil {
				log.Fatal("caught an error while handling a response: %s", err)
			}
		case <-me.stop:
			break
		}
	}
}

func (me *Executor) handleResponse(response *etcd.Response) error {
	switch response.Action {
	case "set":
		return me.handleSet(response)
	case "delete":
		return me.handleDelete(response)
	}

	return nil
}

func (me *Executor) handleSet(response *etcd.Response) error {
	// Build our on-disk paths so we can ensure directories.
	unprefixedKey := strings.Replace(response.Node.Key, "/"+me.baseEtcdDir, "", -1)
	targetDiskPath := filepath.Clean(filepath.Join(me.baseDir, unprefixedKey))
	targetBaseDirectory := filepath.Dir(targetDiskPath)

	// Now let's make sure that the directory this key is going in exists or we can create it.
	if me.baseDir != targetBaseDirectory && !doesDirectoryExist(targetBaseDirectory) {
		err := os.MkdirAll(targetBaseDirectory, 0775)
		if err != nil {
			return fmt.Errorf("failed attempting to ensure '%s' existed", err)
		}
	}

	// Now put the file in place.
	err := ioutil.WriteFile(targetDiskPath, []byte(response.Node.Value), 0664)
	if err != nil {
		return fmt.Errorf("failed to write to '%s': %s", targetDiskPath, err)
	}

	return nil
}

func (me *Executor) handleDelete(response *etcd.Response) error {
	// Build our on-disk paths so we can ensure directories.
	unprefixedKey := strings.Replace(response.Node.Key, "/"+me.baseEtcdDir, "", -1)
	onDiskPath := filepath.Clean(filepath.Join(me.baseDir, unprefixedKey))

	// Now just delete the file/directory.
	err := os.Remove(onDiskPath)
	if err != nil {
		return fmt.Errorf("failed to delete object: %s", err)
	}

	return nil
}

func doesDirectoryExist(directory string) bool {
	_, err := os.Stat(directory)
	return err == nil
}

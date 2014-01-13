package oldschool

import "os"
import "fmt"
import "path/filepath"
import "github.com/coreos/go-etcd/etcd"

type Executor struct {
    etcdClient *etcd.Client
    baseDir string
    baseEtcdDir string
    responses chan *etcd.Response
    running bool
    stop chan bool
}

func NewExecutor(etcdClient *etcd.Client, baseDir string, baseEtcdDir string) (*Executor) {
    return &Executor{
        etcdClient: etcdClient,
        baseDir: baseDir,
        baseEtcdDir: baseEtcdDir,
        responses: make(chan *etcd.Response, 1),
        running: false,
        stop: make(chan bool, 1),
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
    go me.runFilePersister()

    // Start watching, starting from the beginning to bring everything into sync.
    // Let the persister handle figuring out where to pick up from.
    go func() {
        _, err := etcdClient.Watch(baseEtcdDir, 1, true, me.responses, me.stop)
        if err != nil {
            log.Fatalf("caught an error while watching etcd: %s", err)
        }
    }
}

func (me *Executor) Stop() {
    me.stop <- true
    me.running = false
}

func (me *Executor) runFilePersister() {
    for {
        select {
        case response <- me.responses:
            err := me.handleResponse(response)
            if err != nil {
                log.Fatal("caught an error while handling a response: %s", err)
            }
        case _ <- me.stop:
            break
    }
}

func (me *Executor) handleResponse(response *etcd.Response) (error) {
    return fmt.Errorf("implement me!")
}

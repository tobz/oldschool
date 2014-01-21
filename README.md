oldschool
=========

old-fashioned configuration management


what the hell is this
=========

configuration management can be a pain in the ass.  i'm not talking about Chef or Puppet, either.  i'm talking about configuration files: little snippets of golden values and JSON blurbs and XML fragments, all put in place by either some cobbled-together system or some person who doesn't even work for you anymore.

oldschool takes awesome tech... and kicks it old school.  we use etcd as a distributed and redundant data store, and sprinkle some love on top to handle mapping keys, and their parent directories, to actual files and directories on disk.  your application consumes these files just like normal, and oldschool runs in the background, watching and waiting or changes.  as soon as it sees a change, it propagates it to disk.  no fuss, no muss.  it will bootstrap a node from 0 to 60 without intervention.  tell it where to put the files, where etcd is, etc... and it does the rest.


how do i use it
=========

etcd treats keys like a file path - you essentially have keys that resemble a path with directories and files.  so, a key maps perfectly to an actual file on disk, nested in an arbitrary number of folders.  you specify a base directory on disk, and oldschool watches a specific prefix (aka directory) in etcd.  so, if a key is set at "/yourprefix/myfile", it gets populated on disk at /your/base/directory/myfile.

etcd also has only a REST API - you do GETs and PUTs and DELETEs to set data.  the modification-based actions (the PUTs/DELETEs) get captured by oldschool using etcd's watch functionality, which polls the etcd cluster for changes.  when it gets one of these events, it makes the according change on disk.  if it gets a PUT, it makes or update that file with the data to be set, potentially creating the parent directory(s) in the process.  if it's a DELETE, it results in a directory or file being deleted from disk.  the key format etcd uses matches up perfectly to files on a disk.  wunderbar!

thusly, all you need to do is tell oldschool where to use as the base directory, and the prefix of the events to listen for, and it goes off and maps them from etcd to disk.  you also have to tell it what it's unique ID is, which is used for tracking what events have been acknowledged in the case of the process restarting, and the address of a member in the etcd cluster, so it can connect.

    $ ./oldschool -name myspecialhost -join-node 127.0.0.1:4001 -name testing -base-dir=/home/toby/oldschool-data -base-etcd-dir=oldschool
    2014/01/20 19:12:01 [main] etcd cluster appears operational, found these nodes: http://127.0.0.1:4003, http://127.0.0.1:4001, http://127.0.0.1:4002
    2014/01/20 19:12:01 [runner] Found potential indicator of previous work: last processed index set as '11930'
    2014/01/20 19:12:01 [runner] No previous work found.  Starting this node from scratch...
    2014/01/20 19:12:01 [bootstrap] Starting bulk load of universe from etcd...
    2014/01/20 19:12:01 [watcher] Starting watch for latest changes...
    2014/01/20 19:12:01 [handler] Processing event: get -> /oldschool (11931 / 88510 / 18)
    2014/01/20 19:12:01 [handler] Successfully processed get -> /oldschool, with a high index of 11928
    2014/01/20 19:12:01 [bootstrap] Starting processor...
    2014/01/20 19:12:01 [bootstrap] Spinning while we wait for any potential error.  We are now operational and in cruise control mode.
    2014/01/20 19:12:01 [processor] Starting to process event backlog...
    
and here's what it looks like on disk to add and remove values from etcd

    $ find .
    .
     
    $ curl -L http://127.0.0.1:4001/v2/keys/oldschool/confs/services/app.json -X PUT -d value="[1,2,3,4,5]"
    {"action":"set","node":{"key":"/oldschool/confs/services/app.jsondb","value":"[1,2,3,4,5]","modifiedIndex":11924,"createdIndex":11924}}
     
    $ find .
    .
    ./confs
    ./confs/
    ./confs/services/app.json
     
    $ curl -L http://127.0.0.1:4001/v2/keys/oldschool/confs/services/web.json -X PUT -d value="[5,6,7,8,9]"
    {"action":"set","node":{"key":"/oldschool/confs/services/web.json","value":"[1,2,3,4,5]","modifiedIndex":11926,"createdIndex":11926}}
     
    $ find .
    .
    ./confs
    ./confs/services
    ./confs/services/web.json
    ./confs/services/app.json
     
    $ curl -L http://127.0.0.1:4001/v2/keys/oldschool/confs/default/rabbitmq.json -X PUT -d value="[10,11,12,13,14]"
    {"action":"set","node":{"key":"/oldschool/confs/default/rabbitmq.json","value":"[1,2,3,4,5]","modifiedIndex":11928,"createdIndex":11928}}
     
    $ find .
    .
    ./confs
    ./confs/default
    ./confs/default/rabbitmq.json
    ./confs/services
    ./confs/services/web.json
    ./confs/services/app.json
     
    $ curl -L http://127.0.0.1:4001/v2/keys/oldschool/confs/services/web.json -X DELETE
    {"action":"delete","node":{"key":"/oldschool/confs/services/web.json","modifiedIndex":11930,"createdIndex":11926},"prevNode":{"key":"/oldschool/confs/services/web.json","value":"[1,2,3,4,5]","modifiedIndex":11926,"createdIndex":11926}}
     
    $ find .
    .
    ./confs
    ./confs/default
    ./confs/default/rabbitmq.json
    ./confs/services
    ./confs/services/app.json

special thanks
=========

to my employer, @bsdwire, for allowing me the time to tinker with this type of stuff, and to the @coreos team for building etcd

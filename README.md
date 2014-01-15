oldschool
=========

old-fashioned configuration management for complexity seekers


what the hell is this
=========

configuration management can be a pain in the ass.  i'm not talking about Chef or Puppet, either.  i'm talking about configuration files: little snippets of golden values and JSON blurbs and XML fragments, all put in place by either some cobbled-together system or some person who doesn't even work for you anymore.

we've become infatuated with caches and relational databases and crazy complicated ways of pushing out configuration values.  we've forgotten about the humblest of the data containers at our disposal: the file.  oldschool takes awesome tech... and kicks it old school.  we use etcd as a distributed and redundant data store, and sprinkle some love on top to handle mapping keys, and their parent directories, to actual files and directories on disk.  your application consumes these files just like normal, and oldschool runs in the background, watching and waiting or changes.  as soon as it sees a change, it propagates it to disk.  no fuss, no muss.  it will bootstrap a node from 0 to 60 without intervention.  tell it where to put the files, where etcd is, etc... and it does the rest.


special thanks
=========

i'd like to give a shout out to bourbon, bacon and chocolate chip oatmeal cookies.  they are primarily responsible for any potential success this project achieves.   also, my fiancee.  and the cool place i work (@bsdwire) that lets me tinker with these sorts of things.

other than that, the great work of the @coreos contingent who, of course, wrote etcd, which this project depends on.

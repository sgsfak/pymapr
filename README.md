pymapr
======

This is a simple multiprocessing based MapReduce framework in Python that is based on the [Introduction to Data Science ](https://class.coursera.org/datasci-001/) course on [Coursera](https://www.coursera.org/) taught by Bill Howe.

In the course a simple single threaded implementation of the framework was given for the students to experiment in the small datasets that were given in the exercises. This implementation can be seen in ``MapReduce_original.py`` that's included here for comparison's sake.

The code has been tested in Python 2.7.3, in Mac OS X and Linux.

### Execution
You can check the sample ``wordcount.py`` that contains the mapper and reducer functions for the traditional word count problem. You can run it as follows:

    python wordcount.py books.json

and you should get something like the following (in 4-core machine):

	Master[Mapper phase]: starting
	Mapper (61930) started
	Mapper (61931) started
	Master[Mapper phase]: feeding workers ended..
	Mapper (61932) started
	Mapper (61933) started
	Mapper (61932): ending (2 items processed)
	Mapper (61931): ending (3 items processed)
	Mapper (61930): ending (3 items processed)
	Mapper (61933): ending (2 items processed)
	Master[Mapper phase]: gathering results ended..
	Master[Reducer phase]: starting
	Reducer (61934) started
	Reducer (61935) started
	Master[Reducer phase]: feeding workers ended..
	Reducer (61936) started
	Reducer (61937) started
	Reducer (61934): ending (153 items processed)
	Reducer (61936): ending (153 items processed)
	Reducer (61935): ending (153 items processed)
	Reducer (61937): ending (152 items processed)
	Master[Reducer phase]: gathering results ended..
	["all", 4]
	["child", 2]
	["winds", 1]
	["whose", 1]
	["vex", 1]
	["Day", 1]
	["Whitman", 2]
	["rhyme", 1]
	["heaven", 1]
	["small", 1]
	...

For your mapreduce tasks you need to create similar code and provide your own map and reduce functions. The map functions should invoke the ``emit_intermediate`` function of ``MapReduce`` wheres the reduce functions should invoke the ``emit`` instead.

### Design
This framework makes sense in a multicore machine and of course for lightweight processing tasks that can be efficiently handled in single machine. Of course there's no 

The implementation (``MapReduce.py``) uses Python's ``multiprocessing`` module to spawn a number of processes (equal to the number of cores in the machine, by default) for running the map and reduce phases. The master process (i.e. the parent) sends work the spawned children using [pipes](http://docs.python.org/2/library/multiprocessing.html#multiprocessing.Pipe). The worker processes (children) send their results (intermediate, in the case of map processes) using a [Queue](http://docs.python.org/2/library/multiprocessing.html#multiprocessing.Queue) (``IQUEUE`` variable in the code). The master process is using one communication channel per worker (mapper/producer) process in order to submit the work to be done. So during the map phase the master process sends the data to the mappers in a round-robin fashion using the per-worker pipe.

After the submission of the work to be done in the Map processes the Master process proceeds to immediately waiting for the intermediate results to arrive from the workers. It can therefore do the "shuffling" of the intermediate results while waiting for the mapper processes to finish. The intermediate queue (``IQUEUE``) can then in parallel be populated by the mappers (or the reducers) and emptied by the master. 





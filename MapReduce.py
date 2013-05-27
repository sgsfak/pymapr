import json
import sys
import os
import multiprocessing as mp
from collections import defaultdict


def spawn(f):
    def fun(pipe):
        output_q, input_q = pipe
        output_q.close()
        pname = "%s (%d)" % (mp.current_process().name, os.getpid())
        sys.stderr.write(pname + " started\n")
        i = 0
        while True:
            x = input_q.recv()
            if x is None:
                break
            f(*x)
            i += 1
        MapReduce.IQUEUE.put(('__map_reduce_mp_event__', 'worker_ended'))
        sys.stderr.write("%s: ending (%d items processed)\n" % (pname, i))
    return fun


class MapReduce:
    IQUEUE = mp.Queue()

    MAP_PHASE = 0
    REDUCE_PHASE = 1

    def __init__(self, num_workers=None):
        self.intermediate_values = defaultdict(list)
        self.result = []
        if not num_workers:
            num_workers = mp.cpu_count()
        self.num_workers = num_workers

    def emit_intermediate(self, key, value):
        MapReduce.IQUEUE.put( (key, value) )

    def emit(self, value):
        MapReduce.IQUEUE.put(value)

    def _doPar(self, func, iterable, phase=MAP_PHASE):
        """It applies the given function to each item in the iterable by spawing
        a number of worker processes. Each item should be tuple containing the
        arguments for the function."""
        name = "Mapper"
        if phase == MapReduce.REDUCE_PHASE:
            name = "Reducer"
        sys.stderr.write("Master[%s phase]: starting\n" % name)
        pipes = [mp.Pipe() for _ in range(self.num_workers)]
        proc = [mp.Process(target=spawn(func), name=name, args=(q,)) for q in pipes]
        for p in proc:
            p.daemon = True
            p.start()
        for output_p, input_p in pipes:
            input_p.close() # we don't need to read from the pipes
        qi = 0
        for item in iterable:
            pipes[qi][0].send(item)
            qi = (qi+1) % self.num_workers
        for q,_ in pipes:
            q.send(None)  # add termination tokens
            q.close()
        sys.stderr.write("Master[%s phase]: feeding workers ended..\n" % name)
        active_workers = self.num_workers
        if phase == MapReduce.MAP_PHASE:
            # Start gathering the intermediate results:
            while active_workers > 0:
                key, value = MapReduce.IQUEUE.get()
                if key == '__map_reduce_mp_event__' and value == 'worker_ended':
                    active_workers -= 1
                else:
                    self.intermediate_values[key].append(value)
        else:
            # Reduce phase: gather final results:
            while active_workers > 0:
                value = MapReduce.IQUEUE.get()
                if isinstance(value, tuple) and value[0] == '__map_reduce_mp_event__' and value[1] == 'worker_ended':
                    active_workers -= 1
                else:
                    self.result.append(value)
        sys.stderr.write("Master[%s phase]: gathering results ended..\n" % name)
        for p in proc:
            p.join()

    def execute(self, data, mapper, reducer):
        records = (json.loads(line) for line in data)
        self._doPar(mapper, ((rec,) for rec in records), phase=MapReduce.MAP_PHASE)
        self._doPar(reducer, self.intermediate_values.items(), phase=MapReduce.REDUCE_PHASE)
        jenc = json.JSONEncoder()
        for item in self.result:
            print jenc.encode(item)

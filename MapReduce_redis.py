import json
import sys
import os
import multiprocessing as mp
import redis

r = redis.Redis()


def spawn_mapper(f):
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
        sys.stderr.write("%s: ending (%d items processed)\n" % (pname, i))
    return fun


def spawn_reducer(f):
    def fun():
        pname = "%s (%d)" % (mp.current_process().name, os.getpid())
        sys.stderr.write(pname + " started\n")
        i = 0
        while True:
            key = r.spop('mr:ivals:keys')
            if key is None:
                break
            rkey = 'mr:ivals:%s' % key
            vals = [json.loads(v) for v in r.lrange(rkey, 0, -1)]
            f(key, vals)
            r.delete(rkey)
            i += 1
        sys.stderr.write("%s: ending (%d items processed)\n" % (pname, i))
    return fun


class MapReduce:

    def __init__(self, num_workers=None):
        self.result = []
        if not num_workers:
            num_workers = mp.cpu_count()
        self.num_workers = num_workers

    def emit_intermediate(self, key, value):
        pipe = r.pipeline()
        # put the key into the set of intermediate keys
        pipe.sadd('mr:ivals:keys', str(key))
        # and the new intemediate value to the corresponding list
        pipe.lpush('mr:ivals:'+str(key), json.dumps(value))
        pipe.execute()

    def emit(self, value):
        r.lpush('mr:results', json.dumps(value))

    def _doMap(self, func, iterable):
        """It applies the given function to each item in the iterable by spawing
        a number of worker processes. Each item should be tuple containing the
        arguments for the function."""
        name = "Mapper"
        sys.stderr.write("Master[%s phase]: starting\n" % name)
        pipes = [mp.Pipe() for _ in range(self.num_workers)]
        proc = [mp.Process(target=spawn_mapper(func), name=name, args=(q,)) for q in pipes]
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
        for p in proc:
            p.join()
        sys.stderr.write("Master[%s phase]: ended..\n" % name)

    def _doReduce(self, func):
        """It applies the given function to each item in the iterable by spawing
        a number of worker processes. Each item should be tuple containing the
        arguments for the function."""
        name = "Reducer"
        sys.stderr.write("Master[%s phase]: starting\n" % name)
        proc = [mp.Process(target=spawn_reducer(func), name=name) for _ in range(self.num_workers)]
        for p in proc:
            p.daemon = True
            p.start()
        for p in proc:
            p.join()
        sys.stderr.write("Master[%s phase]: ended..\n" % name)

    def execute(self, data, mapper, reducer):
        records = (json.loads(line) for line in data)
        self._doMap(mapper, ((rec,) for rec in records))
        self._doReduce(reducer)
        self.result = [json.loads(i) for i in r.lrange('mr:results', 0, -1)]
        r.delete('mr:results')
        jenc = json.JSONEncoder()
        for item in self.result:
            print jenc.encode(item)

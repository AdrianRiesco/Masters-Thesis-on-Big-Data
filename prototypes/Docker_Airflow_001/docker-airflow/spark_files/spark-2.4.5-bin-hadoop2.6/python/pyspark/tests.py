#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.
"""

from array import array
from glob import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
import random
import threading
import hashlib

from py4j.protocol import Py4JJavaError
try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest
    if sys.version_info[0] >= 3:
        xrange = range
        basestring = str

if sys.version >= "3":
    from io import StringIO
else:
    from StringIO import StringIO


from pyspark import keyword_only
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.java_gateway import _launch_gateway
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.serializers import read_int, BatchedSerializer, MarshalSerializer, PickleSerializer, \
    CloudPickleSerializer, CompressedSerializer, UTF8Deserializer, NoOpSerializer, \
    PairDeserializer, CartesianDeserializer, AutoBatchedSerializer, AutoSerializer, \
    FlattenedValuesSerializer
from pyspark.shuffle import Aggregator, ExternalMerger, ExternalSorter
from pyspark import shuffle
from pyspark.profiler import BasicProfiler
from pyspark.taskcontext import BarrierTaskContext, TaskContext

_have_scipy = False
_have_numpy = False
try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy, but that's okay, we'll skip those tests
    pass
try:
    import numpy as np
    _have_numpy = True
except:
    # No NumPy, but that's okay, we'll skip those tests
    pass


SPARK_HOME = os.environ["SPARK_HOME"]


class MergerTests(unittest.TestCase):

    def setUp(self):
        self.N = 1 << 12
        self.l = [i for i in xrange(self.N)]
        self.data = list(zip(self.l, self.l))
        self.agg = Aggregator(lambda x: [x],
                              lambda x, y: x.append(y) or x,
                              lambda x, y: x.extend(y) or x)

    def test_small_dataset(self):
        m = ExternalMerger(self.agg, 1000)
        m.mergeValues(self.data)
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.items()),
                         sum(xrange(self.N)))

        m = ExternalMerger(self.agg, 1000)
        m.mergeCombiners(map(lambda x_y1: (x_y1[0], [x_y1[1]]), self.data))
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.items()),
                         sum(xrange(self.N)))

    def test_medium_dataset(self):
        m = ExternalMerger(self.agg, 20)
        m.mergeValues(self.data)
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.items()),
                         sum(xrange(self.N)))

        m = ExternalMerger(self.agg, 10)
        m.mergeCombiners(map(lambda x_y2: (x_y2[0], [x_y2[1]]), self.data * 3))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.items()),
                         sum(xrange(self.N)) * 3)

    def test_huge_dataset(self):
        m = ExternalMerger(self.agg, 5, partitions=3)
        m.mergeCombiners(map(lambda k_v: (k_v[0], [str(k_v[1])]), self.data * 10))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(len(v) for k, v in m.items()),
                         self.N * 10)
        m._cleanup()

    def test_group_by_key(self):

        def gen_data(N, step):
            for i in range(1, N + 1, step):
                for j in range(i):
                    yield (i, [j])

        def gen_gs(N, step=1):
            return shuffle.GroupByKey(gen_data(N, step))

        self.assertEqual(1, len(list(gen_gs(1))))
        self.assertEqual(2, len(list(gen_gs(2))))
        self.assertEqual(100, len(list(gen_gs(100))))
        self.assertEqual(list(range(1, 101)), [k for k, _ in gen_gs(100)])
        self.assertTrue(all(list(range(k)) == list(vs) for k, vs in gen_gs(100)))

        for k, vs in gen_gs(50002, 10000):
            self.assertEqual(k, len(vs))
            self.assertEqual(list(range(k)), list(vs))

        ser = PickleSerializer()
        l = ser.loads(ser.dumps(list(gen_gs(50002, 30000))))
        for k, vs in l:
            self.assertEqual(k, len(vs))
            self.assertEqual(list(range(k)), list(vs))

    def test_stopiteration_is_raised(self):

        def stopit(*args, **kwargs):
            raise StopIteration()

        def legit_create_combiner(x):
            return [x]

        def legit_merge_value(x, y):
            return x.append(y) or x

        def legit_merge_combiners(x, y):
            return x.extend(y) or x

        data = [(x % 2, x) for x in range(100)]

        # wrong create combiner
        m = ExternalMerger(Aggregator(stopit, legit_merge_value, legit_merge_combiners), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)) as cm:
            m.mergeValues(data)

        # wrong merge value
        m = ExternalMerger(Aggregator(legit_create_combiner, stopit, legit_merge_combiners), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)) as cm:
            m.mergeValues(data)

        # wrong merge combiners
        m = ExternalMerger(Aggregator(legit_create_combiner, legit_merge_value, stopit), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)) as cm:
            m.mergeCombiners(map(lambda x_y1: (x_y1[0], [x_y1[1]]), data))


class SorterTests(unittest.TestCase):
    def test_in_memory_sort(self):
        l = list(range(1024))
        random.shuffle(l)
        sorter = ExternalSorter(1024)
        self.assertEqual(sorted(l), list(sorter.sorted(l)))
        self.assertEqual(sorted(l, reverse=True), list(sorter.sorted(l, reverse=True)))
        self.assertEqual(sorted(l, key=lambda x: -x), list(sorter.sorted(l, key=lambda x: -x)))
        self.assertEqual(sorted(l, key=lambda x: -x, reverse=True),
                         list(sorter.sorted(l, key=lambda x: -x, reverse=True)))

    def test_external_sort(self):
        class CustomizedSorter(ExternalSorter):
            def _next_limit(self):
                return self.memory_limit
        l = list(range(1024))
        random.shuffle(l)
        sorter = CustomizedSorter(1)
        self.assertEqual(sorted(l), list(sorter.sorted(l)))
        self.assertGreater(shuffle.DiskBytesSpilled, 0)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(sorted(l, reverse=True), list(sorter.sorted(l, reverse=True)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(sorted(l, key=lambda x: -x), list(sorter.sorted(l, key=lambda x: -x)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(sorted(l, key=lambda x: -x, reverse=True),
                         list(sorter.sorted(l, key=lambda x: -x, reverse=True)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)

    def test_external_sort_in_rdd(self):
        conf = SparkConf().set("spark.python.worker.memory", "1m")
        sc = SparkContext(conf=conf)
        l = list(range(10240))
        random.shuffle(l)
        rdd = sc.parallelize(l, 4)
        self.assertEqual(sorted(l), rdd.sortBy(lambda x: x).collect())
        sc.stop()


class SerializationTestCase(unittest.TestCase):

    def test_namedtuple(self):
        from collections import namedtuple
        from pickle import dumps, loads
        P = namedtuple("P", "x y")
        p1 = P(1, 3)
        p2 = loads(dumps(p1, 2))
        self.assertEqual(p1, p2)

        from pyspark.cloudpickle import dumps
        P2 = loads(dumps(P))
        p3 = P2(1, 3)
        self.assertEqual(p1, p3)

    def test_itemgetter(self):
        from operator import itemgetter
        ser = CloudPickleSerializer()
        d = range(10)
        getter = itemgetter(1)
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

        getter = itemgetter(0, 3)
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

    def test_function_module_name(self):
        ser = CloudPickleSerializer()
        func = lambda x: x
        func2 = ser.loads(ser.dumps(func))
        self.assertEqual(func.__module__, func2.__module__)

    def test_attrgetter(self):
        from operator import attrgetter
        ser = CloudPickleSerializer()

        class C(object):
            def __getattr__(self, item):
                return item
        d = C()
        getter = attrgetter("a")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))
        getter = attrgetter("a", "b")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

        d.e = C()
        getter = attrgetter("e.a")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))
        getter = attrgetter("e.a", "e.b")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

    # Regression test for SPARK-3415
    def test_pickling_file_handles(self):
        # to be corrected with SPARK-11160
        if not xmlrunner:
            ser = CloudPickleSerializer()
            out1 = sys.stderr
            out2 = ser.loads(ser.dumps(out1))
            self.assertEqual(out1, out2)

    def test_func_globals(self):

        class Unpicklable(object):
            def __reduce__(self):
                raise Exception("not picklable")

        global exit
        exit = Unpicklable()

        ser = CloudPickleSerializer()
        self.assertRaises(Exception, lambda: ser.dumps(exit))

        def foo():
            sys.exit(0)

        self.assertTrue("exit" in foo.__code__.co_names)
        ser.dumps(foo)

    def test_compressed_serializer(self):
        ser = CompressedSerializer(PickleSerializer())
        try:
            from StringIO import StringIO
        except ImportError:
            from io import BytesIO as StringIO
        io = StringIO()
        ser.dump_stream(["abc", u"123", range(5)], io)
        io.seek(0)
        self.assertEqual(["abc", u"123", range(5)], list(ser.load_stream(io)))
        ser.dump_stream(range(1000), io)
        io.seek(0)
        self.assertEqual(["abc", u"123", range(5)] + list(range(1000)), list(ser.load_stream(io)))
        io.close()

    def test_hash_serializer(self):
        hash(NoOpSerializer())
        hash(UTF8Deserializer())
        hash(PickleSerializer())
        hash(MarshalSerializer())
        hash(AutoSerializer())
        hash(BatchedSerializer(PickleSerializer()))
        hash(AutoBatchedSerializer(MarshalSerializer()))
        hash(PairDeserializer(NoOpSerializer(), UTF8Deserializer()))
        hash(CartesianDeserializer(NoOpSerializer(), UTF8Deserializer()))
        hash(CompressedSerializer(PickleSerializer()))
        hash(FlattenedValuesSerializer(PickleSerializer()))


class QuietTest(object):
    def __init__(self, sc):
        self.log4j = sc._jvm.org.apache.log4j

    def __enter__(self):
        self.old_level = self.log4j.LogManager.getRootLogger().getLevel()
        self.log4j.LogManager.getRootLogger().setLevel(self.log4j.Level.FATAL)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.log4j.LogManager.getRootLogger().setLevel(self.old_level)


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.sc = SparkContext('local[4]', class_name)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path


class ReusedPySparkTestCase(unittest.TestCase):

    @classmethod
    def conf(cls):
        """
        Override this in subclasses to supply a more specific conf
        """
        return SparkConf()

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext('local[4]', cls.__name__, conf=cls.conf())

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


class CheckpointTests(ReusedPySparkTestCase):

    def setUp(self):
        self.checkpointDir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.checkpointDir.name)
        self.sc.setCheckpointDir(self.checkpointDir.name)

    def tearDown(self):
        shutil.rmtree(self.checkpointDir.name)

    def test_basic_checkpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)
        self.assertEqual("file:" + self.checkpointDir.name,
                         os.path.dirname(os.path.dirname(flatMappedRDD.getCheckpointFile())))

    def test_checkpoint_and_restore(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: [x])

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        flatMappedRDD.count()  # forces a checkpoint to be computed
        time.sleep(1)  # 1 second

        self.assertTrue(flatMappedRDD.getCheckpointFile() is not None)
        recovered = self.sc._checkpointFile(flatMappedRDD.getCheckpointFile(),
                                            flatMappedRDD._jrdd_deserializer)
        self.assertEqual([1, 2, 3, 4], recovered.collect())


class LocalCheckpointTests(ReusedPySparkTestCase):

    def test_basic_localcheckpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertFalse(flatMappedRDD.isLocallyCheckpointed())

        flatMappedRDD.localCheckpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.isLocallyCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)


class AddFileTests(PySparkTestCase):

    def test_add_py_file(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this job fails due to `userlibrary` not being on the Python path:
        # disable logging in log4j temporarily
        def func(x):
            from userlibrary import UserClass
            return UserClass().hello()
        with QuietTest(self.sc):
            self.assertRaises(Exception, self.sc.parallelize(range(2)).map(func).first)

        # Add the file, so the job should now succeed:
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addPyFile(path)
        res = self.sc.parallelize(range(2)).map(func).first()
        self.assertEqual("Hello World!", res)

    def test_add_file_locally(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        self.sc.addFile(path)
        download_path = SparkFiles.get("hello.txt")
        self.assertNotEqual(path, download_path)
        with open(download_path) as test_file:
            self.assertEqual("Hello World!\n", test_file.readline())

    def test_add_file_recursively_locally(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello")
        self.sc.addFile(path, True)
        download_path = SparkFiles.get("hello")
        self.assertNotEqual(path, download_path)
        with open(download_path + "/hello.txt") as test_file:
            self.assertEqual("Hello World!\n", test_file.readline())
        with open(download_path + "/sub_hello/sub_hello.txt") as test_file:
            self.assertEqual("Sub Hello World!\n", test_file.readline())

    def test_add_py_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlibrary import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addPyFile(path)
        from userlibrary import UserClass
        self.assertEqual("Hello World!", UserClass().hello())

    def test_add_egg_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlib import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlib-0.1.zip")
        self.sc.addPyFile(path)
        from userlib import UserClass
        self.assertEqual("Hello World from inside a package!", UserClass().hello())

    def test_overwrite_system_module(self):
        self.sc.addPyFile(os.path.join(SPARK_HOME, "python/test_support/SimpleHTTPServer.py"))

        import SimpleHTTPServer
        self.assertEqual("My Server", SimpleHTTPServer.__name__)

        def func(x):
            import SimpleHTTPServer
            return SimpleHTTPServer.__name__

        self.assertEqual(["My Server"], self.sc.parallelize(range(1)).map(func).collect())


class TaskContextTests(PySparkTestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        # Allow retries even though they are normally disabled in local mode
        self.sc = SparkContext('local[4, 2]', class_name)

    def test_stage_id(self):
        """Test the stage ids are available and incrementing as expected."""
        rdd = self.sc.parallelize(range(10))
        stage1 = rdd.map(lambda x: TaskContext.get().stageId()).take(1)[0]
        stage2 = rdd.map(lambda x: TaskContext.get().stageId()).take(1)[0]
        # Test using the constructor directly rather than the get()
        stage3 = rdd.map(lambda x: TaskContext().stageId()).take(1)[0]
        self.assertEqual(stage1 + 1, stage2)
        self.assertEqual(stage1 + 2, stage3)
        self.assertEqual(stage2 + 1, stage3)

    def test_partition_id(self):
        """Test the partition id."""
        rdd1 = self.sc.parallelize(range(10), 1)
        rdd2 = self.sc.parallelize(range(10), 2)
        pids1 = rdd1.map(lambda x: TaskContext.get().partitionId()).collect()
        pids2 = rdd2.map(lambda x: TaskContext.get().partitionId()).collect()
        self.assertEqual(0, pids1[0])
        self.assertEqual(0, pids1[9])
        self.assertEqual(0, pids2[0])
        self.assertEqual(1, pids2[9])

    def test_attempt_number(self):
        """Verify the attempt numbers are correctly reported."""
        rdd = self.sc.parallelize(range(10))
        # Verify a simple job with no failures
        attempt_numbers = rdd.map(lambda x: TaskContext.get().attemptNumber()).collect()
        map(lambda attempt: self.assertEqual(0, attempt), attempt_numbers)

        def fail_on_first(x):
            """Fail on the first attempt so we get a positive attempt number"""
            tc = TaskContext.get()
            attempt_number = tc.attemptNumber()
            partition_id = tc.partitionId()
            attempt_id = tc.taskAttemptId()
            if attempt_number == 0 and partition_id == 0:
                raise Exception("Failing on first attempt")
            else:
                return [x, partition_id, attempt_number, attempt_id]
        result = rdd.map(fail_on_first).collect()
        # We should re-submit the first partition to it but other partitions should be attempt 0
        self.assertEqual([0, 0, 1], result[0][0:3])
        self.assertEqual([9, 3, 0], result[9][0:3])
        first_partition = filter(lambda x: x[1] == 0, result)
        map(lambda x: self.assertEqual(1, x[2]), first_partition)
        other_partitions = filter(lambda x: x[1] != 0, result)
        map(lambda x: self.assertEqual(0, x[2]), other_partitions)
        # The task attempt id should be different
        self.assertTrue(result[0][3] != result[9][3])

    def test_tc_on_driver(self):
        """Verify that getting the TaskContext on the driver returns None."""
        tc = TaskContext.get()
        self.assertTrue(tc is None)

    def test_get_local_property(self):
        """Verify that local properties set on the driver are available in TaskContext."""
        key = "testkey"
        value = "testvalue"
        self.sc.setLocalProperty(key, value)
        try:
            rdd = self.sc.parallelize(range(1), 1)
            prop1 = rdd.map(lambda _: TaskContext.get().getLocalProperty(key)).collect()[0]
            self.assertEqual(prop1, value)
            prop2 = rdd.map(lambda _: TaskContext.get().getLocalProperty("otherkey")).collect()[0]
            self.assertTrue(prop2 is None)
        finally:
            self.sc.setLocalProperty(key, None)

    def test_barrier(self):
        """
        Verify that BarrierTaskContext.barrier() performs global sync among all barrier tasks
        within a stage.
        """
        rdd = self.sc.parallelize(range(10), 4)

        def f(iterator):
            yield sum(iterator)

        def context_barrier(x):
            tc = BarrierTaskContext.get()
            time.sleep(random.randint(1, 10))
            tc.barrier()
            return time.time()

        times = rdd.barrier().mapPartitions(f).map(context_barrier).collect()
        self.assertTrue(max(times) - min(times) < 1)

    def test_barrier_with_python_worker_reuse(self):
        """
        Verify that BarrierTaskContext.barrier() with reused python worker.
        """
        self.sc._conf.set("spark.python.work.reuse", "true")
        rdd = self.sc.parallelize(range(4), 4)
        # start a normal job first to start all worker
        result = rdd.map(lambda x: x ** 2).collect()
        self.assertEqual([0, 1, 4, 9], result)
        # make sure `spark.python.work.reuse=true`
        self.assertEqual(self.sc._conf.get("spark.python.work.reuse"), "true")

        # worker will be reused in this barrier job
        self.test_barrier()

    def test_barrier_infos(self):
        """
        Verify that BarrierTaskContext.getTaskInfos() returns a list of all task infos in the
        barrier stage.
        """
        rdd = self.sc.parallelize(range(10), 4)

        def f(iterator):
            yield sum(iterator)

        taskInfos = rdd.barrier().mapPartitions(f).map(lambda x: BarrierTaskContext.get()
                                                       .getTaskInfos()).collect()
        self.assertTrue(len(taskInfos) == 4)
        self.assertTrue(len(taskInfos[0]) == 4)


class RDDTests(ReusedPySparkTestCase):

    def test_range(self):
        self.assertEqual(self.sc.range(1, 1).count(), 0)
        self.assertEqual(self.sc.range(1, 0, -1).count(), 1)
        self.assertEqual(self.sc.range(0, 1 << 40, 1 << 39).count(), 2)

    def test_id(self):
        rdd = self.sc.parallelize(range(10))
        id = rdd.id()
        self.assertEqual(id, rdd.id())
        rdd2 = rdd.map(str).filter(bool)
        id2 = rdd2.id()
        self.assertEqual(id + 1, id2)
        self.assertEqual(id2, rdd2.id())

    def test_empty_rdd(self):
        rdd = self.sc.emptyRDD()
        self.assertTrue(rdd.isEmpty())

    def test_sum(self):
        self.assertEqual(0, self.sc.emptyRDD().sum())
        self.assertEqual(6, self.sc.parallelize([1, 2, 3]).sum())

    def test_to_localiterator(self):
        from time import sleep
        rdd = self.sc.parallelize([1, 2, 3])
        it = rdd.toLocalIterator()
        sleep(5)
        self.assertEqual([1, 2, 3], sorted(it))

        rdd2 = rdd.repartition(1000)
        it2 = rdd2.toLocalIterator()
        sleep(5)
        self.assertEqual([1, 2, 3], sorted(it2))

    def test_save_as_textfile_with_unicode(self):
        # Regression test for SPARK-970
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = b''.join(open(p, 'rb').read()
                                for p in glob(tempFile.name + "/part-0000*"))
        self.assertEqual(x, raw_contents.strip().decode("utf-8"))

    def test_save_as_textfile_with_utf8(self):
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x.encode("utf-8")])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = b''.join(open(p, 'rb').read()
                                for p in glob(tempFile.name + "/part-0000*"))
        self.assertEqual(x, raw_contents.strip().decode('utf8'))

    def test_transforming_cartesian_result(self):
        # Regression test for SPARK-1034
        rdd1 = self.sc.parallelize([1, 2])
        rdd2 = self.sc.parallelize([3, 4])
        cart = rdd1.cartesian(rdd2)
        result = cart.map(lambda x_y3: x_y3[0] + x_y3[1]).collect()

    def test_transforming_pickle_file(self):
        # Regression test for SPARK-2601
        data = self.sc.parallelize([u"Hello", u"World!"])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsPickleFile(tempFile.name)
        pickled_file = self.sc.pickleFile(tempFile.name)
        pickled_file.map(lambda x: x).collect()

    def test_cartesian_on_textfile(self):
        # Regression test for
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        a = self.sc.textFile(path)
        result = a.cartesian(a).collect()
        (x, y) = result[0]
        self.assertEqual(u"Hello World!", x.strip())
        self.assertEqual(u"Hello World!", y.strip())

    def test_cartesian_chaining(self):
        # Tests for SPARK-16589
        rdd = self.sc.parallelize(range(10), 2)
        self.assertSetEqual(
            set(rdd.cartesian(rdd).cartesian(rdd).collect()),
            set([((x, y), z) for x in range(10) for y in range(10) for z in range(10)])
        )

        self.assertSetEqual(
            set(rdd.cartesian(rdd.cartesian(rdd)).collect()),
            set([(x, (y, z)) for x in range(10) for y in range(10) for z in range(10)])
        )

        self.assertSetEqual(
            set(rdd.cartesian(rdd.zip(rdd)).collect()),
            set([(x, (y, y)) for x in range(10) for y in range(10)])
        )

    def test_zip_chaining(self):
        # Tests for SPARK-21985
        rdd = self.sc.parallelize('abc', 2)
        self.assertSetEqual(
            set(rdd.zip(rdd).zip(rdd).collect()),
            set([((x, x), x) for x in 'abc'])
        )
        self.assertSetEqual(
            set(rdd.zip(rdd.zip(rdd)).collect()),
            set([(x, (x, x)) for x in 'abc'])
        )

    def test_deleting_input_files(self):
        # Regression test for SPARK-1025
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write(b"Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: filtered_data.count())

    def test_sampling_default_seed(self):
        # Test for SPARK-3995 (default seed setting)
        data = self.sc.parallelize(xrange(1000), 1)
        subset = data.takeSample(False, 10)
        self.assertEqual(len(subset), 10)

    def test_aggregate_mutable_zero_value(self):
        # Test for SPARK-9021; uses aggregate and treeAggregate to build dict
        # representing a counter of ints
        # NOTE: dict is used instead of collections.Counter for Python 2.6
        # compatibility
        from collections import defaultdict

        # Show that single or multiple partitions work
        data1 = self.sc.range(10, numSlices=1)
        data2 = self.sc.range(10, numSlices=2)

        def seqOp(x, y):
            x[y] += 1
            return x

        def comboOp(x, y):
            for key, val in y.items():
                x[key] += val
            return x

        counts1 = data1.aggregate(defaultdict(int), seqOp, comboOp)
        counts2 = data2.aggregate(defaultdict(int), seqOp, comboOp)
        counts3 = data1.treeAggregate(defaultdict(int), seqOp, comboOp, 2)
        counts4 = data2.treeAggregate(defaultdict(int), seqOp, comboOp, 2)

        ground_truth = defaultdict(int, dict((i, 1) for i in range(10)))
        self.assertEqual(counts1, ground_truth)
        self.assertEqual(counts2, ground_truth)
        self.assertEqual(counts3, ground_truth)
        self.assertEqual(counts4, ground_truth)

    def test_aggregate_by_key_mutable_zero_value(self):
        # Test for SPARK-9021; uses aggregateByKey to make a pair RDD that
        # contains lists of all values for each key in the original RDD

        # list(range(...)) for Python 3.x compatibility (can't use * operator
        # on a range object)
        # list(zip(...)) for Python 3.x compatibility (want to parallelize a
        # collection, not a zip object)
        tuples = list(zip(list(range(10))*2, [1]*20))
        # Show that single or multiple partitions work
        data1 = self.sc.parallelize(tuples, 1)
        data2 = self.sc.parallelize(tuples, 2)

        def seqOp(x, y):
            x.append(y)
            return x

        def comboOp(x, y):
            x.extend(y)
            return x

        values1 = data1.aggregateByKey([], seqOp, comboOp).collect()
        values2 = data2.aggregateByKey([], seqOp, comboOp).collect()
        # Sort lists to ensure clean comparison with ground_truth
        values1.sort()
        values2.sort()

        ground_truth = [(i, [1]*2) for i in range(10)]
        self.assertEqual(values1, ground_truth)
        self.assertEqual(values2, ground_truth)

    def test_fold_mutable_zero_value(self):
        # Test for SPARK-9021; uses fold to merge an RDD of dict counters into
        # a single dict
        # NOTE: dict is used instead of collections.Counter for Python 2.6
        # compatibility
        from collections import defaultdict

        counts1 = defaultdict(int, dict((i, 1) for i in range(10)))
        counts2 = defaultdict(int, dict((i, 1) for i in range(3, 8)))
        counts3 = defaultdict(int, dict((i, 1) for i in range(4, 7)))
        counts4 = defaultdict(int, dict((i, 1) for i in range(5, 6)))
        all_counts = [counts1, counts2, counts3, counts4]
        # Show that single or multiple partitions work
        data1 = self.sc.parallelize(all_counts, 1)
        data2 = self.sc.parallelize(all_counts, 2)

        def comboOp(x, y):
            for key, val in y.items():
                x[key] += val
            return x

        fold1 = data1.fold(defaultdict(int), comboOp)
        fold2 = data2.fold(defaultdict(int), comboOp)

        ground_truth = defaultdict(int)
        for counts in all_counts:
            for key, val in counts.items():
                ground_truth[key] += val
        self.assertEqual(fold1, ground_truth)
        self.assertEqual(fold2, ground_truth)

    def test_fold_by_key_mutable_zero_value(self):
        # Test for SPARK-9021; uses foldByKey to make a pair RDD that contains
        # lists of all values for each key in the original RDD

        tuples = [(i, range(i)) for i in range(10)]*2
        # Show that single or multiple partitions work
        data1 = self.sc.parallelize(tuples, 1)
        data2 = self.sc.parallelize(tuples, 2)

        def comboOp(x, y):
            x.extend(y)
            return x

        values1 = data1.foldByKey([], comboOp).collect()
        values2 = data2.foldByKey([], comboOp).collect()
        # Sort lists to ensure clean comparison with ground_truth
        values1.sort()
        values2.sort()

        # list(range(...)) for Python 3.x compatibility
        ground_truth = [(i, list(range(i))*2) for i in range(10)]
        self.assertEqual(values1, ground_truth)
        self.assertEqual(values2, ground_truth)

    def test_aggregate_by_key(self):
        data = self.sc.parallelize([(1, 1), (1, 1), (3, 2), (5, 1), (5, 3)], 2)

        def seqOp(x, y):
            x.add(y)
            return x

        def combOp(x, y):
            x |= y
            return x

        sets = dict(data.aggregateByKey(set(), seqOp, combOp).collect())
        self.assertEqual(3, len(sets))
        self.assertEqual(set([1]), sets[1])
        self.assertEqual(set([2]), sets[3])
        self.assertEqual(set([1, 3]), sets[5])

    def test_itemgetter(self):
        rdd = self.sc.parallelize([range(10)])
        from operator import itemgetter
        self.assertEqual([1], rdd.map(itemgetter(1)).collect())
        self.assertEqual([(2, 3)], rdd.map(itemgetter(2, 3)).collect())

    def test_namedtuple_in_rdd(self):
        from collections import namedtuple
        Person = namedtuple("Person", "id firstName lastName")
        jon = Person(1, "Jon", "Doe")
        jane = Person(2, "Jane", "Doe")
        theDoes = self.sc.parallelize([jon, jane])
        self.assertEqual([jon, jane], theDoes.collect())

    def test_large_broadcast(self):
        N = 10000
        data = [[float(i) for i in range(300)] for i in range(N)]
        bdata = self.sc.broadcast(data)  # 27MB
        m = self.sc.parallelize(range(1), 1).map(lambda x: len(bdata.value)).sum()
        self.assertEqual(N, m)

    def test_unpersist(self):
        N = 1000
        data = [[float(i) for i in range(300)] for i in range(N)]
        bdata = self.sc.broadcast(data)  # 3MB
        bdata.unpersist()
        m = self.sc.parallelize(range(1), 1).map(lambda x: len(bdata.value)).sum()
        self.assertEqual(N, m)
        bdata.destroy()
        try:
            self.sc.parallelize(range(1), 1).map(lambda x: len(bdata.value)).sum()
        except Exception as e:
            pass
        else:
            raise Exception("job should fail after destroy the broadcast")

    def test_multiple_broadcasts(self):
        N = 1 << 21
        b1 = self.sc.broadcast(set(range(N)))  # multiple blocks in JVM
        r = list(range(1 << 15))
        random.shuffle(r)
        s = str(r).encode()
        checksum = hashlib.md5(s).hexdigest()
        b2 = self.sc.broadcast(s)
        r = list(set(self.sc.parallelize(range(10), 10).map(
            lambda x: (len(b1.value), hashlib.md5(b2.value).hexdigest())).collect()))
        self.assertEqual(1, len(r))
        size, csum = r[0]
        self.assertEqual(N, size)
        self.assertEqual(checksum, csum)

        random.shuffle(r)
        s = str(r).encode()
        checksum = hashlib.md5(s).hexdigest()
        b2 = self.sc.broadcast(s)
        r = list(set(self.sc.parallelize(range(10), 10).map(
            lambda x: (len(b1.value), hashlib.md5(b2.value).hexdigest())).collect()))
        self.assertEqual(1, len(r))
        size, csum = r[0]
        self.assertEqual(N, size)
        self.assertEqual(checksum, csum)

    def test_multithread_broadcast_pickle(self):
        import threading

        b1 = self.sc.broadcast(list(range(3)))
        b2 = self.sc.broadcast(list(range(3)))

        def f1():
            return b1.value

        def f2():
            return b2.value

        funcs_num_pickled = {f1: None, f2: None}

        def do_pickle(f, sc):
            command = (f, None, sc.serializer, sc.serializer)
            ser = CloudPickleSerializer()
            ser.dumps(command)

        def process_vars(sc):
            broadcast_vars = list(sc._pickled_broadcast_vars)
            num_pickled = len(broadcast_vars)
            sc._pickled_broadcast_vars.clear()
            return num_pickled

        def run(f, sc):
            do_pickle(f, sc)
            funcs_num_pickled[f] = process_vars(sc)

        # pickle f1, adds b1 to sc._pickled_broadcast_vars in main thread local storage
        do_pickle(f1, self.sc)

        # run all for f2, should only add/count/clear b2 from worker thread local storage
        t = threading.Thread(target=run, args=(f2, self.sc))
        t.start()
        t.join()

        # count number of vars pickled in main thread, only b1 should be counted and cleared
        funcs_num_pickled[f1] = process_vars(self.sc)

        self.assertEqual(funcs_num_pickled[f1], 1)
        self.assertEqual(funcs_num_pickled[f2], 1)
        self.assertEqual(len(list(self.sc._pickled_broadcast_vars)), 0)

    def test_large_closure(self):
        N = 200000
        data = [float(i) for i in xrange(N)]
        rdd = self.sc.parallelize(range(1), 1).map(lambda x: len(data))
        self.assertEqual(N, rdd.first())
        # regression test for SPARK-6886
        self.assertEqual(1, rdd.map(lambda x: (x, 1)).groupByKey().count())

    def test_zip_with_different_serializers(self):
        a = self.sc.parallelize(range(5))
        b = self.sc.parallelize(range(100, 105))
        self.assertEqual(a.zip(b).collect(), [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)])
        a = a._reserialize(BatchedSerializer(PickleSerializer(), 2))
        b = b._reserialize(MarshalSerializer())
        self.assertEqual(a.zip(b).collect(), [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)])
        # regression test for SPARK-4841
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        t = self.sc.textFile(path)
        cnt = t.count()
        self.assertEqual(cnt, t.zip(t).count())
        rdd = t.map(str)
        self.assertEqual(cnt, t.zip(rdd).count())
        # regression test for bug in _reserializer()
        self.assertEqual(cnt, t.zip(rdd).count())

    def test_zip_with_different_object_sizes(self):
        # regress test for SPARK-5973
        a = self.sc.parallelize(xrange(10000)).map(lambda i: '*' * i)
        b = self.sc.parallelize(xrange(10000, 20000)).map(lambda i: '*' * i)
        self.assertEqual(10000, a.zip(b).count())

    def test_zip_with_different_number_of_items(self):
        a = self.sc.parallelize(range(5), 2)
        # different number of partitions
        b = self.sc.parallelize(range(100, 106), 3)
        self.assertRaises(ValueError, lambda: a.zip(b))
        with QuietTest(self.sc):
            # different number of batched items in JVM
            b = self.sc.parallelize(range(100, 104), 2)
            self.assertRaises(Exception, lambda: a.zip(b).count())
            # different number of items in one pair
            b = self.sc.parallelize(range(100, 106), 2)
            self.assertRaises(Exception, lambda: a.zip(b).count())
            # same total number of items, but different distributions
            a = self.sc.parallelize([2, 3], 2).flatMap(range)
            b = self.sc.parallelize([3, 2], 2).flatMap(range)
            self.assertEqual(a.count(), b.count())
            self.assertRaises(Exception, lambda: a.zip(b).count())

    def test_count_approx_distinct(self):
        rdd = self.sc.parallelize(xrange(1000))
        self.assertTrue(950 < rdd.countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(float).countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(str).countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(lambda x: (x, -x)).countApproxDistinct(0.03) < 1050)

        rdd = self.sc.parallelize([i % 20 for i in range(1000)], 7)
        self.assertTrue(18 < rdd.countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(float).countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(str).countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(lambda x: (x, -x)).countApproxDistinct() < 22)

        self.assertRaises(ValueError, lambda: rdd.countApproxDistinct(0.00000001))

    def test_histogram(self):
        # empty
        rdd = self.sc.parallelize([])
        self.assertEqual([0], rdd.histogram([0, 10])[1])
        self.assertEqual([0, 0], rdd.histogram([0, 4, 10])[1])
        self.assertRaises(ValueError, lambda: rdd.histogram(1))

        # out of range
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEqual([0], rdd.histogram([0, 10])[1])
        self.assertEqual([0, 0], rdd.histogram((0, 4, 10))[1])

        # in range with one bucket
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEqual([4], rdd.histogram([0, 10])[1])
        self.assertEqual([3, 1], rdd.histogram([0, 4, 10])[1])

        # in range with one bucket exact match
        self.assertEqual([4], rdd.histogram([1, 4])[1])

        # out of range with two buckets
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEqual([0, 0], rdd.histogram([0, 5, 10])[1])

        # out of range with two uneven buckets
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEqual([0, 0], rdd.histogram([0, 4, 10])[1])

        # in range with two buckets
        rdd = self.sc.parallelize([1, 2, 3, 5, 6])
        self.assertEqual([3, 2], rdd.histogram([0, 5, 10])[1])

        # in range with two bucket and None
        rdd = self.sc.parallelize([1, 2, 3, 5, 6, None, float('nan')])
        self.assertEqual([3, 2], rdd.histogram([0, 5, 10])[1])

        # in range with two uneven buckets
        rdd = self.sc.parallelize([1, 2, 3, 5, 6])
        self.assertEqual([3, 2], rdd.histogram([0, 5, 11])[1])

        # mixed range with two uneven buckets
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.0, 11.01])
        self.assertEqual([4, 3], rdd.histogram([0, 5, 11])[1])

        # mixed range with four uneven buckets
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0, 200.0, 200.1])
        self.assertEqual([4, 2, 1, 3], rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1])

        # mixed range with uneven buckets and NaN
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0,
                                   199.0, 200.0, 200.1, None, float('nan')])
        self.assertEqual([4, 2, 1, 3], rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1])

        # out of range with infinite buckets
        rdd = self.sc.parallelize([10.01, -0.01, float('nan'), float("inf")])
        self.assertEqual([1, 2], rdd.histogram([float('-inf'), 0, float('inf')])[1])

        # invalid buckets
        self.assertRaises(ValueError, lambda: rdd.histogram([]))
        self.assertRaises(ValueError, lambda: rdd.histogram([1]))
        self.assertRaises(ValueError, lambda: rdd.histogram(0))
        self.assertRaises(TypeError, lambda: rdd.histogram({}))

        # without buckets
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEqual(([1, 4], [4]), rdd.histogram(1))

        # without buckets single element
        rdd = self.sc.parallelize([1])
        self.assertEqual(([1, 1], [1]), rdd.histogram(1))

        # without bucket no range
        rdd = self.sc.parallelize([1] * 4)
        self.assertEqual(([1, 1], [4]), rdd.histogram(1))

        # without buckets basic two
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEqual(([1, 2.5, 4], [2, 2]), rdd.histogram(2))

        # without buckets with more requested than elements
        rdd = self.sc.parallelize([1, 2])
        buckets = [1 + 0.2 * i for i in range(6)]
        hist = [1, 0, 0, 0, 1]
        self.assertEqual((buckets, hist), rdd.histogram(5))

        # invalid RDDs
        rdd = self.sc.parallelize([1, float('inf')])
        self.assertRaises(ValueError, lambda: rdd.histogram(2))
        rdd = self.sc.parallelize([float('nan')])
        self.assertRaises(ValueError, lambda: rdd.histogram(2))

        # string
        rdd = self.sc.parallelize(["ab", "ac", "b", "bd", "ef"], 2)
        self.assertEqual([2, 2], rdd.histogram(["a", "b", "c"])[1])
        self.assertEqual((["ab", "ef"], [5]), rdd.histogram(1))
        self.assertRaises(TypeError, lambda: rdd.histogram(2))

    def test_repartitionAndSortWithinPartitions_asc(self):
        rdd = self.sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)], 2)

        repartitioned = rdd.repartitionAndSortWithinPartitions(2, lambda key: key % 2, True)
        partitions = repartitioned.glom().collect()
        self.assertEqual(partitions[0], [(0, 5), (0, 8), (2, 6)])
        self.assertEqual(partitions[1], [(1, 3), (3, 8), (3, 8)])

    def test_repartitionAndSortWithinPartitions_desc(self):
        rdd = self.sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)], 2)

        repartitioned = rdd.repartitionAndSortWithinPartitions(2, lambda key: key % 2, False)
        partitions = repartitioned.glom().collect()
        self.assertEqual(partitions[0], [(2, 6), (0, 5), (0, 8)])
        self.assertEqual(partitions[1], [(3, 8), (3, 8), (1, 3)])

    def test_repartition_no_skewed(self):
        num_partitions = 20
        a = self.sc.parallelize(range(int(1000)), 2)
        l = a.repartition(num_partitions).glom().map(len).collect()
        zeros = len([x for x in l if x == 0])
        self.assertTrue(zeros == 0)
        l = a.coalesce(num_partitions, True).glom().map(len).collect()
        zeros = len([x for x in l if x == 0])
        self.assertTrue(zeros == 0)

    def test_repartition_on_textfile(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        rdd = self.sc.textFile(path)
        result = rdd.repartition(1).collect()
        self.assertEqual(u"Hello World!", result[0])

    def test_distinct(self):
        rdd = self.sc.parallelize((1, 2, 3)*10, 10)
        self.assertEqual(rdd.getNumPartitions(), 10)
        self.assertEqual(rdd.distinct().count(), 3)
        result = rdd.distinct(5)
        self.assertEqual(result.getNumPartitions(), 5)
        self.assertEqual(result.count(), 3)

    def test_external_group_by_key(self):
        self.sc._conf.set("spark.python.worker.memory", "1m")
        N = 200001
        kv = self.sc.parallelize(xrange(N)).map(lambda x: (x % 3, x))
        gkv = kv.groupByKey().cache()
        self.assertEqual(3, gkv.count())
        filtered = gkv.filter(lambda kv: kv[0] == 1)
        self.assertEqual(1, filtered.count())
        self.assertEqual([(1, N // 3)], filtered.mapValues(len).collect())
        self.assertEqual([(N // 3, N // 3)],
                         filtered.values().map(lambda x: (len(x), len(list(x)))).collect())
        result = filtered.collect()[0][1]
        self.assertEqual(N // 3, len(result))
        self.assertTrue(isinstance(result.data, shuffle.ExternalListOfList))

    def test_sort_on_empty_rdd(self):
        self.assertEqual([], self.sc.parallelize(zip([], [])).sortByKey().collect())

    def test_sample(self):
        rdd = self.sc.parallelize(range(0, 100), 4)
        wo = rdd.sample(False, 0.1, 2).collect()
        wo_dup = rdd.sample(False, 0.1, 2).collect()
        self.assertSetEqual(set(wo), set(wo_dup))
        wr = rdd.sample(True, 0.2, 5).collect()
        wr_dup = rdd.sample(True, 0.2, 5).collect()
        self.assertSetEqual(set(wr), set(wr_dup))
        wo_s10 = rdd.sample(False, 0.3, 10).collect()
        wo_s20 = rdd.sample(False, 0.3, 20).collect()
        self.assertNotEqual(set(wo_s10), set(wo_s20))
        wr_s11 = rdd.sample(True, 0.4, 11).collect()
        wr_s21 = rdd.sample(True, 0.4, 21).collect()
        self.assertNotEqual(set(wr_s11), set(wr_s21))

    def test_null_in_rdd(self):
        jrdd = self.sc._jvm.PythonUtils.generateRDDWithNull(self.sc._jsc)
        rdd = RDD(jrdd, self.sc, UTF8Deserializer())
        self.assertEqual([u"a", None, u"b"], rdd.collect())
        rdd = RDD(jrdd, self.sc, NoOpSerializer())
        self.assertEqual([b"a", None, b"b"], rdd.collect())

    def test_multiple_python_java_RDD_conversions(self):
        # Regression test for SPARK-5361
        data = [
            (u'1', {u'director': u'David Lean'}),
            (u'2', {u'director': u'Andrew Dominik'})
        ]
        data_rdd = self.sc.parallelize(data)
        data_java_rdd = data_rdd._to_java_object_rdd()
        data_python_rdd = self.sc._jvm.SerDeUtil.javaToPython(data_java_rdd)
        converted_rdd = RDD(data_python_rdd, self.sc)
        self.assertEqual(2, converted_rdd.count())

        # conversion between python and java RDD threw exceptions
        data_java_rdd = converted_rdd._to_java_object_rdd()
        data_python_rdd = self.sc._jvm.SerDeUtil.javaToPython(data_java_rdd)
        converted_rdd = RDD(data_python_rdd, self.sc)
        self.assertEqual(2, converted_rdd.count())

    def test_narrow_dependency_in_join(self):
        rdd = self.sc.parallelize(range(10)).map(lambda x: (x, x))
        parted = rdd.partitionBy(2)
        self.assertEqual(2, parted.union(parted).getNumPartitions())
        self.assertEqual(rdd.getNumPartitions() + 2, parted.union(rdd).getNumPartitions())
        self.assertEqual(rdd.getNumPartitions() + 2, rdd.union(parted).getNumPartitions())

        tracker = self.sc.statusTracker()

        self.sc.setJobGroup("test1", "test", True)
        d = sorted(parted.join(parted).collect())
        self.assertEqual(10, len(d))
        self.assertEqual((0, (0, 0)), d[0])
        jobId = tracker.getJobIdsForGroup("test1")[0]
        self.assertEqual(2, len(tracker.getJobInfo(jobId).stageIds))

        self.sc.setJobGroup("test2", "test", True)
        d = sorted(parted.join(rdd).collect())
        self.assertEqual(10, len(d))
        self.assertEqual((0, (0, 0)), d[0])
        jobId = tracker.getJobIdsForGroup("test2")[0]
        self.assertEqual(3, len(tracker.getJobInfo(jobId).stageIds))

        self.sc.setJobGroup("test3", "test", True)
        d = sorted(parted.cogroup(parted).collect())
        self.assertEqual(10, len(d))
        self.assertEqual([[0], [0]], list(map(list, d[0][1])))
        jobId = tracker.getJobIdsForGroup("test3")[0]
        self.assertEqual(2, len(tracker.getJobInfo(jobId).stageIds))

        self.sc.setJobGroup("test4", "test", True)
        d = sorted(parted.cogroup(rdd).collect())
        self.assertEqual(10, len(d))
        self.assertEqual([[0], [0]], list(map(list, d[0][1])))
        jobId = tracker.getJobIdsForGroup("test4")[0]
        self.assertEqual(3, len(tracker.getJobInfo(jobId).stageIds))

    # Regression test for SPARK-6294
    def test_take_on_jrdd(self):
        rdd = self.sc.parallelize(xrange(1 << 20)).map(lambda x: str(x))
        rdd._jrdd.first()

    def test_sortByKey_uses_all_partitions_not_only_first_and_last(self):
        # Regression test for SPARK-5969
        seq = [(i * 59 % 101, i) for i in range(101)]  # unsorted sequence
        rdd = self.sc.parallelize(seq)
        for ascending in [True, False]:
            sort = rdd.sortByKey(ascending=ascending, numPartitions=5)
            self.assertEqual(sort.collect(), sorted(seq, reverse=not ascending))
            sizes = sort.glom().map(len).collect()
            for size in sizes:
                self.assertGreater(size, 0)

    def test_pipe_functions(self):
        data = ['1', '2', '3']
        rdd = self.sc.parallelize(data)
        with QuietTest(self.sc):
            self.assertEqual([], rdd.pipe('cc').collect())
            self.assertRaises(Py4JJavaError, rdd.pipe('cc', checkCode=True).collect)
        result = rdd.pipe('cat').collect()
        result.sort()
        for x, y in zip(data, result):
            self.assertEqual(x, y)
        self.assertRaises(Py4JJavaError, rdd.pipe('grep 4', checkCode=True).collect)
        self.assertEqual([], rdd.pipe('grep 4').collect())

    def test_pipe_unicode(self):
        # Regression test for SPARK-20947
        data = [u'\u6d4b\u8bd5', '1']
        rdd = self.sc.parallelize(data)
        result = rdd.pipe('cat').collect()
        self.assertEqual(data, result)

    def test_stopiteration_in_user_code(self):

        def stopit(*x):
            raise StopIteration()

        seq_rdd = self.sc.parallelize(range(10))
        keyed_rdd = self.sc.parallelize((x % 2, x) for x in range(10))
        msg = "Caught StopIteration thrown from user's code; failing the task"

        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.map(stopit).collect)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.filter(stopit).collect)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.foreach, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.reduce, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.fold, 0, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.foreach, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg,
                                seq_rdd.cartesian(seq_rdd).flatMap(stopit).collect)

        # these methods call the user function both in the driver and in the executor
        # the exception raised is different according to where the StopIteration happens
        # RuntimeError is raised if in the driver
        # Py4JJavaError is raised if in the executor (wraps the RuntimeError raised in the worker)
        self.assertRaisesRegexp((Py4JJavaError, RuntimeError), msg,
                                keyed_rdd.reduceByKeyLocally, stopit)
        self.assertRaisesRegexp((Py4JJavaError, RuntimeError), msg,
                                seq_rdd.aggregate, 0, stopit, lambda *x: 1)
        self.assertRaisesRegexp((Py4JJavaError, RuntimeError), msg,
                                seq_rdd.aggregate, 0, lambda *x: 1, stopit)


class ProfilerTests(PySparkTestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile", "true")
        self.sc = SparkContext('local[4]', class_name, conf=conf)

    def test_profiler(self):
        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        id, profiler, _ = profilers[0]
        stats = profiler.stats()
        self.assertTrue(stats is not None)
        width, stat_list = stats.get_print_list([])
        func_names = [func_name for fname, n, func_name in stat_list]
        self.assertTrue("heavy_foo" in func_names)

        old_stdout = sys.stdout
        sys.stdout = io = StringIO()
        self.sc.show_profiles()
        self.assertTrue("heavy_foo" in io.getvalue())
        sys.stdout = old_stdout

        d = tempfile.gettempdir()
        self.sc.dump_profiles(d)
        self.assertTrue("rdd_%d.pstats" % id in os.listdir(d))

    def test_custom_profiler(self):
        class TestCustomProfiler(BasicProfiler):
            def show(self, id):
                self.result = "Custom formatting"

        self.sc.profiler_collector.profiler_cls = TestCustomProfiler

        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        _, profiler, _ = profilers[0]
        self.assertTrue(isinstance(profiler, TestCustomProfiler))

        self.sc.show_profiles()
        self.assertEqual("Custom formatting", profiler.result)

    def do_computation(self):
        def heavy_foo(x):
            for i in range(1 << 18):
                x = 1

        rdd = self.sc.parallelize(range(100))
        rdd.foreach(heavy_foo)


class ProfilerTests2(unittest.TestCase):
    def test_profiler_disabled(self):
        sc = SparkContext(conf=SparkConf().set("spark.python.profile", "false"))
        try:
            self.assertRaisesRegexp(
                RuntimeError,
                "'spark.python.profile' configuration must be set",
                lambda: sc.show_profiles())
            self.assertRaisesRegexp(
                RuntimeError,
                "'spark.python.profile' configuration must be set",
                lambda: sc.dump_profiles("/tmp/abc"))
        finally:
            sc.stop()


class InputFormatTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.sc._jvm.WriteInputFormatTestDataGenerator.generateData(cls.tempdir.name, cls.sc._jsc)

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name)

    @unittest.skipIf(sys.version >= "3", "serialize array of byte")
    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfint/",
                                           "org.apache.hadoop.io.IntWritable",
                                           "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        doubles = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfdouble/",
                                              "org.apache.hadoop.io.DoubleWritable",
                                              "org.apache.hadoop.io.Text").collect())
        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.assertEqual(doubles, ed)

        bytes = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbytes/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BytesWritable").collect())
        ebs = [(1, bytearray('aa', 'utf-8')),
               (1, bytearray('aa', 'utf-8')),
               (2, bytearray('aa', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (3, bytearray('cc', 'utf-8'))]
        self.assertEqual(bytes, ebs)

        text = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sftext/",
                                           "org.apache.hadoop.io.Text",
                                           "org.apache.hadoop.io.Text").collect())
        et = [(u'1', u'aa'),
              (u'1', u'aa'),
              (u'2', u'aa'),
              (u'2', u'bb'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.assertEqual(text, et)

        bools = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbool/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.assertEqual(bools, eb)

        nulls = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfnull/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.assertEqual(nulls, en)

        maps = self.sc.sequenceFile(basepath + "/sftestdata/sfmap/",
                                    "org.apache.hadoop.io.IntWritable",
                                    "org.apache.hadoop.io.MapWritable").collect()
        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        for v in maps:
            self.assertTrue(v in em)

        # arrays get pickled to tuples by default
        tuples = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable").collect())
        et = [(1, ()),
              (2, (3.0, 4.0, 5.0)),
              (3, (4.0, 5.0, 6.0))]
        self.assertEqual(tuples, et)

        # with custom converters, primitive arrays can stay as arrays
        arrays = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        ea = [(1, array('d')),
              (2, array('d', [3.0, 4.0, 5.0])),
              (3, array('d', [4.0, 5.0, 6.0]))]
        self.assertEqual(arrays, ea)

        clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                            "org.apache.hadoop.io.Text",
                                            "org.apache.spark.api.python.TestWritable").collect())
        cname = u'org.apache.spark.api.python.TestWritable'
        ec = [(u'1', {u'__class__': cname, u'double': 1.0, u'int': 1, u'str': u'test1'}),
              (u'2', {u'__class__': cname, u'double': 2.3, u'int': 2, u'str': u'test2'}),
              (u'3', {u'__class__': cname, u'double': 3.1, u'int': 3, u'str': u'test3'}),
              (u'4', {u'__class__': cname, u'double': 4.2, u'int': 4, u'str': u'test4'}),
              (u'5', {u'__class__': cname, u'double': 5.5, u'int': 5, u'str': u'test56'})]
        self.assertEqual(clazz, ec)

        unbatched_clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                                      "org.apache.hadoop.io.Text",
                                                      "org.apache.spark.api.python.TestWritable",
                                                      ).collect())
        self.assertEqual(unbatched_clazz, ec)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.hadoopFile(basepath + "/sftestdata/sfint/",
                                         "org.apache.hadoop.mapred.SequenceFileInputFormat",
                                         "org.apache.hadoop.io.IntWritable",
                                         "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        oldconf = {"mapreduce.input.fileinputformat.inputdir": hellopath}
        hello = self.sc.hadoopRDD("org.apache.hadoop.mapred.TextInputFormat",
                                  "org.apache.hadoop.io.LongWritable",
                                  "org.apache.hadoop.io.Text",
                                  conf=oldconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        newconf = {"mapreduce.input.fileinputformat.inputdir": hellopath}
        hello = self.sc.newAPIHadoopRDD("org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                        "org.apache.hadoop.io.LongWritable",
                                        "org.apache.hadoop.io.Text",
                                        conf=newconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newolderror(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.sequenceFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.io.NotValidWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        maps = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfmap/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            keyConverter="org.apache.spark.api.python.TestInputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestInputValueConverter").collect())
        em = [(u'\x01', []),
              (u'\x01', [3.0]),
              (u'\x02', [1.0]),
              (u'\x02', [1.0]),
              (u'\x03', [2.0])]
        self.assertEqual(maps, em)

    def test_binary_files(self):
        path = os.path.join(self.tempdir.name, "binaryfiles")
        os.mkdir(path)
        data = b"short binary data"
        with open(os.path.join(path, "part-0000"), 'wb') as f:
            f.write(data)
        [(p, d)] = self.sc.binaryFiles(path).collect()
        self.assertTrue(p.endswith("part-0000"))
        self.assertEqual(d, data)

    def test_binary_records(self):
        path = os.path.join(self.tempdir.name, "binaryrecords")
        os.mkdir(path)
        with open(os.path.join(path, "part-0000"), 'w') as f:
            for i in range(100):
                f.write('%04d' % i)
        result = self.sc.binaryRecords(path, 4).map(int).collect()
        self.assertEqual(list(range(100)), result)


class OutputFormatTests(ReusedPySparkTestCase):

    def setUp(self):
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)

    def tearDown(self):
        shutil.rmtree(self.tempdir.name, ignore_errors=True)

    @unittest.skipIf(sys.version >= "3", "serialize array of byte")
    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.sc.parallelize(ei).saveAsSequenceFile(basepath + "/sfint/")
        ints = sorted(self.sc.sequenceFile(basepath + "/sfint/").collect())
        self.assertEqual(ints, ei)

        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.sc.parallelize(ed).saveAsSequenceFile(basepath + "/sfdouble/")
        doubles = sorted(self.sc.sequenceFile(basepath + "/sfdouble/").collect())
        self.assertEqual(doubles, ed)

        ebs = [(1, bytearray(b'\x00\x07spam\x08')), (2, bytearray(b'\x00\x07spam\x08'))]
        self.sc.parallelize(ebs).saveAsSequenceFile(basepath + "/sfbytes/")
        bytes = sorted(self.sc.sequenceFile(basepath + "/sfbytes/").collect())
        self.assertEqual(bytes, ebs)

        et = [(u'1', u'aa'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.sc.parallelize(et).saveAsSequenceFile(basepath + "/sftext/")
        text = sorted(self.sc.sequenceFile(basepath + "/sftext/").collect())
        self.assertEqual(text, et)

        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.sc.parallelize(eb).saveAsSequenceFile(basepath + "/sfbool/")
        bools = sorted(self.sc.sequenceFile(basepath + "/sfbool/").collect())
        self.assertEqual(bools, eb)

        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.sc.parallelize(en).saveAsSequenceFile(basepath + "/sfnull/")
        nulls = sorted(self.sc.sequenceFile(basepath + "/sfnull/").collect())
        self.assertEqual(nulls, en)

        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        self.sc.parallelize(em).saveAsSequenceFile(basepath + "/sfmap/")
        maps = self.sc.sequenceFile(basepath + "/sfmap/").collect()
        for v in maps:
            self.assertTrue(v, em)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        dict_data = [(1, {}),
                     (1, {"row1": 1.0}),
                     (2, {"row2": 2.0})]
        self.sc.parallelize(dict_data).saveAsHadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable")
        result = self.sc.hadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable").collect()
        for v in result:
            self.assertTrue(v, dict_data)

        conf = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.MapWritable",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/olddataset/"
        }
        self.sc.parallelize(dict_data).saveAsHadoopDataset(conf)
        input_conf = {"mapreduce.input.fileinputformat.inputdir": basepath + "/olddataset/"}
        result = self.sc.hadoopRDD(
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            conf=input_conf).collect()
        for v in result:
            self.assertTrue(v, dict_data)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        data = [(1, ""),
                (1, "a"),
                (2, "bcdf")]
        self.sc.parallelize(data).saveAsNewAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text")
        result = sorted(self.sc.newAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text").collect())
        self.assertEqual(result, data)

        conf = {
            "mapreduce.job.outputformat.class":
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Text",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/newdataset/"
        }
        self.sc.parallelize(data).saveAsNewAPIHadoopDataset(conf)
        input_conf = {"mapreduce.input.fileinputformat.inputdir": basepath + "/newdataset/"}
        new_dataset = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text",
            conf=input_conf).collect())
        self.assertEqual(new_dataset, data)

    @unittest.skipIf(sys.version >= "3", "serialize of array")
    def test_newhadoop_with_array(self):
        basepath = self.tempdir.name
        # use custom ArrayWritable types and converters to handle arrays
        array_data = [(1, array('d')),
                      (1, array('d', [1.0, 2.0, 3.0])),
                      (2, array('d', [3.0, 4.0, 5.0]))]
        self.sc.parallelize(array_data).saveAsNewAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        result = sorted(self.sc.newAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        self.assertEqual(result, array_data)

        conf = {
            "mapreduce.job.outputformat.class":
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.spark.api.python.DoubleArrayWritable",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/newdataset/"
        }
        self.sc.parallelize(array_data).saveAsNewAPIHadoopDataset(
            conf,
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        input_conf = {"mapreduce.input.fileinputformat.inputdir": basepath + "/newdataset/"}
        new_dataset = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter",
            conf=input_conf).collect())
        self.assertEqual(new_dataset, array_data)

    def test_newolderror(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/newolderror/saveAsHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/newolderror/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/badinputs/saveAsHadoopFile/",
            "org.apache.hadoop.mapred.NotValidOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/badinputs/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.NotValidOutputFormat"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        data = [(1, {3.0: u'bb'}),
                (2, {1.0: u'aa'}),
                (3, {2.0: u'dd'})]
        self.sc.parallelize(data).saveAsNewAPIHadoopFile(
            basepath + "/converters/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            keyConverter="org.apache.spark.api.python.TestOutputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestOutputValueConverter")
        converted = sorted(self.sc.sequenceFile(basepath + "/converters/").collect())
        expected = [(u'1', 3.0),
                    (u'2', 1.0),
                    (u'3', 2.0)]
        self.assertEqual(converted, expected)

    def test_reserialization(self):
        basepath = self.tempdir.name
        x = range(1, 5)
        y = range(1001, 1005)
        data = list(zip(x, y))
        rdd = self.sc.parallelize(x).zip(self.sc.parallelize(y))
        rdd.saveAsSequenceFile(basepath + "/reserialize/sequence")
        result1 = sorted(self.sc.sequenceFile(basepath + "/reserialize/sequence").collect())
        self.assertEqual(result1, data)

        rdd.saveAsHadoopFile(
            basepath + "/reserialize/hadoop",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat")
        result2 = sorted(self.sc.sequenceFile(basepath + "/reserialize/hadoop").collect())
        self.assertEqual(result2, data)

        rdd.saveAsNewAPIHadoopFile(
            basepath + "/reserialize/newhadoop",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
        result3 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newhadoop").collect())
        self.assertEqual(result3, data)

        conf4 = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.output.fileoutputformat.outputdir": basepath + "/reserialize/dataset"}
        rdd.saveAsHadoopDataset(conf4)
        result4 = sorted(self.sc.sequenceFile(basepath + "/reserialize/dataset").collect())
        self.assertEqual(result4, data)

        conf5 = {"mapreduce.job.outputformat.class":
                 "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                 "mapreduce.job.output.key.class": "org.apache.hadoop.io.IntWritable",
                 "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
                 "mapreduce.output.fileoutputformat.outputdir": basepath + "/reserialize/newdataset"
                 }
        rdd.saveAsNewAPIHadoopDataset(conf5)
        result5 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newdataset").collect())
        self.assertEqual(result5, data)

    def test_malformed_RDD(self):
        basepath = self.tempdir.name
        # non-batch-serialized RDD[[(K, V)]] should be rejected
        data = [[(1, "a")], [(2, "aa")], [(3, "aaa")]]
        rdd = self.sc.parallelize(data, len(data))
        self.assertRaises(Exception, lambda: rdd.saveAsSequenceFile(
            basepath + "/malformed/sequence"))


class DaemonTests(unittest.TestCase):
    def connect(self, port):
        from socket import socket, AF_INET, SOCK_STREAM
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(('127.0.0.1', port))
        # send a split index of -1 to shutdown the worker
        sock.send(b"\xFF\xFF\xFF\xFF")
        sock.close()
        return True

    def do_termination_test(self, terminator):
        from subprocess import Popen, PIPE
        from errno import ECONNREFUSED

        # start daemon
        daemon_path = os.path.join(os.path.dirname(__file__), "daemon.py")
        python_exec = sys.executable or os.environ.get("PYSPARK_PYTHON")
        daemon = Popen([python_exec, daemon_path], stdin=PIPE, stdout=PIPE)

        # read the port number
        port = read_int(daemon.stdout)

        # daemon should accept connections
        self.assertTrue(self.connect(port))

        # wait worker process spawned from daemon exit.
        time.sleep(1)

        # request shutdown
        terminator(daemon)
        time.sleep(1)

        # daemon should no longer accept connections
        try:
            self.connect(port)
        except EnvironmentError as exception:
            self.assertEqual(exception.errno, ECONNREFUSED)
        else:
            self.fail("Expected EnvironmentError to be raised")

    def test_termination_stdin(self):
        """Ensure that daemon and workers terminate when stdin is closed."""
        self.do_termination_test(lambda daemon: daemon.stdin.close())

    def test_termination_sigterm(self):
        """Ensure that daemon and workers terminate on SIGTERM."""
        from signal import SIGTERM
        self.do_termination_test(lambda daemon: os.kill(daemon.pid, SIGTERM))


class WorkerTests(ReusedPySparkTestCase):
    def test_cancel_task(self):
        temp = tempfile.NamedTemporaryFile(delete=True)
        temp.close()
        path = temp.name

        def sleep(x):
            import os
            import time
            with open(path, 'w') as f:
                f.write("%d %d" % (os.getppid(), os.getpid()))
            time.sleep(100)

        # start job in background thread
        def run():
            try:
                self.sc.parallelize(range(1), 1).foreach(sleep)
            except Exception:
                pass
        import threading
        t = threading.Thread(target=run)
        t.daemon = True
        t.start()

        daemon_pid, worker_pid = 0, 0
        while True:
            if os.path.exists(path):
                with open(path) as f:
                    data = f.read().split(' ')
                daemon_pid, worker_pid = map(int, data)
                break
            time.sleep(0.1)

        # cancel jobs
        self.sc.cancelAllJobs()
        t.join()

        for i in range(50):
            try:
                os.kill(worker_pid, 0)
                time.sleep(0.1)
            except OSError:
                break  # worker was killed
        else:
            self.fail("worker has not been killed after 5 seconds")

        try:
            os.kill(daemon_pid, 0)
        except OSError:
            self.fail("daemon had been killed")

        # run a normal job
        rdd = self.sc.parallelize(xrange(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_after_exception(self):
        def raise_exception(_):
            raise Exception()
        rdd = self.sc.parallelize(xrange(100), 1)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: rdd.foreach(raise_exception))
        self.assertEqual(100, rdd.map(str).count())

    def test_after_jvm_exception(self):
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write(b"Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name, 1)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: filtered_data.count())

        rdd = self.sc.parallelize(xrange(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_accumulator_when_reuse_worker(self):
        from pyspark.accumulators import INT_ACCUMULATOR_PARAM
        acc1 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(xrange(100), 20).foreach(lambda x: acc1.add(x))
        self.assertEqual(sum(range(100)), acc1.value)

        acc2 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(xrange(100), 20).foreach(lambda x: acc2.add(x))
        self.assertEqual(sum(range(100)), acc2.value)
        self.assertEqual(sum(range(100)), acc1.value)

    def test_reuse_worker_after_take(self):
        rdd = self.sc.parallelize(xrange(100000), 1)
        self.assertEqual(0, rdd.first())

        def count():
            try:
                rdd.count()
            except Exception:
                pass

        t = threading.Thread(target=count)
        t.daemon = True
        t.start()
        t.join(5)
        self.assertTrue(not t.isAlive())
        self.assertEqual(100000, rdd.count())

    def test_with_different_versions_of_python(self):
        rdd = self.sc.parallelize(range(10))
        rdd.count()
        version = self.sc.pythonVer
        self.sc.pythonVer = "2.0"
        try:
            with QuietTest(self.sc):
                self.assertRaises(Py4JJavaError, lambda: rdd.count())
        finally:
            self.sc.pythonVer = version


class SparkSubmitTests(unittest.TestCase):

    def setUp(self):
        self.programDir = tempfile.mkdtemp()
        tmp_dir = tempfile.gettempdir()
        self.sparkSubmit = [
            os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit"),
            "--conf", "spark.driver.extraJavaOptions=-Djava.io.tmpdir={0}".format(tmp_dir),
            "--conf", "spark.executor.extraJavaOptions=-Djava.io.tmpdir={0}".format(tmp_dir),
        ]

    def tearDown(self):
        shutil.rmtree(self.programDir)

    def createTempFile(self, name, content, dir=None):
        """
        Create a temp file with the given name and content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        if dir is None:
            path = os.path.join(self.programDir, name)
        else:
            os.makedirs(os.path.join(self.programDir, dir))
            path = os.path.join(self.programDir, dir, name)
        with open(path, "w") as f:
            f.write(content)
        return path

    def createFileInZip(self, name, content, ext=".zip", dir=None, zip_name=None):
        """
        Create a zip archive containing a file with the given content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        if dir is None:
            path = os.path.join(self.programDir, name + ext)
        else:
            path = os.path.join(self.programDir, dir, zip_name + ext)
        zip = zipfile.ZipFile(path, 'w')
        zip.writestr(name, content)
        zip.close()
        return path

    def create_spark_package(self, artifact_name):
        group_id, artifact_id, version = artifact_name.split(":")
        self.createTempFile("%s-%s.pom" % (artifact_id, version), ("""
            |<?xml version="1.0" encoding="UTF-8"?>
            |<project xmlns="http://maven.apache.org/POM/4.0.0"
            |       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            |       xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
            |       http://maven.apache.org/xsd/maven-4.0.0.xsd">
            |   <modelVersion>4.0.0</modelVersion>
            |   <groupId>%s</groupId>
            |   <artifactId>%s</artifactId>
            |   <version>%s</version>
            |</project>
            """ % (group_id, artifact_id, version)).lstrip(),
            os.path.join(group_id, artifact_id, version))
        self.createFileInZip("%s.py" % artifact_id, """
            |def myfunc(x):
            |    return x + 1
            """, ".jar", os.path.join(group_id, artifact_id, version),
                             "%s-%s" % (artifact_id, version))

    def test_single_script(self):
        """Submit and test a single script file"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect())
            """)
        proc = subprocess.Popen(self.sparkSubmit + [script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out.decode('utf-8'))

    def test_script_with_local_functions(self):
        """Submit and test a single script file calling a global function"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 3
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(foo).collect())
            """)
        proc = subprocess.Popen(self.sparkSubmit + [script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[3, 6, 9]", out.decode('utf-8'))

    def test_module_dependency(self):
        """Submit and test a script with a dependency on another module"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen(self.sparkSubmit + ["--py-files", zip, script],
                                stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode('utf-8'))

    def test_module_dependency_on_cluster(self):
        """Submit and test a script with a dependency on another module on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen(self.sparkSubmit + ["--py-files", zip, "--master",
                                "local-cluster[1,1,1024]", script],
                                stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode('utf-8'))

    def test_package_dependency(self):
        """Submit and test a script with a dependency on a Spark Package"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """)
        self.create_spark_package("a:mylib:0.1")
        proc = subprocess.Popen(
            self.sparkSubmit + ["--packages", "a:mylib:0.1", "--repositories",
                                "file:" + self.programDir, script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode('utf-8'))

    def test_package_dependency_on_cluster(self):
        """Submit and test a script with a dependency on a Spark Package on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(myfunc).collect())
            """)
        self.create_spark_package("a:mylib:0.1")
        proc = subprocess.Popen(
            self.sparkSubmit + ["--packages", "a:mylib:0.1", "--repositories",
                                "file:" + self.programDir, "--master", "local-cluster[1,1,1024]",
                                script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out.decode('utf-8'))

    def test_single_script_on_cluster(self):
        """Submit and test a single script on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 2
            |
            |sc = SparkContext()
            |print(sc.parallelize([1, 2, 3]).map(foo).collect())
            """)
        # this will fail if you have different spark.executor.memory
        # in conf/spark-defaults.conf
        proc = subprocess.Popen(
            self.sparkSubmit + ["--master", "local-cluster[1,1,1024]", script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out.decode('utf-8'))

    def test_user_configuration(self):
        """Make sure user configuration is respected (SPARK-19307)"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkConf, SparkContext
            |
            |conf = SparkConf().set("spark.test_config", "1")
            |sc = SparkContext(conf = conf)
            |try:
            |    if sc._conf.get("spark.test_config") != "1":
            |        raise Exception("Cannot find spark.test_config in SparkContext's conf.")
            |finally:
            |    sc.stop()
            """)
        proc = subprocess.Popen(
            self.sparkSubmit + ["--master", "local", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode, msg="Process failed with error:\n {0}".format(out))


class ContextTests(unittest.TestCase):

    def test_failed_sparkcontext_creation(self):
        # Regression test for SPARK-1550
        self.assertRaises(Exception, lambda: SparkContext("an-invalid-master-name"))

    def test_get_or_create(self):
        with SparkContext.getOrCreate() as sc:
            self.assertTrue(SparkContext.getOrCreate() is sc)

    def test_parallelize_eager_cleanup(self):
        with SparkContext() as sc:
            temp_files = os.listdir(sc._temp_dir)
            rdd = sc.parallelize([0, 1, 2])
            post_parallalize_temp_files = os.listdir(sc._temp_dir)
            self.assertEqual(temp_files, post_parallalize_temp_files)

    def test_set_conf(self):
        # This is for an internal use case. When there is an existing SparkContext,
        # SparkSession's builder needs to set configs into SparkContext's conf.
        sc = SparkContext()
        sc._conf.set("spark.test.SPARK16224", "SPARK16224")
        self.assertEqual(sc._jsc.sc().conf().get("spark.test.SPARK16224"), "SPARK16224")
        sc.stop()

    def test_stop(self):
        sc = SparkContext()
        self.assertNotEqual(SparkContext._active_spark_context, None)
        sc.stop()
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with(self):
        with SparkContext() as sc:
            self.assertNotEqual(SparkContext._active_spark_context, None)
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with_exception(self):
        try:
            with SparkContext() as sc:
                self.assertNotEqual(SparkContext._active_spark_context, None)
                raise Exception()
        except:
            pass
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with_stop(self):
        with SparkContext() as sc:
            self.assertNotEqual(SparkContext._active_spark_context, None)
            sc.stop()
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_progress_api(self):
        with SparkContext() as sc:
            sc.setJobGroup('test_progress_api', '', True)
            rdd = sc.parallelize(range(10)).map(lambda x: time.sleep(100))

            def run():
                try:
                    rdd.count()
                except Exception:
                    pass
            t = threading.Thread(target=run)
            t.daemon = True
            t.start()
            # wait for scheduler to start
            time.sleep(1)

            tracker = sc.statusTracker()
            jobIds = tracker.getJobIdsForGroup('test_progress_api')
            self.assertEqual(1, len(jobIds))
            job = tracker.getJobInfo(jobIds[0])
            self.assertEqual(1, len(job.stageIds))
            stage = tracker.getStageInfo(job.stageIds[0])
            self.assertEqual(rdd.getNumPartitions(), stage.numTasks)

            sc.cancelAllJobs()
            t.join()
            # wait for event listener to update the status
            time.sleep(1)

            job = tracker.getJobInfo(jobIds[0])
            self.assertEqual('FAILED', job.status)
            self.assertEqual([], tracker.getActiveJobsIds())
            self.assertEqual([], tracker.getActiveStageIds())

            sc.stop()

    def test_startTime(self):
        with SparkContext() as sc:
            self.assertGreater(sc.startTime, 0)

    def test_forbid_insecure_gateway(self):
        # By default, we fail immediately if you try to create a SparkContext
        # with an insecure gateway
        gateway = _launch_gateway(insecure=True)
        log4j = gateway.jvm.org.apache.log4j
        old_level = log4j.LogManager.getRootLogger().getLevel()
        try:
            log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)
            with self.assertRaises(Exception) as context:
                SparkContext(gateway=gateway)
            self.assertIn("insecure Py4j gateway", str(context.exception))
            self.assertIn("PYSPARK_ALLOW_INSECURE_GATEWAY", str(context.exception))
            self.assertIn("removed in Spark 3.0", str(context.exception))
        finally:
            log4j.LogManager.getRootLogger().setLevel(old_level)

    def test_allow_insecure_gateway_with_conf(self):
        with SparkContext._lock:
            SparkContext._gateway = None
            SparkContext._jvm = None
        gateway = _launch_gateway(insecure=True)
        try:
            os.environ["PYSPARK_ALLOW_INSECURE_GATEWAY"] = "1"
            with SparkContext(gateway=gateway) as sc:
                a = sc.accumulator(1)
                rdd = sc.parallelize([1, 2, 3])
                rdd.foreach(lambda x: a.add(x))
                self.assertEqual(7, a.value)
        finally:
            os.environ.pop("PYSPARK_ALLOW_INSECURE_GATEWAY", None)


class ConfTests(unittest.TestCase):
    def test_memory_conf(self):
        memoryList = ["1T", "1G", "1M", "1024K"]
        for memory in memoryList:
            sc = SparkContext(conf=SparkConf().set("spark.python.worker.memory", memory))
            l = list(range(1024))
            random.shuffle(l)
            rdd = sc.parallelize(l, 4)
            self.assertEqual(sorted(l), rdd.sortBy(lambda x: x).collect())
            sc.stop()


class KeywordOnlyTests(unittest.TestCase):
    class Wrapped(object):
        @keyword_only
        def set(self, x=None, y=None):
            if "x" in self._input_kwargs:
                self._x = self._input_kwargs["x"]
            if "y" in self._input_kwargs:
                self._y = self._input_kwargs["y"]
            return x, y

    def test_keywords(self):
        w = self.Wrapped()
        x, y = w.set(y=1)
        self.assertEqual(y, 1)
        self.assertEqual(y, w._y)
        self.assertIsNone(x)
        self.assertFalse(hasattr(w, "_x"))

    def test_non_keywords(self):
        w = self.Wrapped()
        self.assertRaises(TypeError, lambda: w.set(0, y=1))

    def test_kwarg_ownership(self):
        # test _input_kwargs is owned by each class instance and not a shared static variable
        class Setter(object):
            @keyword_only
            def set(self, x=None, other=None, other_x=None):
                if "other" in self._input_kwargs:
                    self._input_kwargs["other"].set(x=self._input_kwargs["other_x"])
                self._x = self._input_kwargs["x"]

        a = Setter()
        b = Setter()
        a.set(x=1, other=b, other_x=2)
        self.assertEqual(a._x, 1)
        self.assertEqual(b._x, 2)


class UtilTests(PySparkTestCase):
    def test_py4j_exception_message(self):
        from pyspark.util import _exception_message

        with self.assertRaises(Py4JJavaError) as context:
            # This attempts java.lang.String(null) which throws an NPE.
            self.sc._jvm.java.lang.String(None)

        self.assertTrue('NullPointerException' in _exception_message(context.exception))

    def test_parsing_version_string(self):
        from pyspark.util import VersionUtils
        self.assertRaises(ValueError, lambda: VersionUtils.majorMinorVersion("abced"))


@unittest.skipIf(not _have_scipy, "SciPy not installed")
class SciPyTests(PySparkTestCase):

    """General PySpark tests that depend on scipy """

    def test_serialize(self):
        from scipy.special import gammaln
        x = range(1, 5)
        expected = list(map(gammaln, x))
        observed = self.sc.parallelize(x).map(gammaln).collect()
        self.assertEqual(expected, observed)


@unittest.skipIf(not _have_numpy, "NumPy not installed")
class NumPyTests(PySparkTestCase):

    """General PySpark tests that depend on numpy """

    def test_statcounter_array(self):
        x = self.sc.parallelize([np.array([1.0, 1.0]), np.array([2.0, 2.0]), np.array([3.0, 3.0])])
        s = x.stats()
        self.assertSequenceEqual([2.0, 2.0], s.mean().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.min().tolist())
        self.assertSequenceEqual([3.0, 3.0], s.max().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.sampleStdev().tolist())

        stats_dict = s.asDict()
        self.assertEqual(3, stats_dict['count'])
        self.assertSequenceEqual([2.0, 2.0], stats_dict['mean'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_dict['min'].tolist())
        self.assertSequenceEqual([3.0, 3.0], stats_dict['max'].tolist())
        self.assertSequenceEqual([6.0, 6.0], stats_dict['sum'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_dict['stdev'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_dict['variance'].tolist())

        stats_sample_dict = s.asDict(sample=True)
        self.assertEqual(3, stats_dict['count'])
        self.assertSequenceEqual([2.0, 2.0], stats_sample_dict['mean'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_sample_dict['min'].tolist())
        self.assertSequenceEqual([3.0, 3.0], stats_sample_dict['max'].tolist())
        self.assertSequenceEqual([6.0, 6.0], stats_sample_dict['sum'].tolist())
        self.assertSequenceEqual(
            [0.816496580927726, 0.816496580927726], stats_sample_dict['stdev'].tolist())
        self.assertSequenceEqual(
            [0.6666666666666666, 0.6666666666666666], stats_sample_dict['variance'].tolist())


if __name__ == "__main__":
    from pyspark.tests import *
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'), verbosity=2)
    else:
        unittest.main(verbosity=2)

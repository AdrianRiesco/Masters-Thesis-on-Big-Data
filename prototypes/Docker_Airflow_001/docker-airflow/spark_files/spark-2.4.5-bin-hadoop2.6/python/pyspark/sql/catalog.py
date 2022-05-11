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

import sys
import warnings
from collections import namedtuple

from pyspark import since
from pyspark.rdd import ignore_unicode_prefix, PythonEvalType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.udf import UserDefinedFunction
from pyspark.sql.types import IntegerType, StringType, StructType


Database = namedtuple("Database", "name description locationUri")
Table = namedtuple("Table", "name database description tableType isTemporary")
Column = namedtuple("Column", "name description dataType nullable isPartition isBucket")
Function = namedtuple("Function", "name description className isTemporary")


class Catalog(object):
    """User-facing catalog API, accessible through `SparkSession.catalog`.

    This is a thin wrapper around its Scala implementation org.apache.spark.sql.catalog.Catalog.
    """

    def __init__(self, sparkSession):
        """Create a new Catalog that wraps the underlying JVM object."""
        self._sparkSession = sparkSession
        self._jsparkSession = sparkSession._jsparkSession
        self._jcatalog = sparkSession._jsparkSession.catalog()

    @ignore_unicode_prefix
    @since(2.0)
    def currentDatabase(self):
        """Returns the current default database in this session."""
        return self._jcatalog.currentDatabase()

    @ignore_unicode_prefix
    @since(2.0)
    def setCurrentDatabase(self, dbName):
        """Sets the current default database in this session."""
        return self._jcatalog.setCurrentDatabase(dbName)

    @ignore_unicode_prefix
    @since(2.0)
    def listDatabases(self):
        """Returns a list of databases available across all sessions."""
        iter = self._jcatalog.listDatabases().toLocalIterator()
        databases = []
        while iter.hasNext():
            jdb = iter.next()
            databases.append(Database(
                name=jdb.name(),
                description=jdb.description(),
                locationUri=jdb.locationUri()))
        return databases

    @ignore_unicode_prefix
    @since(2.0)
    def listTables(self, dbName=None):
        """Returns a list of tables/views in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary views.
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listTables(dbName).toLocalIterator()
        tables = []
        while iter.hasNext():
            jtable = iter.next()
            tables.append(Table(
                name=jtable.name(),
                database=jtable.database(),
                description=jtable.description(),
                tableType=jtable.tableType(),
                isTemporary=jtable.isTemporary()))
        return tables

    @ignore_unicode_prefix
    @since(2.0)
    def listFunctions(self, dbName=None):
        """Returns a list of functions registered in the specified database.

        If no database is specified, the current database is used.
        This includes all temporary functions.
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listFunctions(dbName).toLocalIterator()
        functions = []
        while iter.hasNext():
            jfunction = iter.next()
            functions.append(Function(
                name=jfunction.name(),
                description=jfunction.description(),
                className=jfunction.className(),
                isTemporary=jfunction.isTemporary()))
        return functions

    @ignore_unicode_prefix
    @since(2.0)
    def listColumns(self, tableName, dbName=None):
        """Returns a list of columns for the given table/view in the specified database.

        If no database is specified, the current database is used.

        Note: the order of arguments here is different from that of its JVM counterpart
        because Python does not support method overloading.
        """
        if dbName is None:
            dbName = self.currentDatabase()
        iter = self._jcatalog.listColumns(dbName, tableName).toLocalIterator()
        columns = []
        while iter.hasNext():
            jcolumn = iter.next()
            columns.append(Column(
                name=jcolumn.name(),
                description=jcolumn.description(),
                dataType=jcolumn.dataType(),
                nullable=jcolumn.nullable(),
                isPartition=jcolumn.isPartition(),
                isBucket=jcolumn.isBucket()))
        return columns

    @since(2.0)
    def createExternalTable(self, tableName, path=None, source=None, schema=None, **options):
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        :return: :class:`DataFrame`
        """
        warnings.warn(
            "createExternalTable is deprecated since Spark 2.2, please use createTable instead.",
            DeprecationWarning)
        return self.createTable(tableName, path, source, schema, **options)

    @since(2.2)
    def createTable(self, tableName, path=None, source=None, schema=None, **options):
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used. When ``path`` is specified, an external table is
        created from the data at the given path. Otherwise a managed table is created.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created table.

        :return: :class:`DataFrame`
        """
        if path is not None:
            options["path"] = path
        if source is None:
            source = self._sparkSession._wrapped._conf.defaultDataSourceName()
        if schema is None:
            df = self._jcatalog.createTable(tableName, source, options)
        else:
            if not isinstance(schema, StructType):
                raise TypeError("schema should be StructType")
            scala_datatype = self._jsparkSession.parseDataType(schema.json())
            df = self._jcatalog.createTable(tableName, source, scala_datatype, options)
        return DataFrame(df, self._sparkSession._wrapped)

    @since(2.0)
    def dropTempView(self, viewName):
        """Drops the local temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        Note that, the return type of this method was None in Spark 2.0, but changed to Boolean
        in Spark 2.1.

        >>> spark.createDataFrame([(1, 1)]).createTempView("my_table")
        >>> spark.table("my_table").collect()
        [Row(_1=1, _2=1)]
        >>> spark.catalog.dropTempView("my_table")
        >>> spark.table("my_table") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        self._jcatalog.dropTempView(viewName)

    @since(2.1)
    def dropGlobalTempView(self, viewName):
        """Drops the global temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        >>> spark.createDataFrame([(1, 1)]).createGlobalTempView("my_table")
        >>> spark.table("global_temp.my_table").collect()
        [Row(_1=1, _2=1)]
        >>> spark.catalog.dropGlobalTempView("my_table")
        >>> spark.table("global_temp.my_table") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        self._jcatalog.dropGlobalTempView(viewName)

    @since(2.0)
    def registerFunction(self, name, f, returnType=None):
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. note:: Deprecated in 2.3.0. Use :func:`spark.udf.register` instead.
        """
        warnings.warn(
            "Deprecated in 2.3.0. Use spark.udf.register instead.",
            DeprecationWarning)
        return self._sparkSession.udf.register(name, f, returnType)

    @since(2.0)
    def isCached(self, tableName):
        """Returns true if the table is currently cached in-memory."""
        return self._jcatalog.isCached(tableName)

    @since(2.0)
    def cacheTable(self, tableName):
        """Caches the specified table in-memory."""
        self._jcatalog.cacheTable(tableName)

    @since(2.0)
    def uncacheTable(self, tableName):
        """Removes the specified table from the in-memory cache."""
        self._jcatalog.uncacheTable(tableName)

    @since(2.0)
    def clearCache(self):
        """Removes all cached tables from the in-memory cache."""
        self._jcatalog.clearCache()

    @since(2.0)
    def refreshTable(self, tableName):
        """Invalidates and refreshes all the cached data and metadata of the given table."""
        self._jcatalog.refreshTable(tableName)

    @since('2.1.1')
    def recoverPartitions(self, tableName):
        """Recovers all the partitions of the given table and update the catalog.

        Only works with a partitioned table, and not a view.
        """
        self._jcatalog.recoverPartitions(tableName)

    @since('2.2.0')
    def refreshByPath(self, path):
        """Invalidates and refreshes all the cached data (and the associated metadata) for any
        DataFrame that contains the given data source path.
        """
        self._jcatalog.refreshByPath(path)

    def _reset(self):
        """(Internal use only) Drop all existing databases (except "default"), tables,
        partitions and functions, and set the current database to "default".

        This is mainly used for tests.
        """
        self._jsparkSession.sessionState().catalog().reset()


def _test():
    import os
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.catalog

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.catalog.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.catalog tests")\
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    globs['spark'] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.catalog,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    spark.stop()
    if failure_count:
        sys.exit(-1)

if __name__ == "__main__":
    _test()

import json
import sys
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from datetime import datetime
from workingdays import date_utilities as wd


class parent:
    def __init__(self, spark):
        # ---------------------------------------------------------------------
        # set config attributes
        # ---------------------------------------------------------------------
        if len(sys.argv) > 1:
            with open(sys.argv[1]) as dependencyFile:
                conf = json.load(dependencyFile)
            for x in conf:
                setattr(parent, x, conf.get(x, ""))
        # ---------------------------------------------------------------------
        # set the process state
        # ---------------------------------------------------------------------
        processStart = wd.dateCleanup(datetime
                                      .utcnow()
                                      .strftime("%Y%m%d%H%M%S"))
        # ---------------------------------------------------------------------
        # create udf dateCleanup
        # ---------------------------------------------------------------------

        def dc(val):
            result = wd.dateCleanup(val, epoch=True)
            return result
        dateCleanup = F.udf(dc, TimestampType())
        # ---------------------------------------------------------------------
        # set audit
        # ---------------------------------------------------------------------
        tableId = self.name
        columnAudit = "tableid,ins_gmt_ts,process_timestamp"
        namespaceAudit = "ea_sc_kif"
        tableAudit = "batch_process_times"
        cfAudit = "o"
        dfAudit = DataFrame(spark.sparkContext
                            ._jvm.com.hpe.hbase
                            .HbaseManager
                            .getDF(columnAudit,
                                   namespaceAudit,
                                   tableAudit,
                                   cfAudit), spark
                            ).where(F.col("tableid") == tableId)
        # ---------------------------------------------------------------------
        # get last proccesed time
        # ---------------------------------------------------------------------
        col = "ins_gmt_ts"
        lastProcessedDate = dfAudit.select(col).collect()
        if len(lastProcessedDate) > 0:
            DTS = str([ele[col] for ele in lastProcessedDate][0])
            deltaFilter = (F.col('epoch_ts') > DTS)
        else:
            DTS = ""
            deltaFilter = ""
        # ---------------------------------------------------------------------
        # get mapper attributes
        # ---------------------------------------------------------------------
        with open(self.app["mapper-properties"]) as mapperFile:
            topMap = json.load(mapperFile)
        # ---------------------------------------------------------------------
        # set hbase table properties
        # ---------------------------------------------------------------------
        hbase = topMap["hbase"]
        table = hbase.get("table", "none")
        cf = hbase.get("cf", "none")
        namespace = hbase.get("namespace", "none")
        column = hbase.get("column", "none")
        latestVersionMapped = hbase.get("latestVersion", "none")
        if latestVersionMapped == "True":
            latestVersion = True
        else:
            latestVersion = False
        # ---------------------------------------------------------------------
        # create data frame
        # ---------------------------------------------------------------------
        df = spark.sparkContext \
            ._jvm.com.hpe.hbase \
            .HbaseManager.getDF(column,
                                namespace,
                                table,
                                cf,
                                latestVersion)
        pyDF = DataFrame(df, spark)
        pyDF = pyDF.withColumn("epoch_ts",
                               F.to_timestamp
                               (dateCleanup
                                (pyDF["ts"]), 'yyyy-MM-dd HH:mm:ss'))
        sqlDF = pyDF
        if type(deltaFilter) is not str:
            sqlDF = sqlDF.where(deltaFilter)
        # ---------------------------------------------------------------------
        # max date
        # ---------------------------------------------------------------------
        maxDate = dfColumnToString(sqlDF.agg(F.max("epoch_ts"))
                                   .select("max(epoch_ts)"), "max(epoch_ts)")
        # ---------------------------------------------------------------------
        # if no delta results available then end else process delta
        # ---------------------------------------------------------------------
        if maxDate:
            printInc = ("Found delta maxdate of '{}'."
                        + " Starting incremental update on process of "
                        + "delta records.")
            print(printInc.format(maxDate))
            # -----------------------------------------------------------------
            # set checkpoint dir
            # -----------------------------------------------------------------
            spark \
                .sparkContext \
                .setCheckpointDir(self.app["checkpoint"])
            sqlDF.persist(StorageLevel.MEMORY_AND_DISK)
            sqlDF.take(1)
            # -----------------------------------------------------------------
            # set recursive table properties
            # -----------------------------------------------------------------
            recursive = topMap["recursive"]
            id = str(recursive.get("id", "none"))
            parentid = str(recursive.get("parentid", "none"))
            parentLookupCol = str(recursive.get("parentLookupCol", "none"))
            lookupCol = str(recursive.get("lookupCol", "none"))
            levels = recursive.get("levels", "2")
            # -----------------------------------------------------------------
            # set output DF columns
            # -----------------------------------------------------------------
            outCols = stringToList(column)
            outCols.insert(0, id)
            outCols.append(lookupCol)
            # -----------------------------------------------------------------
            # run recursiveLookup
            # -----------------------------------------------------------------
            recursiveDF = recursiveLookup(sqlDF,
                                          id,
                                          parentid,
                                          parentLookupCol,
                                          lookupCol,
                                          levels
                                          ).select(outCols)
            recursiveDF = recursiveDF.withColumn("parentlevel",
                                                 F.col("parentlevel")
                                                 .cast(StringType()))
            recursiveDF = recursiveDF.withColumn("key", F.col(id))
            # -----------------------------------------------------------------
            # store df to hbase
            # -----------------------------------------------------------------
            outputCols = appendString("key", "parentlevel", lookupCol)
            print("writing recursive output to Hbase . . .")
            spark.sparkContext \
                ._jvm.com.hpe.hbase \
                .HbaseManager.setDF(recursiveDF._jdf,
                                    outputCols,
                                    "key",
                                    namespace,
                                    table,
                                    cf)
            # -----------------------------------------------------------------
            # close process state
            # -----------------------------------------------------------------
            processEnd = wd.dateCleanup(datetime
                                        .utcnow()
                                        .strftime("%Y%m%d%H%M%S"))
            dfAuditWrite = sqlDF.agg(F.max("epoch_ts")
                                     .alias("ins_gmt_ts")) \
                .select(F.col("ins_gmt_ts").cast(StringType())) \
                .withColumn("tableid", F.lit(tableId)) \
                .withColumn("process_start_ts",
                            F.lit(processStart).cast(StringType())) \
                .withColumn("process_end_ts", F.lit(processEnd
                                                    ).cast(StringType()))
            auditWriteCols = "tableid,ins_gmt_ts,process_start_ts" \
                + ",process_end_ts"
            # -----------------------------------------------------------------
            # write to the audit log
            # -----------------------------------------------------------------
            print("updating audit log . . .")
            spark.sparkContext \
                ._jvm.com.hpe.hbase \
                .HbaseManager \
                .setDF(dfAuditWrite._jdf,
                       auditWriteCols,
                       "tableid",
                       namespaceAudit,
                       tableAudit,
                       cfAudit)
            # -----------------------------------------------------------------
            # add finial df to class (if run outside of __main__)
            # -----------------------------------------------------------------
            self.df = recursiveDF
        else:
            # -----------------------------------------------------------------
            # No delta results
            # -----------------------------------------------------------------
            print("No delta available.")


def stringToList(str, delimiter=","):
    list = []
    for value in str.split(delimiter):
        list.append(value)
    return list


def appendString(*args, **delimiter):
    if len(delimiter) == 0:
        delimiter = ","
    else:
        delimiter = delimiter.get("delimiter", ",")
    count = 1
    output = ''
    for string in args:
        if count != len(args):
            output = output + string + delimiter
        else:
            output = output + string
        count += 1
    return output


def dfColumnToDict(dataFrame, headerColumn):
    """
    FUNCTION: dfColumnToDict

    DESCRIPTION:
        """
    dfCollect = dataFrame.collect()

    header = json.loads(json.dumps
                        ([Dict[headerColumn] for Dict in dfCollect]))
    myDict = {}
    for i in header:
        myDict.update(i)
    return myDict


def dfColumnToList(dataFrame, headerColumn):
    """
    FUNCTION: dfColumnToList

    DESCRIPTION:
        """
    dfCollect = dataFrame.collect()
    header = [Dict[headerColumn] for Dict in dfCollect]
    return header


def dfColumnToString(dataFrame, headerColumn):
    """
    FUNCTION: dfColumnToString

    DESCRIPTION:
        """
    dfCollect = dataFrame.collect()
    header = [Dict[headerColumn] for Dict in dfCollect]
    return header[0]


def lookup(mappingBroadcasted):
    def f(x):
        return mappingBroadcasted.value.get(x)
    return F.udf(f)


def createBroadcast(df, map):
    broadcastLookup = spark \
        .sparkContext \
        .broadcast(dfColumnToDict(df, map))
    return broadcastLookup


def parentMap(df, id, parentid, parentLookupCol, lookupCol):
    top = "top" + lookupCol
    rec = df.withColumn("parent_flag",
                        F.when(F.col(parentid) == "",
                               "true").otherwise("false"))
    rec = rec.withColumn(top,
                         F.when(F.col("parent_flag") == "true",
                                F.col(parentLookupCol)))
    parent = rec.where(F.col("parent_flag") == "true") \
        .select(F.create_map([F.col(id),
                              F.col(top)]).alias("parentMap"))
    return parent


def childMap(df, id, parentid, parentLookupCol, lookupCol):
    top = "top" + parentLookupCol
    rec = df.where((F.col(lookupCol).isNotNull())
                   & (F.col("parent_flag") == "false"))
    rec = rec.select(F.col(id),
                     F.lit("").alias(parentid),
                     F.col(lookupCol).alias(top))
    child = rec.select(F.create_map([F.col(id),
                                     F.col(top)]).alias("childMap"))
    return child


def recursiveLookup(df, id, parentid, parentLookupCol, lookupCol, levels):
    """
    FUNCTION: recursiveLookup

    DESCRIPTION:
        """
    if levels <= 1:
        return df
    else:
        a = {}
        # ---------------------------------------------------------------------
        # Define child 1
        # ---------------------------------------------------------------------
        b1 = createBroadcast(parentMap(df,
                                       id,
                                       parentid,
                                       parentLookupCol,
                                       lookupCol), "parentMap")
        child = df.withColumn("parent_flag",
                              F.when(F.col(parentid) == "",
                                     "true").otherwise("false"))
        child = child.withColumn(lookupCol,
                                 F.when(F.col("parent_flag") == "true",
                                        F.col(parentLookupCol))
                                 .otherwise(lookup(b1)(F.col(parentid)))
                                 )
        child = child.withColumn("parentlevel", F.when(F.col("parent_flag")
                                                       == "true", F.lit(1)))
        count = 1
        level = 2
        # ---------------------------------------------------------------------
        # dynamically create key
        # ---------------------------------------------------------------------
        key = "childDF" + str(1)
        returnKey = "childDF" + str(levels - 1)
        # ---------------------------------------------------------------------
        # calculate value
        # ---------------------------------------------------------------------
        value = child
        a[key] = value
        # ---------------------------------------------------------------------
        # find nested children
        # ---------------------------------------------------------------------
        while count > 0:
            if level >= levels:
                count = 0
            else:
                # -------------------------------------------------------------
                # create 1st mapping & broadcast
                # -------------------------------------------------------------
                b = createBroadcast(childMap(a.get("childDF" + str(count)),
                                             id,
                                             parentid,
                                             "hli",
                                             "topli"), "childMap")
                # -------------------------------------------------------------
                # dynamically create key
                # -------------------------------------------------------------
                key = "childDF" + str(level)
                # -------------------------------------------------------------
                # calculate value
                # -------------------------------------------------------------
                childDF = a.get("childDF" + str(count)) \
                    .withColumn("parent_lv" + str(level) + "_flag",
                                F.when(F.col(lookupCol).isNull(),
                                       "false").otherwise("true")) \
                    .withColumn("parentlevel",
                                F.when((F.col(lookupCol).isNotNull())
                                       & (F.col("parentlevel").isNull()),
                                       F.lit(level))
                                .otherwise(F.col("parentlevel"))) \
                    .withColumn(lookupCol,
                                F.when(F.col("parent_lv"
                                             + str(level)
                                             + "_flag") == "true",
                                       F.col(lookupCol)
                                       ).otherwise(lookup(b)(F.col(parentid))))
                value = childDF
                a[key] = value
                level += 1
                count += 1
                # -------------------------------------------------------------
        df.unpersist()
        return a[returnKey]


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Define Spark Session
    # -------------------------------------------------------------------------
    spark = SparkSession \
        .builder \
        .appName("Recursive Lookup - tophli") \
        .enableHiveSupport() \
        .getOrCreate()
    # -------------------------------------------------------------------------
    # run parent class
    # -------------------------------------------------------------------------
    parent(spark)
    spark.stop()

hbase utils row key scanner
mvn assembly:assembly
copy target/hbase-utils-0.1-SNAPSHOT-job.jar to hadoop gw
hadoop jar hbase-utils-0.1-SNAPSHOT-job.jar com.mozilla.hbase.utils.RowkeyRegex hdfs_dir_path startrow stoprow
package com.mozilla.hbase.utils;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

public class RowkeyRegex  implements Tool {
    private final Log LOG = LogFactory.getLog(RowkeyRegex.class);
    private static final String NAME = "RowkeyRegex";
    private static final String TABLE_NAME = "telemetry";
    private Configuration conf;
    
    public static class RowkeyMapper extends TableMapper<IntWritable,IntWritable>  {
        private static final IntWritable one = new IntWritable(1);
        @Override
            public void map(ImmutableBytesWritable row, Result values,Context context) throws InterruptedException,IOException {
            String rowKey = new String(values.getRow());
            String timestamp = String.valueOf(values.raw()[0].getTimestamp());
            context.write(new IntWritable(rowKey.length()),one);
        }

    }

    public static class RowkeyReducer extends TableReducer<IntWritable, IntWritable,IntWritable> {
       @Override
       public void reduce(IntWritable keyLength, Iterable<IntWritable> values, Context context) throws InterruptedException,IOException {
           int count = 0;
           for (IntWritable val : values) {
               count += val.get();
           }
           context.write(keyLength,new IntWritable(count));
       }
    }
                                              
    public Job initJob(String[] args) throws IOException {
        Job job = new Job(getConf());
        job.setJobName(NAME);
        job.setJarByClass(RowkeyRegex.class);
        Scan scan = new Scan();
        String columns = "data:json";
        /*scan.addColumns(columns);
        long minTimestamp = Long.valueOf(args[1]);
        long maxTimestamp = Long.valueOf(args[2]);
        scan.setTimeRange(minTimestamp,maxTimestamp);*/
        scan.setStartRow(Bytes.toBytes(args[1]));
        scan.setStopRow(Bytes.toBytes(args[2]));
        Filter qfilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("([0-9a-f][^2])"));
        //        scan.setFilter(qfilter);
        TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, RowkeyMapper.class, IntWritable.class, IntWritable.class, job);
        job.setReducerClass(RowkeyReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        return job;
    }

    public int run(String[] args) throws Exception {
        int rc = -1;
        Job job = initJob(args);
        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            rc = 0;
        }
        return rc;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RowkeyRegex(), args);
        System.exit(res);
    }
}



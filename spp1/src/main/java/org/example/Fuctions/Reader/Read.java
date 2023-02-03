package org.example.Fuctions.Reader;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class Read implements Supplier<Dataset<Row>> {

    private final FileSystem hdfs;
    private final String inputPathStr;
    private final SparkSession spark;


    @Override
    public Dataset<Row> get() {
        try {
            Path f = new Path(inputPathStr);
            if (hdfs.exists(f))
            {
                FileStatus[] fileStatuses = hdfs.listStatus(f);
               String []  listFiles = Arrays.stream(fileStatuses)
                       .filter(l -> !l.isDirectory())
                       .map(t -> t.getPath().toString()).toArray(String[]::new);

                return spark.read().option("delimiter", ";").option("header", "true").csv(listFiles);}
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return spark.emptyDataFrame();
    }
}

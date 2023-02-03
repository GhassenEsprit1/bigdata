package org.example.Fuctions.Reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadUT {

    String input = ConfigFactory.load().getString("app.data.input");
    @Test
    public void testRead() throws IOException {


        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();
        FileSystem hdfs =FileSystem.get(spark.sparkContext().hadoopConfiguration());
        Read r=new Read(hdfs,input,spark);

        Dataset<Row> axtual=r.get();
        assertThat(axtual)
                .isNotNull();

        // assertNotNull(axtual);
        // assertEquals(1, axtual.count());
    }
}

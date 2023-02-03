package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Fuctions.Mapper;
import org.example.Fuctions.Reader.Read;
import org.example.Fuctions.StatFunc;
import org.example.Fuctions.Writer.Write;
import org.example.Ville.beans.Ville;

import org.apache.hadoop.fs.*;

import java.io.IOException;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

@Slf4j
public class Main {
    public static void main(String[] args) throws IOException {
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();

        FileSystem hdfs =FileSystem.get(spark.sparkContext().hadoopConfiguration());

        String inputPath = config.getString("app.data.input");
        String outputPath = config.getString("app.data.output");
        Read r=new Read(hdfs,inputPath,spark);
        Dataset<Row> rowDataset = r.get();
        Write w =new Write(outputPath);

        Mapper m=new Mapper();
        Dataset<Ville> dsa = m.apply(rowDataset);
        //Dataset<Row> statds=rowDataset.groupBy("annee").agg(count("insee").as("commune"),sum("dep").as("expense"));
        //statds.show(20,true);
        //statds.write().mode(SaveMode.Overwrite).csv(outputPath);
        //w.accept(dsa);
        log.info("nbre ligne ={}",new StatFunc().apply(dsa));
        rowDataset.show(5,false);




    }
}
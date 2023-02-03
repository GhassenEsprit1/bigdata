package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Fuctions.Mapper;
import org.example.Fuctions.Receiver.VilleReciever;
import org.example.Fuctions.StatFunc;
import org.example.Fuctions.Writer.Write;
import org.example.Ville.beans.Ville;

import org.apache.hadoop.fs.*;

import java.io.IOException;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

@Slf4j
public class KafkaMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        String inputPath = config.getString("app.data.input");
        String outputPath = config.getString("app.data.output");
        String checkPoint = config.getString("app.data.checkpoint");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();

        FileSystem hdfs =FileSystem.get(spark.sparkContext().hadoopConfiguration());

        JavaStreamingContext jsc=JavaStreamingContext.getOrCreate(
                checkPoint,
                ()->
                {
                    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(spark.sparkContext()),
                            new Duration(1000 * 10)

                    );
                    javaStreamingContext.checkpoint(checkPoint);
                    VilleReciever reciever=new VilleReciever(inputPath,javaStreamingContext);
                    JavaDStream<Ville> javaDStream = reciever.get();
                    javaDStream.foreachRDD(
                            rdd->{
                                long o = System.currentTimeMillis();
                                log.info("micro batch at time {} ", o);
                                Dataset<Ville> villeDataset=spark.createDataset(
                                        rdd.rdd(),
                                        Encoders.bean(Ville.class)
                                ).cache();
                                villeDataset.printSchema();
                                Write w =new Write(outputPath+"/time="+o);

                                Mapper m=new Mapper();
                                //Dataset<Ville> dsa = m.apply(villeDataset);
                                //Dataset<Row> statds=villeDataset.groupBy("annee").agg(count("insee").as("commune"),sum("dep").as("expense"));
                                //statds.show(20,true);
                                //statds.write().mode(SaveMode.Overwrite).csv(outputPath);
                                //w.accept(dsa);
                                log.info("nbre ligne ={}",new StatFunc().apply(villeDataset));
                                villeDataset.show(5,false);
                                villeDataset.unpersist();
                            }
                    );
                    return javaStreamingContext;
                }

        );


        //Read r=new Read(hdfs,inputPath,spark);
        // Dataset<Row> rowDataset = r.get();
        jsc.start();


        jsc.awaitTerminationOrTimeout(1000 * 60 * 5);


    }
}
package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import scala.Tuple2;

import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
@Slf4j
public class JavaMain {
    //private static final Logger logger= LoggerFactory.getLogger(JavaMain.class);
    private static DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static void main(String[] args) throws IOException {

        Config config = ConfigFactory.load("application.conf");
        String appName = config.getString("app.name");
        String appID = formatter.format(LocalDateTime.now()) + "_" + new Random().nextLong();
        FileSystem hdfs = FileSystem.get(new Configuration());

        Path inputPath = new Path(config.getString("app.data.input"));
        Path outputPath = new Path(config.getString("app.data.output"));
        System.out.println("heelo");
        /**DATA PROCESSING**/
        log.info("Listing files frominputPath={} ...", inputPath);

        RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(inputPath, true);
        while (fileIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileIterator.next();
            log.info("fileInfo=<getName={}, getLen={}, getBlockSize={}, getPath={}>",
                    locatedFileStatus.getPath().getName(),
                    locatedFileStatus.getLen(),
                    locatedFileStatus.getBlockSize(),
                    locatedFileStatus.getPath().toString());
            FSDataInputStream instream = hdfs.open(locatedFileStatus.getPath());
            StringWriter writer = new StringWriter();
            IOUtils.copy(instream, writer, "UTF-8");
            Stream<String> lines = Arrays.stream(writer.toString().split("\n"))
                    .filter(l -> !l.startsWith("\""));

            Map<String, Integer> wordCount= lines.flatMap(l -> Arrays.stream(l.split(";")))
                    .filter(x -> !NumberUtils.isNumber(x))
                    .map(x -> new Tuple2<>(x, 1))
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, Integer::sum));
            log.info("wordCount");
            wordCount.forEach((k, v) -> log.info("({} -> {})", k, v));
            String outlines= wordCount.entrySet().stream()
                    .map(e -> String.format("%s,%d", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
            FSDataOutputStream outstream= hdfs.create(outputPath.suffix(String.format("/wordcount/%s", locatedFileStatus.getPath().getName())));
            IOUtils.write(outlines, outstream, "UTF-8");
            outstream.close();
            instream.close();}
        log.info("Copying files from inputPath={} to outputPath={}...", inputPath, outputPath);
        FileUtil.copy(
                FileSystem.get(inputPath.toUri(), hdfs.getConf()),
                inputPath,
                FileSystem.get(outputPath.toUri(), hdfs.getConf()),
                outputPath,
                false,
                hdfs.getConf()
        );

        }
        }

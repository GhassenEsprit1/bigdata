package org.example.Fuctions.Writer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.Ville.beans.Ville;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
@Slf4j
public class WriteUT {

    @Test
    public void testwrite() throws IOException {
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();
        String outputPath = config.getString("app.data.output");
        List<Ville> lista = Arrays.asList(Ville.builder()
                .annee("a")
                .insee("b")
                .commune("c")
                .dep("d")
                .distinction("e")
                .build());
        Dataset<Ville> dataset = spark.createDataset(lista, Encoders.bean(Ville.class));
        Write w=new Write(outputPath);
        w.accept(dataset);
        Path output = Paths.get(outputPath);
        Stream<Path> jsonFilePaths = Files.list(output)
                .filter(p -> p.getFileName().toString().startsWith("part-") && p.toString().endsWith(".csv"))
                ;
        List<String> lines=jsonFilePaths
                .flatMap(
                        outputJsonfilepath ->{
                            Stream<String> jsonFileContent= Stream.empty();
                            try {
                                jsonFileContent=Files.lines(outputJsonfilepath);
                            }
                            catch (IOException e)
                            {
                                log.info("ccc");
                            }
                            return jsonFileContent;

                        }
                )
                .collect(Collectors.toList());
        assertThat(lines)
                .isNotEmpty()
                .contains("e,f,c,d,a,b");



    }
}

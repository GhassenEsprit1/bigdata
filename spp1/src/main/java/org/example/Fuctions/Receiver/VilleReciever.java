package org.example.Fuctions.Receiver;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Fuctions.StringToVille;
import org.example.Fuctions.Type.VilleLongWritable;
import org.example.Fuctions.Type.VilleText;
import org.example.Fuctions.Type.VilleFileInputFormat;
import org.example.Ville.beans.Ville;


import java.io.Serializable;
import java.util.function.Supplier;
@RequiredArgsConstructor
@Builder
public class VilleReciever implements Supplier<JavaDStream<Ville>>, Serializable {
    private final String hdfsInputPathStr;
    private final JavaStreamingContext jsc;


    private final Function<Path,Boolean> filter = hdfsPath->{
        return hdfsPath.getName().endsWith(".csv") &&
                !hdfsPath.getName().startsWith("_") &&
                !hdfsPath.getName().startsWith(".tmp") ;
    };

    @Override
    public JavaDStream<Ville> get() {
        JavaPairInputDStream<VilleLongWritable, VilleText> inputDStream =jsc
                .fileStream(
                        hdfsInputPathStr,
                        VilleLongWritable.class,
                        VilleText.class,
                        VilleFileInputFormat.class,
                        filter,
                        true
                );
        StringToVille dr=new StringToVille();
JavaDStream<Ville> javaDStream=inputDStream.map(tuple -> dr.call(tuple._2().toString()));
        return javaDStream;
    }
}

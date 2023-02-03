package org.example.Fuctions.Writer;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.example.Ville.beans.Ville;

import java.util.function.Consumer;
@RequiredArgsConstructor
public class Write implements Consumer<Dataset<Ville>> {
    private final String outputPath;
    @Override
    public void accept(Dataset<Ville> Dataset) {
        Dataset.write().mode(SaveMode.Overwrite).csv(outputPath);
    }
}

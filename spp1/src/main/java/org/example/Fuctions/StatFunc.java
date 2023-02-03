package org.example.Fuctions;

import org.apache.spark.sql.Dataset;
import org.example.Ville.beans.Ville;

import java.util.function.Function;

public class StatFunc implements Function<Dataset<Ville>, Long > {
    @Override
    public Long apply(Dataset<Ville> villeDataset) {
        return villeDataset.count();
    }
}

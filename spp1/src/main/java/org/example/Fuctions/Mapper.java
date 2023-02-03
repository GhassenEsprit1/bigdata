package org.example.Fuctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.Ville.Function.VilleMapper;
import org.example.Ville.beans.Ville;

import java.util.function.Function;

public class Mapper implements Function<Dataset<Row>, Dataset<Ville> > {
    @Override
    public Dataset<Ville> apply(Dataset<Row> rowDataset) {
        return new VilleMapper().apply(rowDataset);
    }
}

package org.example.Ville.Function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.Ville.beans.Ville;

import java.util.function.Function;

public class VilleMapper implements Function<Dataset<Row>, Dataset<Ville>> {
    private final RowToVille parser = new RowToVille();
    private final MapFunction<Row, Ville> task = parser::apply;
    //private final MapFunction<Row, Ville> task = new RowToVilleSpark();

    @Override
    public Dataset<Ville> apply(Dataset<Row> inputDS) {
        return inputDS.map(task, Encoders.bean(Ville.class));
    }
}

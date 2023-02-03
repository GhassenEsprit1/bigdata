package org.example.Ville.Function;

import org.apache.spark.sql.Row;
import org.example.Ville.beans.Ville;

import java.io.Serializable;
import java.util.function.Function;

public class RowToVille implements Function<Row, Ville>, Serializable {
    @Override
    public Ville apply(Row row) {
        String annee = row.getAs("annee") ;
        String insee = row.getAs("insee") ;
        String commune = row.getAs("commune") ;
        String dep = row.getAs("dep") ;
        String distinction = row.getAs("distinction") ;



        return Ville.builder()
                .annee(annee)
                .insee(insee)
                .commune(commune)
                .dep(dep)
                .distinction(distinction)
                .build();   }

}

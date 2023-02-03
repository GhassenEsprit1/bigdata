package org.example.Fuctions;

import org.apache.spark.api.java.function.Function;
import org.example.Ville.beans.Ville;


public class StringToVille implements Function<String, Ville> {


    @Override
    public Ville call(String s) throws Exception {
        String[] split = s.split(";");


        return Ville.builder()
                .annee(split[0])
                .insee(split[1])
                .commune(split[2])
                .dep(split[3])
                .distinction(split[4])
                .build();   }
}

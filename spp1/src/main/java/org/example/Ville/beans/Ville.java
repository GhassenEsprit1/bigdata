package org.example.Ville.beans;


import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
@Data
@Getter
@Builder
public class Ville implements Serializable {

    private String annee ;
    private String  insee ;
    private String  commune;
    private String dep;
    private String distinction;

}

package io.druid.query.aggregation.weightedHyperUnique;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.logger.Logger;

import java.io.IOException;

/**
 * Created by sajo on 24/9/16.
 */
public class WeightedDuration {
    private Integer weight;
    private Double duration;
    public static Logger log = new Logger("io.druid.initialization.Initialization");

    public Integer getWeight(){
        return weight;
    }

    public Double getDuration(){
        return duration;
    }

    public void setWeight(Integer w){
        this.weight = w;
    }

    public void setDuration(Double d){
        this.duration = d;
    }

    public static WeightedDuration fromJson(String json){
        ObjectMapper mapper = new ObjectMapper();
        try {
            log.error("json: %s", json);
            return mapper.readValue(json, WeightedDuration.class);
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static WeightedDuration fromPackedIntegerString(String str){
        Integer a = ((1<<16) - 1);
        WeightedDuration duration = new WeightedDuration();
        Integer packed = Integer.parseInt(str);
        Integer weight = a & packed;
        Double dur = 1.0*(packed >> 16);
        duration.setWeight(weight);
        duration.setDuration(dur);
        return duration;
    }
}

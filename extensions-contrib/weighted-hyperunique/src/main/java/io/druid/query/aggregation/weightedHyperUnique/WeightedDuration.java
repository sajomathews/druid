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
    private String user_id;
    public static Logger log = new Logger(WeightedDuration.class.getName());

    public Integer getWeight(){
        return weight;
    }

    public String getUser_id() { return user_id; }

    public void setWeight(Integer w){
        this.weight = w;
    }

    public void setUser_id(String user_id){
        this.user_id = user_id;
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
}

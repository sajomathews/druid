package io.druid.query.aggregation.weightedHyperUnique;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.logger.Logger;

import java.io.IOException;

/**
 * Created by sajo on 24/9/16.
 */
public class WeightedUserId {
    private Integer weight;
    private String id;
    public static Logger log = new Logger(WeightedUserId.class.getName());

    public Integer getWeight(){
        return weight;
    }

    public String getId() { return id; }

    public void setWeight(Integer w){
        this.weight = w;
    }

    public void setId(String id){
        this.id = id;
    }

    public static WeightedUserId fromJson(String json){
        ObjectMapper mapper = new ObjectMapper();
        try {
            log.error("json: %s", json);
            return mapper.readValue(json, WeightedUserId.class);
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

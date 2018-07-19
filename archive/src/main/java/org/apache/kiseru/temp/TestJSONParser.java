package org.apache.kiseru.temp;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

public class TestJSONParser {

    public static void main(String[] args) {
        String file = "/Users/chamibuddhika/Builds/kiseru/src/main/config/dsl.json";
        try {
            String json = new String(Files.readAllBytes(Paths.get(file)));
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            JsonNode resources  = root.path("resources");
            JsonNode data       = root.path("data");
            JsonNode schedules  = root.path("schedules");
            JsonNode activities = root.path("activities");
            JsonNode pipeline   = root.path("pipeline");

            if (resources != null) {
                Iterator<JsonNode> iterator = resources.getElements();
                System.out.print("Marks: [ ");

                while (iterator.hasNext()) {
                    JsonNode resource = iterator.next();
                    JsonNode id = resource.path("id");
                    System.out.println(id.getTextValue());
                }

                System.out.println("]");
            }

            System.out.println("resources : " + resources.getTextValue());
            System.out.println("data: "       + data.getTextValue());
            System.out.println("schedules: "  + schedules.getTextValue());
            System.out.println("activities: " + activities.getTextValue());
            System.out.println("pipline: "    + pipeline.getTextValue());

            // Read and store defs to a database

            // Read activities and construct the pipeline
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

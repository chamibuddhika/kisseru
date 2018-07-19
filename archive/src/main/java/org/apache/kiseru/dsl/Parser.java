/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.kiseru.dsl;

import org.apache.kiseru.pipeline.*;
import org.apache.kiseru.utils.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Parser {

    String getNodeValue(JsonNode node) {
        if (node != null) {
            return node.getTextValue();
        }
        return null;
    }

    Type parseType(String typeStr) {
        if ("table".equalsIgnoreCase(typeStr)) {
            return Type.TABLE;
        } else if ("scalar".equalsIgnoreCase(typeStr)) {
            return Type.SCALAR;
        } else if ("opaque".equalsIgnoreCase(typeStr)) {
            return Type.OPAQUE;
        }

        return Type.OPAQUE;
    }

    SchemaType parseSchemaType(String schemaTypeStr) {
        // BOOLEAN, INTEGER, FLOAT, DOUBLE, BIGINT, DATE, TIMESTAMP, VARCHAR
        if ("boolean".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.BOOLEAN;
        } else if ("integer".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.INTEGER;
        } else if ("float".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.FLOAT;
        } else if ("double".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.DOUBLE;
        } else if ("bigint".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.BIGINT;
        } else if ("date".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.DATE;
        } else if ("timestamp".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.TIMESTAMP;
        } else if ("varchar".equalsIgnoreCase(schemaTypeStr)) {
            return SchemaType.VARCHAR;
        }

        return SchemaType.OPAQUE;
    }

    Map<String, SchemaType> parseSchema(String schemaStr) {
       Map<String, SchemaType> schema = new HashMap<>();
       String[] items = schemaStr.split(",");
       for (String item : items) {
           String[] pair = item.split(":");
           if (pair.length == 2) {
              schema.put(pair[0], parseSchemaType(pair[1]));
           } else {
              // Log error
           }
       }

       return schema;
    }

    List<InPort> parseInPorts(JsonNode node) {
        List<InPort> inPorts = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode inPort = iterator.next();
                JsonNode id     = inPort.path(Constants.ID);
                JsonNode type   = inPort.path(Constants.TYPE);
                JsonNode schema = inPort.path(Constants.SCHEMA);

                InPort in = new InPort();
                in.setId(getNodeValue(id));
                in.setType(parseType(getNodeValue(type)));
                // in.setSchema(parseSchema(getNodeValue(schema)));

                in.validate();

                inPorts.add(in);
            }
        }

        return inPorts;
    }

    List<OutPort> parseOutPorts(JsonNode node) {
        List<OutPort> outPorts = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode inPort = iterator.next();
                JsonNode id     = inPort.path(Constants.ID);
                JsonNode type   = inPort.path(Constants.TYPE);
                JsonNode schema = inPort.path(Constants.SCHEMA);

                OutPort out = new OutPort();
                out.setId(getNodeValue(id));
                out.setType(parseType(getNodeValue(type)));
                // out.setSchema(parseSchema(getNodeValue(schema)));

                out.validate();

                outPorts.add(out);
            }
        }

        return outPorts;
    }

    List<RunConfiguration> parseRunConfigs(JsonNode node) {
        List<RunConfiguration> configs = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode config = iterator.next();
                JsonNode id = config.path(Constants.ID);
                JsonNode executable = config.path(Constants.EXECUTABLE);
                JsonNode resource   = config.path(Constants.RESOURCE);

                RunConfiguration rc = new RunConfiguration();
                rc.setId(getNodeValue(id));
                rc.setExecutable(getNodeValue(executable));
                rc.setResourceId(getNodeValue(resource));

                rc.validate();

                configs.add(rc);
            }
        }

        return configs;
    }

    List<Activity> parseActivities(JsonNode node) {
        List<Activity> activities = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode activity = iterator.next();
                JsonNode id = activity.path(Constants.ID);
                JsonNode configs = activity.path(Constants.RUNCONFIGS);
                JsonNode inputs  = activity.path(Constants.INPUTS);
                JsonNode outputs = activity.path(Constants.OUTPUTS);

                Activity a = new Activity();
                a.setId(getNodeValue(id));
                a.setRunConfigs(parseRunConfigs(configs));
                a.setInputs(parseInPorts(inputs));
                a.setOutputs(parseOutPorts(outputs));

                a.validate();

                activities.add(a);
            }
        }

        return activities;
    }

    List<Schedule> parseSchedules(JsonNode node) {
        List<Schedule> schedules = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode schedule = iterator.next();
                JsonNode id = schedule.path(Constants.ID);
                JsonNode cron = schedule.path(Constants.CRON);

                Schedule s = new Schedule();
                s.setId(getNodeValue(id));
                s.setCron(getNodeValue(cron));

                s.validate();

                schedules.add(s);
            }
        }

        return schedules;
    }

    List<DataNode> parseData(JsonNode node) {
        List<DataNode> data = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode resource = iterator.next();
                JsonNode id = resource.path(Constants.ID);
                JsonNode uri = resource.path(Constants.URI);
                JsonNode username = resource.path(Constants.USERNAME);
                JsonNode password = resource.path(Constants.PASSWORD);
                JsonNode table    = resource.path(Constants.TABLE);
                JsonNode query    = resource.path(Constants.QUERY);

                DataNode d = new DataNode();
                d.setId(getNodeValue(id));
                d.setUri(getNodeValue(uri));
                d.setUserName(getNodeValue(username));
                d.setPassword(getNodeValue(password));
                d.setTable(getNodeValue(table));
                d.setQuery(getNodeValue(query));

                d.validate();

                data.add(d);
            }
        }

        return data;
    }

    List<Resource> parseResources(JsonNode node) {
        List<Resource> resources = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode schedule = iterator.next();
                JsonNode id = schedule.path(Constants.ID);
                JsonNode uri = schedule.path(Constants.CRON);
                JsonNode username = schedule.path(Constants.USERNAME);
                JsonNode password = schedule.path(Constants.PASSWORD);

                Resource r = new Resource();
                r.setId(getNodeValue(id));
                r.setUri(getNodeValue(uri));
                r.setUsername(getNodeValue(username));
                r.setPassword(getNodeValue(password));

                r.validate();

                resources.add(r);
            }
        }

        return resources;
    }

    List<Input> parseInputs(JsonNode node) {
        List<Input> inputs = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode input  = iterator.next();
                JsonNode id     = input.path(Constants.ID);
                JsonNode source = input.path(Constants.SOURCE);

                Input in = new Input();
                in.setId(getNodeValue(id));
                in.setSource(getNodeValue(source));

                in.validate();

                inputs.add(in);
            }
        }

        return inputs;
    }

    List<Output> parseOutputs(JsonNode node) {
        List<Output> outputs = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode input  = iterator.next();
                JsonNode id     = input.path(Constants.ID);
                JsonNode source = input.path(Constants.SINK);

                Output out = new Output();
                out.setId(getNodeValue(id));
                out.setSink(getNodeValue(source));

                out.validate();

                outputs.add(out);
            }
        }

        return outputs;
    }

    List<Component> parseComponents(JsonNode node) {
        List<Component> components = new ArrayList<>();

        if (node != null) {
            Iterator<JsonNode> iterator = node.getElements();

            while (iterator.hasNext()) {
                JsonNode component = iterator.next();
                JsonNode activity  = component.path(Constants.ACTIVITY);
                JsonNode runConfig = component.path(Constants.RUNCONFIG);
                JsonNode inputs    = component.path(Constants.INPUTS);
                JsonNode outputs   = component.path(Constants.OUTPUTS);

                Component c = new Component();
                c.setId(getNodeValue(activity));
                c.setRunConfigId(getNodeValue(runConfig));
                c.setInputs(parseInputs(inputs));
                c.setOutputs(parseOutputs(outputs));

                c.validate();

                components.add(c);
            }
        }

        return components;
    }

    Pipeline parsePipeline(JsonNode node) {
        if (node != null) {
            JsonNode id       = node.path(Constants.ID);
            JsonNode schedule = node.path(Constants.SCHEDULE);
            JsonNode nodes    = node.path(Constants.NODES);

            Pipeline p = new Pipeline();
            p.setId(getNodeValue(id));
            // p.setScheduleId(getNodeValue(schedule));
            p.setComponents(parseComponents(nodes));

            return p;
        }

        return null;
    }

    Pipeline parse(String file) {
        try {
            String json = new String(Files.readAllBytes(Paths.get(file)));
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            JsonNode resourcesNode = root.path("resources");
            JsonNode dataNode = root.path("data");
            JsonNode schedulesNode = root.path("schedules");
            JsonNode activitiesNode = root.path("activities");
            JsonNode pipelineNode = root.path("pipeline");

            List<Resource> resources  = parseResources(resourcesNode);
            List<DataNode> data       = parseData(dataNode);
            List<Schedule> schedules  = parseSchedules(schedulesNode);
            List<Activity> activities = parseActivities(activitiesNode);

            Pipeline pipeline = parsePipeline(pipelineNode);

            return pipeline;
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

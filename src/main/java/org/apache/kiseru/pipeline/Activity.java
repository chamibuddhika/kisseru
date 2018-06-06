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
package org.apache.kiseru.pipeline;

import org.apache.kiseru.annotation.TaskParam;
import org.apache.kiseru.dsl.ParseObject;

import java.util.List;

public class Activity implements ParseObject {

    @TaskParam(name = "Task Id")
    private String id = "";

    private List<RunConfiguration> runConfigs;

    private List<InPort> inputs;

    private List<OutPort> outputs;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<RunConfiguration> getRunConfigs() {
        return runConfigs;
    }

    public void setRunConfigs(List<RunConfiguration> runConfigs) {
        this.runConfigs = runConfigs;
    }

    public List<InPort> getInputs() {
        return inputs;
    }

    public void setInputs(List<InPort> inputs) {
        this.inputs = inputs;
    }

    public List<OutPort> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<OutPort> outputs) {
        this.outputs = outputs;
    }

    /*
    public abstract void run(String id, int numPartitions, int partitionNum) throws Exception;

    public abstract void onEntry();

    public abstract void onSuccess();

    public abstract void onFailure();
    */

    public void validate() {
    }
}

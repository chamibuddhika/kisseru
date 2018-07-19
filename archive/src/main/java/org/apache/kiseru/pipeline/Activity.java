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

import org.apache.kiseru.dsl.ParseObject;

import javax.persistence.*;
import java.util.List;

@Entity(name = "Activity")
@Table(name = "ACTIVITY")
@NamedQueries({
        @NamedQuery(name="Activity.isActivityPresent",
                query="SELECT count(ACTIVITY_ID) FROM Activity a where a.id = :id"),
        @NamedQuery(name="Activity.getActivity",
                query="SELECT a FROM Activity a WHERE a.id = :id"),
})
public class Activity implements ParseObject {

    private String id = "";

    private List<RunConfiguration> runConfigs;

    private List<InPort> inputs;

    private List<OutPort> outputs;

    @Id
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @OneToMany(mappedBy="activity",
            cascade = CascadeType.ALL,
            orphanRemoval = true)
    public List<RunConfiguration> getRunConfigs() {
        return runConfigs;
    }

    public void setRunConfigs(List<RunConfiguration> runConfigs) {
        this.runConfigs = runConfigs;
    }

    @OneToMany(mappedBy="activity",
            cascade = CascadeType.ALL,
            orphanRemoval = true)
    public List<InPort> getInputs() {
        return inputs;
    }

    public void setInputs(List<InPort> inputs) {
        this.inputs = inputs;
    }

    @OneToMany(mappedBy="activity",
            cascade = CascadeType.ALL,
            orphanRemoval = true)
    public List<OutPort> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<OutPort> outputs) {
        this.outputs = outputs;
    }

    /*
    public abstract void run(String id, int numPartitions, int partitionNum) throws Exception;

    public abstract void onEntry()gg;

    public abstract void onSuccess();

    public abstract void onFailure();
    */

    public void validate() {
    }
}

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
package org.apache.kiseru.activity;

import org.apache.helix.HelixManager;
import org.apache.kiseru.runner.JobRunner;
import org.apache.kiseru.runner.impl.ScriptRunner;
import org.apache.kiseru.task.Task;
import org.apache.kiseru.task.TaskResultStore;

import java.util.Set;

public class ScriptActivity extends Task {

    String script;
    JobRunner runner;

    public ScriptActivity(String id, Set<String> parentIds, HelixManager helixManager,
                          TaskResultStore resultStore, String script) {
        super(id, parentIds, helixManager, resultStore);
        this.script = script;
        runner = new ScriptRunner(script);
    }

    @Override
    protected void runImpl(String resourceName, int numPartitions, int partitionNum)
            throws Exception {
        runner.run();
    }
}

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
package org.apache.kiseru.task;

import java.util.Set;

import org.apache.kiseru.activity.ScriptActivity;
import org.apache.helix.HelixManager;

public class AnalyticsTaskFactory implements TaskFactory {

  @Override
  public Task createActivity(String id, Set<String> parentIds, HelixManager helixManager,
                             TaskResultStore taskResultStore) {
    if (id.equalsIgnoreCase("script-1")) {
      return new ScriptActivity(id, parentIds, helixManager, taskResultStore, "/Users/chamibuddhika/temp.py");
    } else if (id.equalsIgnoreCase("script-2")) {
      return new ScriptActivity(id, parentIds, helixManager, taskResultStore, "/Users/chamibuddhika/td.py");
    }

    throw new IllegalArgumentException("Cannot create task for " + id);
  }

}

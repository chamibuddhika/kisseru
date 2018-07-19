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
package org.apache.kiseru.runner.impl;

import org.apache.kiseru.runner.JobRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ScriptRunner implements JobRunner {

    String script;

    public ScriptRunner(String script) {
        this.script = script;
    }

    @Override
    public boolean run() {
        // TODO: Make script execution more sophisticated to include
        // stdout and stderr handling and better error reporting
        System.out.println("Inside script runner");
        Process p = null;
        try {
            if (script.endsWith(".py")) {
                p = Runtime.getRuntime().exec("python " + script);
            } else if (script.endsWith(".sh")) {
                p = Runtime.getRuntime().exec("bash" + script);
            }

            if (p != null) {
                try {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(p.getInputStream()));

                    String line;
                    while ((line = in.readLine()) != null) {
                        System.out.println(line);
                    }
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}

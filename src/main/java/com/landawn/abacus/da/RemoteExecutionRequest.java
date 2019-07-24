/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.da;

import java.util.Map;

import com.landawn.abacus.annotation.Internal;
import com.landawn.abacus.annotation.Type;
import com.landawn.abacus.da.RemoteTask.RunMode;
import com.landawn.abacus.util.N; 

/**
 * 
 * @since 0.8
 * 
 * @author Haiyang Li
 */
@Internal
@Deprecated
public class RemoteExecutionRequest {
    private String requestHost;
    // TODO
    private String requestId;
    // TODO
    private RunMode runMode;

    private long requestTime;

    // TODO: quartz scheduler?    
    private String schedule;

    private String className;
    private Object parameter;
    private Map<String, byte[]> classBytesMap;

    //    public static void main(String[] args) {
    //        CodeGenerator.printClassMethod(RemoteExecutionRequest.class, true, true, true, null);
    //    }

    public String getRequestHost() {
        return requestHost;
    }

    public RemoteExecutionRequest setRequestHost(String requestHost) {
        this.requestHost = requestHost;

        return this;
    }

    public String getRequestId() {
        return requestId;
    }

    public RemoteExecutionRequest setRequestId(String requestId) {
        this.requestId = requestId;

        return this;
    }

    public com.landawn.abacus.da.RemoteTask.RunMode getRunMode() {
        return runMode;
    }

    public RemoteExecutionRequest setRunMode(com.landawn.abacus.da.RemoteTask.RunMode runMode) {
        this.runMode = runMode;

        return this;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public RemoteExecutionRequest setRequestTime(long requestTime) {
        this.requestTime = requestTime;

        return this;
    }

    public String getSchedule() {
        return schedule;
    }

    public RemoteExecutionRequest setSchedule(String schedule) {
        this.schedule = schedule;

        return this;
    }

    public String getClassName() {
        return className;
    }

    public RemoteExecutionRequest setClassName(String className) {
        this.className = className;

        return this;
    }

    public Object getParameter() {
        return parameter;
    }

    public RemoteExecutionRequest setParameter(Object parameter) {
        this.parameter = parameter;

        return this;
    }

    @Type("Map<String, byte[]>")
    public Map<String, byte[]> getClassBytesMap() {
        return classBytesMap;
    }

    public RemoteExecutionRequest setClassBytesMap(Map<String, byte[]> classBytesMap) {
        this.classBytesMap = classBytesMap;

        return this;
    }

    public RemoteExecutionRequest copy() {
        final RemoteExecutionRequest copy = new RemoteExecutionRequest();

        copy.requestHost = this.requestHost;
        copy.requestId = this.requestId;
        copy.runMode = this.runMode;
        copy.requestTime = this.requestTime;
        copy.schedule = this.schedule;
        copy.className = this.className;
        copy.parameter = this.parameter;
        copy.classBytesMap = this.classBytesMap;

        return copy;
    }

    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + N.hashCode(requestHost);
        h = 31 * h + N.hashCode(requestId);
        h = 31 * h + N.hashCode(runMode);
        h = 31 * h + N.hashCode(requestTime);
        h = 31 * h + N.hashCode(schedule);
        h = 31 * h + N.hashCode(className);
        h = 31 * h + N.hashCode(parameter);
        h = 31 * h + N.hashCode(classBytesMap);

        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof RemoteExecutionRequest) {
            final RemoteExecutionRequest other = (RemoteExecutionRequest) obj;

            return N.equals(requestHost, other.requestHost) && N.equals(requestId, other.requestId) && N.equals(runMode, other.runMode)
                    && N.equals(requestTime, other.requestTime) && N.equals(schedule, other.schedule) && N.equals(className, other.className)
                    && N.equals(parameter, other.parameter) && N.equals(classBytesMap, other.classBytesMap);
        }

        return false;
    }

    @Override
    public String toString() {
        return "{requestHost=" + N.toString(requestHost) + ", requestId=" + N.toString(requestId) + ", runMode=" + N.toString(runMode) + ", requestTime="
                + N.toString(requestTime) + ", schedule=" + N.toString(schedule) + ", className=" + N.toString(className) + ", parameter="
                + N.toString(parameter) + ", classBytesMap=" + N.toString(classBytesMap) + "}";
    }

}

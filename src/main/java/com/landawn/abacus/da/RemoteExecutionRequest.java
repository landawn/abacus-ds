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

// TODO: Auto-generated Javadoc
/**
 * The Class RemoteExecutionRequest.
 *
 * @author Haiyang Li
 * @since 0.8
 */
@Internal
@Deprecated
public class RemoteExecutionRequest {

    /** The request host. */
    private String requestHost;

    /** The request id. */
    // TODO
    private String requestId;

    /** The run mode. */
    // TODO
    private RunMode runMode;

    /** The request time. */
    private long requestTime;

    /** The schedule. */
    // TODO: quartz scheduler?    
    private String schedule;

    /** The class name. */
    private String className;

    /** The parameter. */
    private Object parameter;

    /** The class bytes map. */
    private Map<String, byte[]> classBytesMap;

    //    public static void main(String[] args) {
    //        CodeGenerator.printClassMethod(RemoteExecutionRequest.class, true, true, true, null);
    //    }

    /**
     * Gets the request host.
     *
     * @return the request host
     */
    public String getRequestHost() {
        return requestHost;
    }

    /**
     * Sets the request host.
     *
     * @param requestHost the request host
     * @return the remote execution request
     */
    public RemoteExecutionRequest setRequestHost(String requestHost) {
        this.requestHost = requestHost;

        return this;
    }

    /**
     * Gets the request id.
     *
     * @return the request id
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * Sets the request id.
     *
     * @param requestId the request id
     * @return the remote execution request
     */
    public RemoteExecutionRequest setRequestId(String requestId) {
        this.requestId = requestId;

        return this;
    }

    /**
     * Gets the run mode.
     *
     * @return the run mode
     */
    public com.landawn.abacus.da.RemoteTask.RunMode getRunMode() {
        return runMode;
    }

    /**
     * Sets the run mode.
     *
     * @param runMode the run mode
     * @return the remote execution request
     */
    public RemoteExecutionRequest setRunMode(com.landawn.abacus.da.RemoteTask.RunMode runMode) {
        this.runMode = runMode;

        return this;
    }

    /**
     * Gets the request time.
     *
     * @return the request time
     */
    public long getRequestTime() {
        return requestTime;
    }

    /**
     * Sets the request time.
     *
     * @param requestTime the request time
     * @return the remote execution request
     */
    public RemoteExecutionRequest setRequestTime(long requestTime) {
        this.requestTime = requestTime;

        return this;
    }

    /**
     * Gets the schedule.
     *
     * @return the schedule
     */
    public String getSchedule() {
        return schedule;
    }

    /**
     * Sets the schedule.
     *
     * @param schedule the schedule
     * @return the remote execution request
     */
    public RemoteExecutionRequest setSchedule(String schedule) {
        this.schedule = schedule;

        return this;
    }

    /**
     * Gets the class name.
     *
     * @return the class name
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the class name.
     *
     * @param className the class name
     * @return the remote execution request
     */
    public RemoteExecutionRequest setClassName(String className) {
        this.className = className;

        return this;
    }

    /**
     * Gets the parameter.
     *
     * @return the parameter
     */
    public Object getParameter() {
        return parameter;
    }

    /**
     * Sets the parameter.
     *
     * @param parameter the parameter
     * @return the remote execution request
     */
    public RemoteExecutionRequest setParameter(Object parameter) {
        this.parameter = parameter;

        return this;
    }

    /**
     * Gets the class bytes map.
     *
     * @return the class bytes map
     */
    @Type("Map<String, byte[]>")
    public Map<String, byte[]> getClassBytesMap() {
        return classBytesMap;
    }

    /**
     * Sets the class bytes map.
     *
     * @param classBytesMap the class bytes map
     * @return the remote execution request
     */
    public RemoteExecutionRequest setClassBytesMap(Map<String, byte[]> classBytesMap) {
        this.classBytesMap = classBytesMap;

        return this;
    }

    /**
     * Copy.
     *
     * @return the remote execution request
     */
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

    /**
     * Hash code.
     *
     * @return the int
     */
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

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
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

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "{requestHost=" + N.toString(requestHost) + ", requestId=" + N.toString(requestId) + ", runMode=" + N.toString(runMode) + ", requestTime="
                + N.toString(requestTime) + ", schedule=" + N.toString(schedule) + ", className=" + N.toString(className) + ", parameter="
                + N.toString(parameter) + ", classBytesMap=" + N.toString(classBytesMap) + "}";
    }

}

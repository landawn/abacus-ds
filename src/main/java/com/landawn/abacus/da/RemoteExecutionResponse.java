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

import com.landawn.abacus.annotation.Type;
import com.landawn.abacus.util.N;

// TODO: Auto-generated Javadoc
/**
 * The Class RemoteExecutionResponse.
 *
 * @author Haiyang Li
 * @since 0.8
 */
public class RemoteExecutionResponse {

    /** The result. */
    private Object result;

    /** The error code. */
    private String errorCode;

    /** The error message. */
    private String errorMessage;

    /** The request host. */
    private String requestHost;

    /** The request time. */
    private long requestTime;

    /** The response time. */
    private long responseTime;

    /** The Execution time. */
    private long ExecutionTime;

    /** The Execution host. */
    private String ExecutionHost;

    /** The elapsed time. */
    private long elapsedTime;

    //    public static void main(String[] args) {
    //        CodeGenerator.printClassMethod(RemoteExecutionResponse.class, true, true, true, null);
    //    }

    /**
     * Gets the result.
     *
     * @return
     */
    @Type("Object")
    public Object getResult() {
        return result;
    }

    /**
     * Sets the result.
     *
     * @param result
     * @return
     */
    public RemoteExecutionResponse setResult(Object result) {
        this.result = result;

        return this;
    }

    /**
     * Gets the error code.
     *
     * @return
     */
    @Type("String")
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Sets the error code.
     *
     * @param errorCode
     * @return
     */
    public RemoteExecutionResponse setErrorCode(String errorCode) {
        this.errorCode = errorCode;

        return this;
    }

    /**
     * Gets the error message.
     *
     * @return
     */
    @Type("String")
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Sets the error message.
     *
     * @param errorMessage
     * @return
     */
    public RemoteExecutionResponse setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;

        return this;
    }

    /**
     * Gets the request host.
     *
     * @return
     */
    @Type("String")
    public String getRequestHost() {
        return requestHost;
    }

    /**
     * Sets the request host.
     *
     * @param requestHost
     * @return
     */
    public RemoteExecutionResponse setRequestHost(String requestHost) {
        this.requestHost = requestHost;

        return this;
    }

    /**
     * Gets the request time.
     *
     * @return
     */
    @Type("long")
    public long getRequestTime() {
        return requestTime;
    }

    /**
     * Sets the request time.
     *
     * @param requestTime
     * @return
     */
    public RemoteExecutionResponse setRequestTime(long requestTime) {
        this.requestTime = requestTime;

        return this;
    }

    /**
     * Gets the response time.
     *
     * @return
     */
    @Type("long")
    public long getResponseTime() {
        return responseTime;
    }

    /**
     * Sets the response time.
     *
     * @param responseTime
     * @return
     */
    public RemoteExecutionResponse setResponseTime(long responseTime) {
        this.responseTime = responseTime;

        return this;
    }

    /**
     * Gets the execution time.
     *
     * @return
     */
    @Type("long")
    public long getExecutionTime() {
        return ExecutionTime;
    }

    /**
     * Sets the execution time.
     *
     * @param ExecutionTime
     * @return
     */
    public RemoteExecutionResponse setExecutionTime(long ExecutionTime) {
        this.ExecutionTime = ExecutionTime;

        return this;
    }

    /**
     * Gets the execution host.
     *
     * @return
     */
    @Type("String")
    public String getExecutionHost() {
        return ExecutionHost;
    }

    /**
     * Sets the execution host.
     *
     * @param ExecutionHost
     * @return
     */
    public RemoteExecutionResponse setExecutionHost(String ExecutionHost) {
        this.ExecutionHost = ExecutionHost;

        return this;
    }

    /**
     * Gets the elapsed time.
     *
     * @return
     */
    @Type("long")
    public long getElapsedTime() {
        return elapsedTime;
    }

    /**
     * Sets the elapsed time.
     *
     * @param elapsedTime
     * @return
     */
    public RemoteExecutionResponse setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;

        return this;
    }

    /**
     *
     * @return
     */
    public RemoteExecutionResponse copy() {
        final RemoteExecutionResponse copy = new RemoteExecutionResponse();

        copy.result = this.result;
        copy.errorCode = this.errorCode;
        copy.errorMessage = this.errorMessage;
        copy.requestHost = this.requestHost;
        copy.requestTime = this.requestTime;
        copy.responseTime = this.responseTime;
        copy.ExecutionTime = this.ExecutionTime;
        copy.ExecutionHost = this.ExecutionHost;
        copy.elapsedTime = this.elapsedTime;

        return copy;
    }

    /**
     *
     * @return
     */
    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + N.hashCode(result);
        h = 31 * h + N.hashCode(errorCode);
        h = 31 * h + N.hashCode(errorMessage);
        h = 31 * h + N.hashCode(requestHost);
        h = 31 * h + N.hashCode(requestTime);
        h = 31 * h + N.hashCode(responseTime);
        h = 31 * h + N.hashCode(ExecutionTime);
        h = 31 * h + N.hashCode(ExecutionHost);
        h = 31 * h + N.hashCode(elapsedTime);

        return h;
    }

    /**
     *
     * @param obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof RemoteExecutionResponse) {
            RemoteExecutionResponse other = (RemoteExecutionResponse) obj;

            if (N.equals(result, other.result) && N.equals(errorCode, other.errorCode) && N.equals(errorMessage, other.errorMessage)
                    && N.equals(requestHost, other.requestHost) && N.equals(requestTime, other.requestTime) && N.equals(responseTime, other.responseTime)
                    && N.equals(ExecutionTime, other.ExecutionTime) && N.equals(ExecutionHost, other.ExecutionHost)
                    && N.equals(elapsedTime, other.elapsedTime)) {

                return true;
            }
        }

        return false;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "{result=" + N.toString(result) + ", errorCode=" + N.toString(errorCode) + ", errorMessage=" + N.toString(errorMessage) + ", requestHost="
                + N.toString(requestHost) + ", requestTime=" + N.toString(requestTime) + ", responseTime=" + N.toString(responseTime) + ", ExecutionTime="
                + N.toString(ExecutionTime) + ", ExecutionHost=" + N.toString(ExecutionHost) + ", elapsedTime=" + N.toString(elapsedTime) + "}";
    }
}

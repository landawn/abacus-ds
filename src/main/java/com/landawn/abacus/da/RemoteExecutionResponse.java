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

/**
 * 
 * @since 0.8
 * 
 * @author Haiyang Li
 */
public class RemoteExecutionResponse {
    private Object result;
    private String errorCode;
    private String errorMessage;
    private String requestHost;
    private long requestTime;
    private long responseTime;
    private long ExecutionTime;
    private String ExecutionHost;
    private long elapsedTime;

    //    public static void main(String[] args) {
    //        CodeGenerator.printClassMethod(RemoteExecutionResponse.class, true, true, true, null);
    //    }

    @Type("Object")
    public Object getResult() {
        return result;
    }

    public RemoteExecutionResponse setResult(Object result) {
        this.result = result;

        return this;
    }

    @Type("String")
    public String getErrorCode() {
        return errorCode;
    }

    public RemoteExecutionResponse setErrorCode(String errorCode) {
        this.errorCode = errorCode;

        return this;
    }

    @Type("String")
    public String getErrorMessage() {
        return errorMessage;
    }

    public RemoteExecutionResponse setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;

        return this;
    }

    @Type("String")
    public String getRequestHost() {
        return requestHost;
    }

    public RemoteExecutionResponse setRequestHost(String requestHost) {
        this.requestHost = requestHost;

        return this;
    }

    @Type("long")
    public long getRequestTime() {
        return requestTime;
    }

    public RemoteExecutionResponse setRequestTime(long requestTime) {
        this.requestTime = requestTime;

        return this;
    }

    @Type("long")
    public long getResponseTime() {
        return responseTime;
    }

    public RemoteExecutionResponse setResponseTime(long responseTime) {
        this.responseTime = responseTime;

        return this;
    }

    @Type("long")
    public long getExecutionTime() {
        return ExecutionTime;
    }

    public RemoteExecutionResponse setExecutionTime(long ExecutionTime) {
        this.ExecutionTime = ExecutionTime;

        return this;
    }

    @Type("String")
    public String getExecutionHost() {
        return ExecutionHost;
    }

    public RemoteExecutionResponse setExecutionHost(String ExecutionHost) {
        this.ExecutionHost = ExecutionHost;

        return this;
    }

    @Type("long")
    public long getElapsedTime() {
        return elapsedTime;
    }

    public RemoteExecutionResponse setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;

        return this;
    }

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

    @Override
    public String toString() {
        return "{result=" + N.toString(result) + ", errorCode=" + N.toString(errorCode) + ", errorMessage=" + N.toString(errorMessage) + ", requestHost="
                + N.toString(requestHost) + ", requestTime=" + N.toString(requestTime) + ", responseTime=" + N.toString(responseTime) + ", ExecutionTime="
                + N.toString(ExecutionTime) + ", ExecutionHost=" + N.toString(ExecutionHost) + ", elapsedTime=" + N.toString(elapsedTime) + "}";
    }
}

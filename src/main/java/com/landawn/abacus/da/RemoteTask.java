/*
 * Copyright (C) 2016 HaiYang Li
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

/**
 * Anonymous classes are supported except the ones created by lambda. For example:
 * <pre> 
 *    RemoteTask<?, Object> remoteTask = new RemoteTask<Object, Object>() {
 *      public Object run(Object t) {
 *          // doing something.
 *          return null;
 *      }
 *    };
 *    
 *    remoteExecutor.execute(remoteTask, param);
 * </pre>
 *
 * @param <T>
 * @param <R>
 */
public interface RemoteTask<T, R> {

    /**
     * The function to be executed on remote servers. 
     * 
     * @param t The parameter must be primitive types/string (array)/date/collection/map/entity... which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     *        For example: primitive types/string (array)/date/collection/map/entity.
     * @return value of the type which can be serialized to JSON.
     *        The element in the Collection or key/value in Map must be concrete types: primitive types wrapper/String/Date/Entity..., can't be Collection or Map again.
     *        For example: primitive types/string (array)/date/collection/map/entity.
     * @throws Exception
     */
    public R run(T t) throws Exception;

    // TODO
    public static enum OperationType {
        /**
         * Start a new or replace the existing task. 
         */
        START,
        /**
         * Suspend the existing task if it's running.
         */
        SUSPEND,
        /**
         * Stop the existing task if it's running.
         */
        STOP,
        /**
         * Remove the task if it exists.
         */
        REMOVE;
    }

    // TODO
    public static enum RunMode {
        INCLUSIVE, EXCLUSIVE;
    }
}

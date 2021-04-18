/*
 * Copyright (C) 2021 HaiYang Li
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
package com.landawn.abacus.da.canssandra;

import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

class ResultSetImpl implements ResultSet {
    private final com.datastax.driver.core.ResultSet rs;

    ResultSetImpl(final com.datastax.driver.core.ResultSet rs) {
        this.rs = rs;
    }

    public static ResultSetImpl wrap(final com.datastax.driver.core.ResultSet rs) {
        return new ResultSetImpl(rs);
    }

    @Override
    public Row one() {
        return rs.one();
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return rs.getColumnDefinitions();
    }

    @Override
    public boolean wasApplied() {
        return rs.wasApplied();
    }

    @Override
    public boolean isExhausted() {
        return rs.isExhausted();
    }

    @Override
    public boolean isFullyFetched() {
        return rs.isFullyFetched();
    }

    @Override
    public int getAvailableWithoutFetching() {
        return rs.getAvailableWithoutFetching();
    }

    @Override
    public ListenableFuture<com.datastax.driver.core.ResultSet> fetchMoreResults() {
        return rs.fetchMoreResults();
    }

    @Override
    public List<Row> all() {
        return rs.all();
    }

    @Override
    public Iterator<Row> iterator() {
        return rs.iterator();
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return rs.getExecutionInfo();
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        return rs.getAllExecutionInfo();
    }
}

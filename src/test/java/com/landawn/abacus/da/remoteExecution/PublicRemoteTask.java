package com.landawn.abacus.da.remoteExecution;

import com.landawn.abacus.util.N;
import com.landawn.abacus.util.RemoteTask;

public class PublicRemoteTask<T, R> implements RemoteTask<T, R> {
    @Override
    public R run(T t) {
        N.println("========================$$$$$$$$$$$$$$$$$$$$$%%%%%%%%%%%%%%%%%%%%%%" + t);
        return null;
    }
}

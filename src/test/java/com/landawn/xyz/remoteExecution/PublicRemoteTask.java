package com.landawn.xyz.remoteExecution;

import com.landawn.abacus.da.RemoteTask;
import com.landawn.abacus.util.N;

public class PublicRemoteTask<T, R> implements RemoteTask<T, R> {
    @Override
    public R run(T t) {
        N.println("========================$$$$$$$$$$$$$$$$$$$$$%%%%%%%%%%%%%%%%%%%%%%" + t);
        return null;
    }
}

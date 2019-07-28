package com.landawn.xyz.remoteExecution;

import com.landawn.abacus.util.N;
import com.landawn.abacus.util.StringUtil;

public class ClassB {
    private String name;

    public ClassB() {
    }

    public ClassB(String name) {
        this.name = name;
    }

    public String a() {
        // N.println(StringUtils.class);
        return "=======%%%%%%===" + N.uuid() + StringUtil.split("a, b, c", ",");
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}

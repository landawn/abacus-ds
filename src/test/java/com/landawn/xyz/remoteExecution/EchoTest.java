package com.landawn.xyz.remoteExecution;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.http.HttpRequest;
import com.landawn.abacus.util.N;

class EchoTest {

    static final String url = "http://localhost:8080/abacus/echo";

    @Test
    void test() {
        String resp = HttpRequest.url(url).post("hello");
        N.println(resp);
    }

}

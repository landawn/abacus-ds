package com.landawn.xyz.remoteExecution;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.landawn.abacus.da.RemoteExecutionResponse;
import com.landawn.abacus.da.RemoteExecutor;
import com.landawn.abacus.da.RemoteTask;
import com.landawn.abacus.http.ContentFormat;
import com.landawn.abacus.http.HttpSettings;
import com.landawn.abacus.util.DateUtil;
import com.landawn.abacus.util.N;

public class RemoteExecutorTest {
    static final String url = "http://localhost:8080/abacus/javaExecution";
    static final RemoteExecutor remoteExecutor = new RemoteExecutor(url);
    //
    //    @Test
    //    public void test_invoke_01() {
    //        final Method method = N.getDeclaredMethod(LocalTask.class, "test_print", Object.class);
    //
    //        List<RemoteExecutionResponse<Object>> respList = remoteExecutor.invoke(method, 123);
    //        N.println(respList);
    //
    //        respList = remoteExecutor.invoke(method, N.uuid(), ContentFormat.JSON);
    //        N.println(respList);
    //
    //        respList = remoteExecutor.invoke(method, Long.MAX_VALUE, ContentFormat.JSON_GZIP);
    //        N.println(respList);
    //
    //        respList = remoteExecutor.invoke(method, N.currentDate(), ContentFormat.JSON_SNAPPY);
    //        N.println(respList);
    //
    //        //        respList = remoteExecutor.invoke(method, new ClassB("ClassB Name : ClassB"), ContentFormat.JSON_LZ4);
    //        //        N.println(respList);
    //        //
    //        //        respList = remoteExecutor.invoke(method, new ClassB("ClassB Name : ClassB").a(), ContentFormat.JSON_LZ4);
    //        //        N.println(respList);
    //        //
    //        //        respList = remoteExecutor.invoke(method, N.asList(new ClassB("ClassB Name : ClassB")), ContentFormat.JSON_LZ4);
    //        //        N.println(respList);
    //    }
    //
    //    @Test
    //    public void test_invoke_02() {
    //        Method method = N.getDeclaredMethod(LocalTask.class, "test_add", List.class);
    //
    //        List<RemoteExecutionResponse<Object>> respList = remoteExecutor.invoke(method, (Object) N.asList(1, 2, 3));
    //        N.println(respList);
    //
    //        method = N.getDeclaredMethod(LocalTask.class, "test_split", List.class);
    //
    //        respList = remoteExecutor.invoke(method, (Object) N.asList(1, 2, 3));
    //        N.println(respList);
    //
    //        method = N.getDeclaredMethod(LocalTask.class, "test_split_2", List.class);
    //
    //        respList = remoteExecutor.invoke(method, (Object) N.asList(1, 2, 3));
    //        N.println(respList);
    //
    //        method = N.getDeclaredMethod(LocalTask.class, "test_map", Map.class);
    //
    //        Map<String, Integer> m = N.asMap(N.uuid(), 123, N.uuid(), 789);
    //
    //        respList = remoteExecutor.invoke(method, (Object) m);
    //        N.println(respList);
    //        assertTrue(((Map<String, Integer>) respList.get(0).getResult()).keySet().containsAll(m.keySet()));
    //        assertTrue(((Map<String, Integer>) respList.get(0).getResult()).values().containsAll(m.values()));
    //
    //        method = N.getDeclaredMethod(LocalTask.class, "test_dataset", Map.class);
    //
    //        respList = remoteExecutor.invoke(method, (Object) m);
    //        N.println(respList);
    //
    //        N.println(respList.get(0).getResult());
    //
    //        Map<String, ?> serverParameterMapper = N.asMap("localhost", m);
    //        respList = remoteExecutor.invoke(method, serverParameterMapper);
    //        N.println(respList);
    //
    //        N.println(respList.get(0).getResult());
    //    }

    //    @Test
    //    public void test_echo() {
    //        final String url = "http://localhost:8080/abacus/echo";
    //
    //        KryoParser kryoParser = ParserFactory.createKryoParser();
    //
    //        RemoteTask<?, Object> remoteTask = new RemoteTask<Object, Object>() {
    //            @Override
    //            public Object run(Object t) {
    //                LocalTask.test_print(t);
    //                return null;
    //            }
    //        };
    //
    //        final RemoteExecutionRequest request = remoteExecutor.createRemoteRequest((Class) remoteTask.getClass());
    //
    //        String resp = HttpRequest.url(url).header("Content-Type", HttpHeaders.Values.APPLICATION_KRYO).post(request);
    //        N.println(resp);
    //    }
    //
    //    @Test
    //    public void test_kryo() {
    //        KryoParser kryoParser = ParserFactory.createKryoParser();
    //
    //        RemoteTask<?, Object> remoteTask = new RemoteTask<Object, Object>() {
    //            @Override
    //            public Object run(Object t) {
    //                LocalTask.test_print(t);
    //                return null;
    //            }
    //        };
    //
    //        final RemoteExecutionRequest request = remoteExecutor.createRemoteRequest((Class) remoteTask.getClass());
    //
    //        String str = kryoParser.serialize(request);
    //        N.println(str);
    //
    //        RemoteExecutionRequest request2 = kryoParser.deserialize(RemoteExecutionRequest.class, str);
    //        N.println(request2);
    //        // assertEquals(request, request2);
    //
    //        File file = new File("./tmp");
    //
    //        kryoParser.serialize(file, request);
    //        request2 = kryoParser.deserialize(RemoteExecutionRequest.class, file);
    //        N.println(request2);
    //
    //        N.println(IOUtil.readBytes(file, 0, 64));
    //
    //        IOUtil.deleteIfExists(file);
    //
    //        kryoParser.serialize(file, request);
    //        request2 = kryoParser.deserialize(RemoteExecutionRequest.class, file);
    //        N.println(request2);
    //
    //        N.println(IOUtil.readBytes(file, 0, 64));
    //
    //        IOUtil.deleteIfExists(file);
    //    }

    @Test
    public void test_execute_01() {
        RemoteTask<?, Object> remoteTask = new RemoteTask<Object, Object>() {
            @Override
            public Object run(Object t) {
                LocalTask.test_print(t);
                return null;
            }
        };

        List<RemoteExecutionResponse> respList = remoteExecutor.execute(remoteTask, 123, HttpSettings.create().setContentFormat(ContentFormat.KRYO),
                Long.MAX_VALUE, null);
        N.println(respList);

        respList = remoteExecutor.execute(remoteTask, N.uuid(), HttpSettings.create().setContentFormat(ContentFormat.JSON), Long.MAX_VALUE, null);
        N.println(respList);

        respList = remoteExecutor.execute(remoteTask, Long.MAX_VALUE);
        N.println(respList);

        respList = remoteExecutor.execute(remoteTask, DateUtil.currentDate());
        N.println(respList);

        remoteTask = new RemoteTask<List<Integer>, Object>() {
            @Override
            public Object run(List<Integer> t) {
                return LocalTask.test_add(t);
            }
        };

        respList = remoteExecutor.execute(remoteTask, N.asList(1, 2, 3));
        N.println(respList);

        remoteTask = new RemoteTask<List<Integer>, Object>() {
            @Override
            public Object run(List<Integer> t) {
                return LocalTask.test_split(t);
            }
        };
        respList = remoteExecutor.execute(remoteTask, N.asList(1, 2, 3));
        N.println(respList);

        remoteTask = new RemoteTask<List<Integer>, Object>() {
            @Override
            public Object run(List<Integer> t) {
                return LocalTask.test_split_2(t);
            }
        };
        respList = remoteExecutor.execute(remoteTask, N.asList(1, 2, 3));
        N.println(respList);

        Map<String, Integer> m = N.asMap(N.uuid(), 123, N.uuid(), 789);
        remoteTask = new RemoteTask<Map<String, Integer>, Object>() {
            @Override
            public Object run(Map<String, Integer> t) {
                return LocalTask.test_map(t);
            }
        };
        respList = remoteExecutor.execute(remoteTask, (Object) m);
        N.println(respList);
        assertTrue(((Map<String, Integer>) respList.get(0).getResult()).keySet().containsAll(m.keySet()));
        assertTrue(((Map<String, Integer>) respList.get(0).getResult()).values().containsAll(m.values()));

        remoteTask = new RemoteTask<Map<String, Integer>, Object>() {
            @Override
            public Object run(Map<String, Integer> t) {
                LocalTask.test_dataset(t);
                return null;
            }
        };
        respList = remoteExecutor.execute(remoteTask, (Object) m);
        N.println(respList);
        N.println(respList.get(0).getResult());

        Map<String, ?> serverParameterMapper = N.asMap("localhost", m);
        respList = remoteExecutor.execute(remoteTask, serverParameterMapper);
        N.println(respList);

        N.println(respList.get(0).getResult());

    }

    @Test
    public void test_execute_02() {
        RemoteTask<?, Object> remoteTask = new RemoteTask_println_10();

        List<RemoteExecutionResponse> respList = remoteExecutor.execute(remoteTask, 123);
        N.println(respList);

        respList = remoteExecutor.execute(remoteTask, N.uuid());
        N.println(respList);

        respList = remoteExecutor.execute(remoteTask, Long.MAX_VALUE);
        N.println(respList);

        respList = remoteExecutor.execute(remoteTask, DateUtil.currentDate());
        N.println(respList);

        remoteTask = new RemoteTask_println_11();
        respList = remoteExecutor.execute(remoteTask, 123);
        N.println(respList);

        remoteTask = new RemoteTask_println_12();
        respList = remoteExecutor.execute(remoteTask, 123);
        N.println(respList);

        RemoteTask<?, Integer> remoteAddTask = new RemoteTask_add_20();
        List<RemoteExecutionResponse> addRespList = remoteExecutor.execute(remoteAddTask, N.asList(1, 2, 3));
        N.println(addRespList);

        remoteAddTask = new RemoteTask_add_21();
        addRespList = remoteExecutor.execute(remoteAddTask, N.asList(1, 2, 3));
        N.println(addRespList);

        remoteAddTask = new RemoteTask_add_22();
        addRespList = remoteExecutor.execute(remoteAddTask, N.asList(1, 2, 3));
        N.println(addRespList);

        remoteAddTask = new PublicRemoteTask<Object, Integer>();
        addRespList = remoteExecutor.execute(remoteAddTask, N.asList(1, 2, 3));
        N.println(addRespList);
    }

    class RemoteTask_println_10 implements RemoteTask<Object, Object> {
        @Override
        public Object run(Object t) {
            // TODO Auto-generated method stub
            LocalTask.test_print(t);
            return null;
        }
    }

    static class RemoteTask_println_11 implements RemoteTask<Object, Object> {
        @Override
        public Object run(Object t) {
            // TODO Auto-generated method stub
            LocalTask.test_print(t);
            return null;
        }
    }

    public static class RemoteTask_println_12 implements RemoteTask<Object, Object> {
        @Override
        public Object run(Object t) {
            // TODO Auto-generated method stub
            LocalTask.test_print(t);
            return null;
        }
    }

    class RemoteTask_add_20 implements RemoteTask<List<Integer>, Integer> {
        @Override
        public Integer run(List<Integer> t) {
            // TODO Auto-generated method stub
            return LocalTask.test_add(t);
        }
    }

    static class RemoteTask_add_21 implements RemoteTask<List<Integer>, Integer> {
        @Override
        public Integer run(List<Integer> t) {
            // TODO Auto-generated method stub
            return LocalTask.test_add(t);
        }
    }

    public static class RemoteTask_add_22 implements RemoteTask<List<Integer>, Integer> {
        @Override
        public Integer run(List<Integer> t) {
            // TODO Auto-generated method stub
            return LocalTask.test_add(t);
        }
    }
}

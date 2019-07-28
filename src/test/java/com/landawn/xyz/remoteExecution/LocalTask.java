package com.landawn.xyz.remoteExecution;

import java.util.List;
import java.util.Map;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.StringUtil;
import com.landawn.abacus.util.function.ToIntFunction;
import com.landawn.abacus.util.stream.Collectors;
import com.landawn.abacus.util.stream.Stream;

public class LocalTask {
    public static void test_print(Object parameter) {
        N.println("abc");

        N.println(parameter);

        N.println(new ClassB().a());
    }

    public static int test_add(List<Integer> intList) {
        return Stream.of(intList).collect(Collectors.summingInt(new ToIntFunction<Integer>() {
            @Override
            public int applyAsInt(Integer value) {
                return value.intValue();
            }
        })).intValue();
    }

    public static String[] test_split(List<Integer> intList) {
        return StringUtil.split(StringUtil.join(intList), ", ");
    }

    public static List<String> test_split_2(List<Integer> intList) {
        return N.asList(StringUtil.split(StringUtil.join(intList), ", "));
    }

    public static Map<String, Integer> test_map(Map<String, Integer> m) {
        m.put(N.uuid(), Integer.MIN_VALUE);
        return m;
    }

    public static DataSet test_dataset(Map<String, Integer> m) {
        m.put(N.uuid(), Integer.MIN_VALUE);
        return N.newDataSet("key", "value", m);
    }
}

/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.landawn.abacus.DataSet;
import com.landawn.abacus.da.Account;
import com.landawn.abacus.da.type.SheetType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.type.TypeFactory;
import com.landawn.abacus.util.IntList;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.StringUtil;
import com.landawn.abacus.util.StringWriter;
import com.landawn.abacus.util.TypeReference;
import com.landawn.abacus.util.function.BiFunction;
import com.landawn.abacus.util.function.Function;

import junit.framework.TestCase;

/**
 *
 * @since 0.8
 *
 * @author Haiyang Li
 */
public class SheetTest extends TestCase {

    static {
        TypeFactory.registerType(Sheet.class, new SheetType(Sheet.class, "Object", "Object", "Object"));
        // TypeFactory.registerClass(Sheet.class, "Sheet");
    }

    public void test_print() throws Exception {
        Sheet<String, String, String> sheet = new Sheet<>(N.<String> asList(), N.<String> asList());
        sheet.println();

        N.println(StringUtil.repeat("=", 80));

        sheet = new Sheet<>(N.asList("row1", "row2"), N.<String> asList());
        sheet.println();

        N.println(StringUtil.repeat("=", 80));

        sheet = new Sheet<>(N.<String> asList(), N.asList("col1", "col2"));
        sheet.println();
        N.println(sheet.println(new StringWriter()).toString());

        N.println(StringUtil.repeat("=", 80));

        sheet = new Sheet<>(N.asList("row1", "row2"), N.asList("col1", "col2"));
        sheet.println();

        N.println(StringUtil.repeat("=", 80));

        sheet = Sheet.rows(N.asList("rowlkajf0iafl1", "row2"), N.asList("col1", "coajsfiewflasl2"),
                new String[][] { { "1lkasjfiejwalfsfja", "1b" }, { "2a", "kajfiewajflsflsalkfjoiewajfsl;b" } });
        sheet.println();

        N.println(StringUtil.repeat("=", 80));

        sheet = Sheet.columns(N.asList("rowlkajf0iafl1", "row2"), N.asList("col1", "coajsfiewflasl2"),
                new String[][] { { "1lkasjfiejwalfsfja", "1b" }, { "2a", "kajfiewajflsflsalkfjoiewajfsl;b" } });
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.println(N.asList("row2"), N.asList("col1"));
        sheet.println(N.asList("rowlkajf0iafl1"), N.asList("coajsfiewflasl2"));
        sheet.println(N.asList("row2"), N.asList("coajsfiewflasl2"));
        sheet.println(N.<String> asList(), N.asList("coajsfiewflasl2"));
        sheet.println(N.asList("row2"), N.<String> asList());
        sheet.println(N.<String> asList(), N.<String> asList());
    }

    public void test_merge() throws Exception {
        Sheet<Integer, String, String> sheet1 = Sheet.rows(N.asList(1, 2), N.asList("a", "b"), new String[][] { { "1a", "1b" }, { "2a", "2b" } });
        Sheet<Integer, String, String> sheet2 = Sheet.rows(N.asList(1, 3), N.asList("a", "c"), new String[][] { { "1a", "1c" }, { "3a", "3c" } });

        Sheet<Integer, String, Integer> sheet3 = sheet1.merge(sheet2, new BiFunction<String, String, Integer>() {
            @Override
            public Integer apply(String t, String u) {
                return N.hashCode(t + ": " + u);
            }
        });

        sheet3.println();
    }

    public void test_swap() throws Exception {
        Account account = N.fill(Account.class);
        DataSet ds = N.newDataSet(N.asList(account, account)).copy(N.asList("firstName", "lastName"));
        Sheet<Integer, String, Object> sheet = Sheet.rows(IntList.rangeClosed(1, ds.size()).toList(), ds.columnNameList(), ds.columns().toList());

        sheet.updateRow(1, new Function<Object, Object>() {
            @Override
            public Object apply(Object t) {
                return t instanceof String ? t + "___" : t;
            }
        });

        sheet.println();

        sheet.swapColumns("firstName", "lastName");
        sheet.println();

        sheet.swapRows(1, 2);
        sheet.println();

        sheet.moveColumn("firstName", 0);
        sheet.println();

        sheet.moveRow(1, 0);
        sheet.println();

        sheet.renameRow(1, 3);
        sheet.println();

        sheet.renameColumn("firstName", "firstName2");
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.copy().println();

        N.println(StringUtil.repeat("=", 80));
        sheet.copy(N.asList(2), N.asList("lastName")).println();

        N.println(StringUtil.repeat("=", 80));
        sheet.clone().println();

        N.println(StringUtil.repeat("=", 80));
        sheet.addRow(10, N.asList("firstName_10", "lastName_10"));
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.addRow(0, 11, N.asList("firstName_11", "lastName_11"));
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.addRow(4, 12, N.asList("firstName_12", "lastName_12"));
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.addColumn(0, "middleName_00", N.asList("MN_01", "MN_02", "MN_03", "MN_04", "MN_05"));
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.addColumn(3, "middleName_03", N.asList("MN_01", "MN_02", "MN_03", "MN_04", "MN_05"));
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.swapColumns("middleName_00", "middleName_03");
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.moveColumn("middleName_03", 3);
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.removeColumn("middleName_00");
        sheet.println();

        N.println(StringUtil.repeat("=", 80));
        sheet.removeRow(11);
        sheet.println();
    }

    public void test_update() throws Exception {
        Account account = N.fill(Account.class);
        DataSet ds = N.newDataSet(N.asList(account, account)).copy(N.asList("firstName", "lastName"));
        Sheet<Integer, String, Object> sheet = Sheet.columns(IntList.rangeClosed(1, ds.size()).toList(), ds.columnNameList(), ds.columns().toList());

        sheet.updateRow(1, new Function<Object, Object>() {
            @Override
            public Object apply(Object t) {
                return t instanceof String ? t + "___" : t;
            }
        });

        sheet.println();

        sheet.updateColumn("firstName", new Function<Object, Object>() {
            @Override
            public Object apply(Object t) {
                return t instanceof String ? t + "###" : t;
            }
        });

        sheet.println();

        sheet.updateAll(new Function<Object, Object>() {
            @Override
            public Object apply(Object t) {
                return t instanceof String ? t + "+++" : t;
            }
        });

        sheet.println();
    }

    @Test
    public void test_00() {
        Sheet<String, String, Number> sheet = this.createSheet();
        sheet.println();

        List<Number> row = sheet.getRow("106");
        sheet.removeRow("106");
        sheet.println();
        assertEquals(103, sheet.get("102", "aa"));
        assertEquals(120, sheet.get("107", "cc"));
        assertEquals(122, sheet.get("108", "bb"));

        sheet.addRow("106", row);
        sheet.println();

        sheet = this.createSheet();
        List<Number> column = sheet.getColumn("bb");
        sheet.removeColumn("bb");
        sheet.println();
        assertEquals(103, sheet.get("102", "aa"));
        assertEquals(120, sheet.get("107", "cc"));
        sheet.addColumn("bb", column);
        sheet.println();
        Sheet<String, String, Number> copy = sheet.clone();
        copy.println();
    }

    @Test
    public void test_01() {
        Set<String> columnKeySet = N.asLinkedHashSet("aa", "bb", "cc");
        Set<String> rowKeySet = N.asLinkedHashSet("101", "102", "103", "104", "105", "106", "107", "108");
        Sheet<String, String, Object> sheet = new Sheet<>(rowKeySet, columnKeySet);

        int i = 100;
        for (String rowKey : rowKeySet) {
            for (String columnKey : columnKeySet) {
                sheet.put(rowKey, columnKey, i++);
            }
        }

        N.println(sheet);

        i = 10000;
        for (String columnKey : columnKeySet) {
            for (String rowKey : rowKeySet) {
                sheet.put(rowKey, columnKey, i++);
            }
        }

        N.println(sheet);

        N.println("*****************************");
        sheet.println();
        N.println("---------toDataSet-----------");
        sheet.toDataSetH().println();
        N.println("---------toDataSet0-----------");
        sheet.toDataSetV().println();
        N.println("---------toMatrix-----------");
        sheet.toMatrixH(Object.class).println();
        N.println("---------toMatrix0-----------");
        sheet.toMatrixV(Object.class).println();
        N.println("---------toArray-----------");
        N.println(sheet.toArrayH());
        N.println("---------toArray-----------");
        Number[][] tmp = sheet.toArrayH(Number.class);
        N.println(tmp);
        N.println("---------toArray0-----------");
        N.println(sheet.toArrayV());
        N.println("---------rotate-----------");
        sheet.transpose().println();
        N.println("---------rotate-rotate----------");
        sheet.transpose().transpose().println();
        N.println("--------------------");
        assertEquals(sheet, sheet.transpose().transpose());
        N.println("---------stream-----------");
        N.println(sheet.streamH().join(", "));
        N.println("---------stream0-----------");
        N.println(sheet.streamV().join("-"));
        N.println("---------stream2-----------");
        N.println(sheet.streamR().join("="));
        N.println("---------stream02-----------");
        N.println(sheet.streamC().join("#"));
        N.println("---------cells-----------");
        N.println(sheet.cellsH().join("&"));
        N.println("---------cells0-----------");
        N.println(sheet.cellsV().join("|"));
        N.println("---------set row-----------");
        sheet.setRow("106", N.asList(111, 112, 113));
        sheet.println();
        N.println("---------add row-----------");
        sheet.addRow("109", N.asList(111, 112, 113));
        sheet.println();
        N.println("---------remove row-----------");
        sheet.removeRow("106");
        sheet.println();
        N.println("---------set column-----------");
        sheet.setColumn("bb", IntList.repeat(111, 8).toList());
        sheet.println();
        N.println("---------add column-----------");
        sheet.addColumn("dd", IntList.repeat(112, 8).toList());
        sheet.println();
        N.println("---------remove column-----------");
        sheet.removeColumn("bb");
        sheet.println();
        N.println("*****************************");

        assertEquals(sheet, Sheet.rows(sheet.rowKeySet(), sheet.columnKeySet(), sheet.toArrayH()));
        assertEquals(sheet, Sheet.columns(sheet.rowKeySet(), sheet.columnKeySet(), sheet.transpose().toArrayH()));
        // assertEquals(sheet.rotate(), new ArraySheet<>(sheet.columnKeySet(), sheet.rowKeySet(), sheet.toArray0()));

        Collection<List<Object>> columns = new ArrayList<>();
        for (String columnKey : sheet.columnKeySet()) {
            columns.add(sheet.getColumn(columnKey));
        }
        assertEquals(sheet, Sheet.columns(sheet.rowKeySet(), sheet.columnKeySet(), columns));
        assertEquals(sheet.transpose(), Sheet.rows(sheet.columnKeySet(), sheet.rowKeySet(), columns));

        columns = new ArrayList<>();
        for (String rowKey : sheet.rowKeySet()) {
            columns.add(sheet.getRow(rowKey));
        }
        assertEquals(sheet.transpose(), Sheet.columns(sheet.columnKeySet(), sheet.rowKeySet(), columns));

        String rowKey = rowKeySet.iterator().next();
        String columnKey = columnKeySet.iterator().next();

        N.println("---------------------------------------------------------------------------");

        N.println("### before remove element: " + sheet.get(rowKey, columnKey));
        N.println(sheet);
        sheet.remove(rowKey, columnKey);
        N.println("### after remove element: " + sheet.get(rowKey, columnKey));
        N.println(sheet);

        N.println("---------------------------------------------------------------------------");

        List<Object> row = sheet.getRow(rowKey);
        N.println("### before remove/add row: " + row);
        N.println(sheet);
        sheet.removeRow(rowKey);
        N.println(sheet);
        sheet.addRow(rowKey, row);
        N.println("### after remove/add row: " + sheet.getRow(rowKey));
        N.println(sheet);

        sheet.trimToSize();

        N.println("---------------------------------------------------------------------------");

        List<Object> column = sheet.getColumn(columnKey);
        N.println("### before remove/add column: " + column);
        N.println(sheet);
        sheet.removeColumn(columnKey);
        N.println(sheet);
        sheet.addColumn(columnKey, column);
        N.println("### after remove/add column: " + sheet.getRow(rowKey));
        N.println(sheet);

        N.println("---------------------------------------------------------------------------");

        N.println("### clear sheet: ");
        sheet.clear();
        N.println(sheet);

        N.println(sheet.getRow(sheet.rowKeySet().iterator().next()));
        N.println(sheet.getColumn(sheet.columnKeySet().iterator().next()));

        sheet = new Sheet<>(rowKeySet, columnKeySet);
        N.println(sheet);
        N.println("*****************************");
        sheet.println();
        sheet.toDataSetH().println();
        // sheet.toDataSet0().println();
        sheet.toMatrixH(Object.class).println();
        sheet.transpose().println();
        sheet.transpose().transpose().println();
        assertEquals(sheet, sheet.transpose().transpose());
        N.println(sheet.streamH().join(", "));
        //        N.println(sheet.stream().join(", "));
        //        N.println("*****************************");

        assertTrue(sheet.containsRow("101"));
        assertTrue(sheet.containsColumn("aa"));

        assertNull(sheet.get("101", "aa"));
        assertNull(sheet.get(0, 0));

        assertNull(sheet.remove("101", "aa"));
        assertNull(sheet.remove(0, 0));

        assertEquals(8, sheet.rowLength());
        assertEquals(3, sheet.columnLength());

        Sheet<String, String, Object> sheet2 = new Sheet<>(rowKeySet, columnKeySet);
        assertTrue(N.asSet(sheet).contains(sheet2));
    }

    @Test
    public void test_11() {
        Set<String> columnKeySet = N.asLinkedHashSet("aa", "bb", "cc");
        Set<String> rowKeySet = N.asLinkedHashSet("101", "102", "103", "104", "105", "106", "107", "108");
        Sheet<String, String, Object> sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.println();

        N.println("*****************************");
        sheet.println();
        N.println("---------toDataSet-----------");
        sheet.toDataSetH().println();
        N.println("---------toDataSet0-----------");
        sheet.toDataSetV().println();
        N.println("---------toMatrix-----------");
        sheet.toMatrixH(Object.class).println();
        N.println("---------toMatrix0-----------");
        sheet.toMatrixV(Object.class).println();
        N.println("---------toArray-----------");
        N.println(sheet.toArrayH());
        N.println("---------toArray-----------");
        Number[][] tmp = sheet.toArrayH(Number.class);
        N.println(tmp);
        N.println("---------toArray0-----------");
        N.println(sheet.toArrayV());
        N.println("---------rotate-----------");
        sheet.transpose().println();
        N.println("---------rotate-rotate----------");
        sheet.transpose().transpose().println();
        N.println("--------------------");
        assertEquals(sheet, sheet.transpose().transpose());
        N.println("---------stream-----------");
        N.println(sheet.streamH().join(", "));
        N.println("---------stream0-----------");
        N.println(sheet.streamV().join("-"));
        N.println("---------stream2-----------");
        N.println(sheet.streamR().join("="));
        N.println("---------stream02-----------");
        N.println(sheet.streamC().join("#"));
        N.println("---------cells-----------");
        N.println(sheet.cellsH().join("&"));
        N.println("---------cells0-----------");
        N.println(sheet.cellsV().join("|"));
        N.println("---------set row-----------");
        sheet.setRow("106", N.asList(111, 112, 113));
        sheet.println();
        N.println("---------add row-----------");
        sheet.addRow("109", N.asList(111, 112, 113));
        sheet.println();
        N.println("---------remove row-----------");
        sheet.removeRow("106");
        sheet.println();
        N.println("---------set column-----------");
        sheet.setColumn("bb", IntList.repeat(111, 8).toList());
        sheet.println();
        N.println("---------add column-----------");
        sheet.addColumn("dd", IntList.repeat(112, 8).toList());
        sheet.println();
        N.println("---------remove column-----------");
        sheet.removeColumn("bb");
        sheet.println();
        N.println("*****************************");
    }

    @Test
    public void test_02() {
        Set<String> columnKeySet = N.asLinkedHashSet("aa");
        Set<String> rowKeySet = N.asLinkedHashSet("11");
        Sheet<String, String, String> sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("11", "aa", "11aa");

        N.println(sheet);

        sheet.addRow("22", N.asList("22aa"));

        N.println(sheet);

        sheet.addColumn("bb", N.asList("11bb", "22bb"));

        N.println(sheet);
    }

    @Test
    public void test_03() {
        Set<String> rowKeySet = N.asLinkedHashSet("11");
        Set<String> columnKeySet = N.asLinkedHashSet("aa");
        Sheet<String, String, String> sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("11", "aa", "11aa");

        sheet.addRow("22", N.asList("22aa"));

        N.println(sheet.row("11"));

        N.println(sheet.rowMap());

        N.println(sheet.column("aa"));

        N.println(sheet.columnMap());

        Sheet<String, String, String> sheet2 = new Sheet<>(N.asLinkedHashSet("11", "22", "33"), N.asLinkedHashSet("aa"));

        sheet2.putAll(sheet);

        N.println(sheet2);

        sheet2.addColumn("bb", N.asList("11bb", "22bb", "33bb"));

        N.println(sheet2);
    }

    @Test
    public void test_copy() {
        Set<String> rowKeySet = N.asLinkedHashSet("11");
        Set<String> columnKeySet = N.asLinkedHashSet("aa");
        Sheet<String, String, String> sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("11", "aa", "11aa");

        sheet.addRow("22", N.asList("22aa"));

        Sheet<String, String, String> copy = sheet.copy();
        N.println(sheet);
        N.println(copy);

        assertEquals(sheet, copy);
    }

    @Test
    public void test_stringOf() {
        Type<Sheet<String, String, String>> type = N.typeOf(Sheet.class);

        Set<String> rowKeySet = N.asLinkedHashSet("R1");
        Set<String> columnKeySet = N.asLinkedHashSet("C1");
        Sheet<String, String, String> sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("R1", "C1", "11");

        String str = type.stringOf(sheet);

        N.println(str);

        Sheet<String, String, String> sheet2 = type.valueOf(str);
        N.println(sheet2);
        assertEquals(sheet, sheet2);

        rowKeySet = N.asLinkedHashSet("R1", "R2");
        columnKeySet = N.asLinkedHashSet("C1", "C2");
        sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("R1", "C1", "11");
        sheet.put("R1", "C2", "12");
        sheet.put("R2", "C1", "21");
        sheet.put("R2", "C2", "22");

        str = type.stringOf(sheet);

        N.println(str);

        sheet2 = type.valueOf(str);
        N.println(sheet2);
        assertEquals(sheet, sheet2);
    }

    @Test
    public void test_stringOf_2() {
        Type<Sheet<String, String, Integer>> type = new TypeReference<Sheet<String, String, Integer>>() {
        }.type();

        Set<String> rowKeySet = N.asLinkedHashSet("R1");
        Set<String> columnKeySet = N.asLinkedHashSet("C1");
        Sheet<String, String, Integer> sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("R1", "C1", 11);

        String str = type.stringOf(sheet);

        N.println(str);

        Sheet<String, String, Integer> sheet2 = type.valueOf(str);
        N.println(sheet2);
        assertEquals(sheet, sheet2);

        rowKeySet = N.asLinkedHashSet("R1", "R2");
        columnKeySet = N.asLinkedHashSet("C1", "C2");
        sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("R1", "C1", 11);
        sheet.put("R1", "C2", 12);
        sheet.put("R2", "C1", 21);
        sheet.put("R2", "C2", 22);

        str = type.stringOf(sheet);

        N.println(str);

        sheet2 = type.valueOf(str);
        N.println(sheet2);
        assertEquals(sheet, sheet2);
    }

    @Test
    public void test_stringOf_3() {
        Type<Sheet<String, Float, Float>> type = new TypeReference<Sheet<String, Float, Float>>() {
        }.type();

        Set<String> rowKeySet = N.asLinkedHashSet("R1");
        Set<Float> columnKeySet = N.asLinkedHashSet(1.11f);
        Sheet<String, Float, Float> sheet = new Sheet<>(rowKeySet, columnKeySet);

        sheet.put("R1", 1.11f, 2.22f);

        String str = type.stringOf(sheet);

        N.println(str);

        Sheet<String, Float, Float> sheet2 = type.valueOf(str);
        N.println(sheet2);
        assertEquals(sheet, sheet2);
    }

    Sheet<String, String, Number> createSheet() {
        Set<String> columnKeySet = N.asLinkedHashSet("aa", "bb", "cc");
        Set<String> rowKeySet = N.asLinkedHashSet("101", "102", "103", "104", "105", "106", "107", "108");
        Sheet<String, String, Number> sheet = new Sheet<>(rowKeySet, columnKeySet);

        int i = 100;
        for (String rowKey : rowKeySet) {
            for (String columnKey : columnKeySet) {
                sheet.put(rowKey, columnKey, i++);
            }
        }

        return sheet;
    }
}

package com.rocksdb.api.column_families;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnFamiliesApi {

    /***
     * todo: 九师兄  2024/10/17 18:53
     * 测试点：想测试列族的使用，但是没有运行成功
     *
     * org.rocksdb.RocksDBException: Column family not found: : cf1
     * 	at org.rocksdb.RocksDB.open(Native Method)
     * 	at org.rocksdb.RocksDB.open(RocksDB.java:286)
     * 	at com.rocksdb.api.column_families.ColumnFamiliesApi.main(ColumnFamiliesApi.java:25)
     *
     */
    public void oneTest(String[] args) {
// 初始化 RocksDB 库
        RocksDB.loadLibrary();

        try (final DBOptions options = new DBOptions().setCreateIfMissing(true)) {
            // 定义列族及其选项
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()));

            // 创建一个空的列族句柄列表
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            // 打开数据库并获取列族句柄
            try (final RocksDB db = RocksDB.open(options, "/Users/lcc/temp/rocksdb/testdb", columnFamilyDescriptors, columnFamilyHandles)) {
                // 输出列族句柄的数量
                System.out.println("Number of column family handles: " + columnFamilyHandles.size());

                // 写入数据
                try (WriteOptions writeOptions = new WriteOptions()) {
                    db.put(columnFamilyHandles.get(0), writeOptions, "key1".getBytes(), "value1".getBytes());
                    db.put(columnFamilyHandles.get(1), writeOptions, "key2".getBytes(), "value2".getBytes());
                    db.put(columnFamilyHandles.get(2), writeOptions, "key3".getBytes(), "value3".getBytes());
                }

                // 读取数据
                try (ReadOptions readOptions = new ReadOptions()) {
                    byte[] value1 = db.get(columnFamilyHandles.get(0), readOptions, "key1".getBytes());
                    byte[] value2 = db.get(columnFamilyHandles.get(1), readOptions, "key2".getBytes());
                    byte[] value3 = db.get(columnFamilyHandles.get(2), readOptions, "key3".getBytes());

                    System.out.println("Value from default CF: " + (value1 != null ? new String(value1) : "null"));
                    System.out.println("Value from cf1: " + (value2 != null ? new String(value2) : "null"));
                    System.out.println("Value from cf2: " + (value3 != null ? new String(value3) : "null"));
                }
            } finally {
                // 关闭所有列族句柄
                for (ColumnFamilyHandle handle : columnFamilyHandles) {
                    handle.close();
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    /***
     * todo: 九师兄  2024/10/17 19:33
     * 测试点：测试多列族使用
     *
     * 首先清空目录
     * lcc@lcc testdb$ rm -rf /Users/lcc/temp/rocksdb/testdb/*
     * 然后执行结果如下
     *
     * Number of column family handles: 3
     * Value from default CF: value1
     * Value from cf1: value2
     * Value from cf2: value3
     * Assertion failed: (refs_.load(std::memory_order_relaxed) == 0), function ~ColumnFamilyData, file db/column_family.cc, line 443.
     *
     * 但是第二次再次执行报错
     *
     * org.rocksdb.RocksDBException: You have to open all column families. Column families not opened: cf2, cf1
     * 	at org.rocksdb.RocksDB.open(Native Method)
     * 	at org.rocksdb.RocksDB.open(RocksDB.java:231)
     * 	at com.rocksdb.api.column_families.ColumnFamiliesApi.twoMethod(ColumnFamiliesApi.java:103)
     * 	at com.rocksdb.api.column_families.ColumnFamiliesApiTest.twoMethod(ColumnFamiliesApiTest.java:13)
     *
     */
    public void twoMethod() {
        // 初始化 RocksDB 库
        RocksDB.loadLibrary();


        try (final Options options = new Options().setCreateIfMissing(true)) {
            // 定义列族及其选项
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()));

            // 创建一个空的列族句柄列表
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            // 打开数据库并获取列族句柄
            try (final RocksDB db = RocksDB.open(options, "/Users/lcc/temp/rocksdb/testdb")) {

                ColumnFamilyHandle columnFamilyA = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
                ColumnFamilyHandle columnFamilyB = db.createColumnFamily(new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()));

                ColumnFamilyHandle defaultColumnFamily = db.getDefaultColumnFamily();
                columnFamilyHandles.add(defaultColumnFamily);
                columnFamilyHandles.add(columnFamilyA);
                columnFamilyHandles.add(columnFamilyB);

                // 输出列族句柄的数量
                System.out.println("Number of column family handles: " + columnFamilyHandles.size());

                // 写入数据
                try (WriteOptions writeOptions = new WriteOptions()) {
                    db.put(columnFamilyHandles.get(0), writeOptions, "key1".getBytes(), "value1".getBytes());
                    db.put(columnFamilyHandles.get(1), writeOptions, "key2".getBytes(), "value2".getBytes());
                    db.put(columnFamilyHandles.get(2), writeOptions, "key3".getBytes(), "value3".getBytes());
                }

                // 读取数据
                try (ReadOptions readOptions = new ReadOptions()) {
                    byte[] value1 = db.get(columnFamilyHandles.get(0), readOptions, "key1".getBytes());
                    byte[] value2 = db.get(columnFamilyHandles.get(1), readOptions, "key2".getBytes());
                    byte[] value3 = db.get(columnFamilyHandles.get(2), readOptions, "key3".getBytes());

                    System.out.println("Value from default CF: " + (value1 != null ? new String(value1) : "null"));
                    System.out.println("Value from cf1: " + (value2 != null ? new String(value2) : "null"));
                    System.out.println("Value from cf2: " + (value3 != null ? new String(value3) : "null"));
                }
            } finally {
                // 关闭所有列族句柄
                for (ColumnFamilyHandle handle : columnFamilyHandles) {
                    handle.close();
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /***
     * todo: 九师兄  2024/10/17 19:52
     * 测试点：重启测试每次open 还是有问题
     * org.rocksdb.RocksDBException: You have to open all column families. Column families not opened: cf2, cf1
     *
     * 	at org.rocksdb.RocksDB.open(Native Method)
     * 	at org.rocksdb.RocksDB.open(RocksDB.java:231)
     * 	at com.rocksdb.api.column_families.ColumnFamiliesApi.createColumnFamily(ColumnFamiliesApi.java:233)
     */
    public void threeMethod() throws RocksDBException {
        // 初始化 RocksDB 库
        RocksDB.loadLibrary();

        // 定义列族及其选项
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()));

        // 检测 ColumnFamily 是否存在 不存在 那么创建
        List<byte[]> bytes = RocksDB.listColumnFamilies(new Options(), "/Users/lcc/temp/rocksdb/testdb");
        List<String> collect = bytes.stream().map(x -> new String(x)).collect(Collectors.toList());

        createColumnFamily(collect, columnFamilyDescriptors);
        getColumnFamily(collect, columnFamilyDescriptors);

        try (final Options options = new Options().setCreateIfMissing(true)) {

            // 创建一个空的列族句柄列表
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            // 打开数据库并获取列族句柄
            try (final RocksDB db = RocksDB.open(options, "/Users/lcc/temp/rocksdb/testdb")) {

                ColumnFamilyHandle columnFamilyA = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
                ColumnFamilyHandle columnFamilyB = db.createColumnFamily(new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()));

                ColumnFamilyHandle defaultColumnFamily = db.getDefaultColumnFamily();
                columnFamilyHandles.add(defaultColumnFamily);
                columnFamilyHandles.add(columnFamilyA);
                columnFamilyHandles.add(columnFamilyB);

                // 输出列族句柄的数量
                System.out.println("Number of column family handles: " + columnFamilyHandles.size());

                // 写入数据
                try (WriteOptions writeOptions = new WriteOptions()) {
                    db.put(columnFamilyHandles.get(0), writeOptions, "key1".getBytes(), "value1".getBytes());
                    db.put(columnFamilyHandles.get(1), writeOptions, "key2".getBytes(), "value2".getBytes());
                    db.put(columnFamilyHandles.get(2), writeOptions, "key3".getBytes(), "value3".getBytes());
                }

                // 读取数据
                try (ReadOptions readOptions = new ReadOptions()) {
                    byte[] value1 = db.get(columnFamilyHandles.get(0), readOptions, "key1".getBytes());
                    byte[] value2 = db.get(columnFamilyHandles.get(1), readOptions, "key2".getBytes());
                    byte[] value3 = db.get(columnFamilyHandles.get(2), readOptions, "key3".getBytes());

                    System.out.println("Value from default CF: " + (value1 != null ? new String(value1) : "null"));
                    System.out.println("Value from cf1: " + (value2 != null ? new String(value2) : "null"));
                    System.out.println("Value from cf2: " + (value3 != null ? new String(value3) : "null"));
                }
            } finally {
                // 关闭所有列族句柄
                for (ColumnFamilyHandle handle : columnFamilyHandles) {
                    handle.close();
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private void getColumnFamily(List<String> collect, List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final Options options = new Options().setCreateIfMissing(true);
        // 打开数据库并获取列族句柄
        final RocksDB db = RocksDB.open(options, "/Users/lcc/temp/rocksdb/testdb");
        for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
            if (collect.contains(columnFamilyDescriptor.columnFamilyName())) {
                // 根据 collect 或者 columnFamilyDescriptor 获取 ColumnFamilyHandle
                // 没有 db.getColumnFamilyHandle 这个方法
                ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(columnFamilyDescriptor);
                continue;
            }
            db.createColumnFamily(columnFamilyDescriptor);
        }
        // 获取 rocksdb 所有的 ColumnFamilyHandle
        options.close();
    }

    private void createColumnFamily(List<String> collect, List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final Options options = new Options().setCreateIfMissing(true);
        // 打开数据库并获取列族句柄
        final RocksDB db = RocksDB.open(options, "/Users/lcc/temp/rocksdb/testdb");
        for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
            if (collect.contains(columnFamilyDescriptor.columnFamilyName())) {
                continue;
            }
            String name = new String(columnFamilyDescriptor.getName());
            if (name.equals("default")) {
                continue;
            }
            System.out.println("准备创建:" + name);
            db.createColumnFamily(columnFamilyDescriptor);
        }
        // 获取 rocksdb 所有的 ColumnFamilyHandle
        options.close();
        db.close();
    }
}
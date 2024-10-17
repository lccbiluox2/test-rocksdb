package com.rocksdb.api.column_families;

import org.junit.Test;
import org.rocksdb.RocksDBException;

import static org.junit.Assert.*;

public class ColumnFamiliesApiTest {

    private ColumnFamiliesApi columnFamiliesApi = new ColumnFamiliesApi();

    @Test
    public void twoMethod() {
        columnFamiliesApi.twoMethod();
    }

    @Test
    public void threeMethod() throws RocksDBException {
        columnFamiliesApi.threeMethod();
    }
}
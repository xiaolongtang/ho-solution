package com.example.h2sync.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlUtilsTest {

    @Test
    void isSafeSelectRequiresNonEmptySelectWithoutMutations() {
        assertFalse(SqlUtils.isSafeSelect(null));
        assertFalse(SqlUtils.isSafeSelect(""));
        assertFalse(SqlUtils.isSafeSelect("   \n   "));
        assertFalse(SqlUtils.isSafeSelect("DELETE FROM test"));
        assertFalse(SqlUtils.isSafeSelect("SELECT * FROM dual;"));
        assertFalse(SqlUtils.isSafeSelect("SELECT * FROM users WHERE name = 'drop table'"));
        assertFalse(SqlUtils.isSafeSelect("SELECT * FROM test RUNSCRIPT from '/tmp'"));
    }

    @Test
    void isSafeSelectAcceptsSimpleSelects() {
        assertTrue(SqlUtils.isSafeSelect("select 1"));
        assertTrue(SqlUtils.isSafeSelect("  \n Select * from demo where id = 1  \n"));
        assertTrue(SqlUtils.isSafeSelect("SELECT name FROM people WHERE status = 'active'"));
    }
}

package com.example.h2sync.util;

public final class SqlUtils {
    private SqlUtils(){}

    public static boolean isSafeSelect(String sql) {
        if (sql == null) return false;
        String s = sql.trim();
        if (s.isEmpty()) return false;
        if (s.contains(";")) return false;
        String upper = s.toUpperCase();
        if (!upper.startsWith("SELECT")) return false;
        String[] forbidden = new String[]{"INSERT ", "UPDATE ", "DELETE ", "MERGE ", "CREATE ", "DROP ", "ALTER ", "TRUNCATE ", "GRANT ", "REVOKE ", "CALL ", "EXEC", "RUNSCRIPT", "SCRIPT "};
        for (String f : forbidden) {
            if (upper.contains(f)) return false;
        }
        return true;
    }
}

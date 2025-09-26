package com.example.h2sync.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Translates Oracle view SQL definitions into H2-compatible SQL.
 */
class OracleViewSqlTranslator {

    private static final Set<String> H2_RESERVED_IDENTIFIERS = Set.of("VALUE", "TYPE");

    private final String oracleSchema;

    OracleViewSqlTranslator(String oracleSchema) {
        this.oracleSchema = oracleSchema;
    }

    String translate(String oracleSql) {
        if (oracleSql == null) {
            throw new IllegalArgumentException("Oracle view SQL is null");
        }
        String stripped = stripSqlTerminator(stripComments(oracleSql)).trim();
        if (stripped.isEmpty()) {
            throw new IllegalArgumentException("Oracle view SQL is empty after cleaning");
        }
        String withoutSchema = removeOracleSchemaQualifiers(stripped);
        String cleaned = removeTrailingReadOnly(withoutSchema).trim();
        String uppercased = uppercaseUnquotedIdentifiers(cleaned);
        String protectedKeywords = quoteH2ReservedKeywords(uppercased);
        return replaceOracleSpecificFunctions(protectedKeywords);
    }

    private String stripSqlTerminator(String sql) {
        String out = sql.trim();
        while (out.endsWith(";") || out.endsWith("/")) {
            out = out.substring(0, out.length() - 1).trim();
        }
        return out;
    }

    private String stripComments(String sql) {
        String noBlock = sql.replaceAll("(?is)/\\*.*?\\*/", " ");
        StringBuilder sb = new StringBuilder();
        try (Scanner scanner = new Scanner(noBlock)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                int idx = line.indexOf("--");
                if (idx >= 0) {
                    line = line.substring(0, idx);
                }
                if (!line.isBlank()) {
                    if (sb.length() > 0) {
                        sb.append('\n');
                    }
                    sb.append(line);
                }
            }
        }
        return sb.toString();
    }

    private String removeOracleSchemaQualifiers(String sql) {
        if (oracleSchema == null || oracleSchema.isBlank()) {
            return sql;
        }
        String result = sql;
        String quotedSchema = Pattern.quote("\"" + oracleSchema + "\"");
        result = result.replaceAll("(?i)" + quotedSchema + "\\.", "");
        result = result.replaceAll("(?i)" + Pattern.quote(oracleSchema) + "\\.", "");
        return result;
    }

    private String removeTrailingReadOnly(String sql) {
        return sql.replaceAll("(?i)WITH\\s+READ\\s+ONLY", " ")
                .replaceAll("(?i)WITH\\s+CHECK\\s+OPTION", " ");
    }

    private String uppercaseUnquotedIdentifiers(String sql) {
        StringBuilder result = new StringBuilder(sql.length());
        int len = sql.length();
        int i = 0;
        while (i < len) {
            char c = sql.charAt(i);
            if (c == '\'') {
                result.append(c);
                i++;
                while (i < len) {
                    char current = sql.charAt(i);
                    result.append(current);
                    if (current == '\'') {
                        if (i + 1 < len && sql.charAt(i + 1) == '\'') {
                            result.append(sql.charAt(i + 1));
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    i++;
                }
            } else if (c == '"') {
                result.append(c);
                i++;
                while (i < len) {
                    char current = sql.charAt(i);
                    result.append(current);
                    if (current == '"') {
                        if (i + 1 < len && sql.charAt(i + 1) == '"') {
                            result.append(sql.charAt(i + 1));
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    i++;
                }
            } else if (Character.isLetter(c) || c == '_' || c == '$' || c == '#') {
                int start = i;
                i++;
                while (i < len) {
                    char current = sql.charAt(i);
                    if (Character.isLetterOrDigit(current) || current == '_' || current == '$' || current == '#') {
                        i++;
                    } else {
                        break;
                    }
                }
                result.append(sql.substring(start, i).toUpperCase(Locale.ROOT));
            } else {
                result.append(c);
                i++;
            }
        }
        return result.toString();
    }

    private String quoteH2ReservedKeywords(String sql) {
        StringBuilder result = new StringBuilder(sql.length() + 16);
        int len = sql.length();
        int i = 0;
        while (i < len) {
            char c = sql.charAt(i);
            if (c == '\'') {
                result.append(c);
                i++;
                while (i < len) {
                    char current = sql.charAt(i);
                    result.append(current);
                    if (current == '\'') {
                        if (i + 1 < len && sql.charAt(i + 1) == '\'') {
                            result.append(sql.charAt(i + 1));
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    i++;
                }
            } else if (c == '"') {
                result.append(c);
                i++;
                while (i < len) {
                    char current = sql.charAt(i);
                    result.append(current);
                    if (current == '"') {
                        if (i + 1 < len && sql.charAt(i + 1) == '"') {
                            result.append(sql.charAt(i + 1));
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    i++;
                }
            } else if (Character.isLetter(c) || c == '_' || c == '$' || c == '#') {
                int start = i;
                i++;
                while (i < len) {
                    char current = sql.charAt(i);
                    if (Character.isLetterOrDigit(current) || current == '_' || current == '$' || current == '#') {
                        i++;
                    } else {
                        break;
                    }
                }
                String token = sql.substring(start, i);
                if (H2_RESERVED_IDENTIFIERS.contains(token)) {
                    result.append('"').append(token).append('"');
                } else {
                    result.append(token);
                }
            } else {
                result.append(c);
                i++;
            }
        }
        return result.toString();
    }

    private String replaceOracleSpecificFunctions(String sql) {
        return replaceNvl2(sql);
    }

    private String replaceNvl2(String sql) {
        StringBuilder result = new StringBuilder(sql.length());
        int len = sql.length();
        int i = 0;
        while (i < len) {
            if (matchesFunction(sql, i, "NVL2")) {
                int funcStart = i;
                i += 4;
                int afterName = skipWhitespace(sql, i);
                if (afterName >= len || sql.charAt(afterName) != '(') {
                    result.append(sql, funcStart, i);
                    continue;
                }
                int openParenIndex = afterName;
                int closeParenIndex = findMatchingParenthesis(sql, openParenIndex);
                if (closeParenIndex == -1) {
                    result.append(sql, funcStart, i);
                    continue;
                }
                String inner = sql.substring(openParenIndex + 1, closeParenIndex);
                List<String> args = splitTopLevelArguments(inner);
                if (args.size() != 3) {
                    result.append(sql, funcStart, closeParenIndex + 1);
                    i = closeParenIndex + 1;
                    continue;
                }
                String expr = replaceNvl2(args.get(0).trim());
                String notNullValue = replaceNvl2(args.get(1).trim());
                String nullValue = replaceNvl2(args.get(2).trim());
                result.append("CASE WHEN ")
                        .append(expr)
                        .append(" IS NOT NULL THEN ")
                        .append(notNullValue)
                        .append(" ELSE ")
                        .append(nullValue)
                        .append(" END");
                i = closeParenIndex + 1;
            } else {
                result.append(sql.charAt(i));
                i++;
            }
        }
        return result.toString();
    }

    private boolean matchesFunction(String sql, int offset, String name) {
        int len = sql.length();
        int nameLen = name.length();
        if (offset + nameLen > len) {
            return false;
        }
        for (int i = 0; i < nameLen; i++) {
            char c = sql.charAt(offset + i);
            if (Character.toUpperCase(c) != name.charAt(i)) {
                return false;
            }
        }
        if (offset > 0) {
            char prev = sql.charAt(offset - 1);
            if (Character.isLetterOrDigit(prev) || prev == '_' || prev == '$' || prev == '#') {
                return false;
            }
        }
        if (offset + nameLen < len) {
            char next = sql.charAt(offset + nameLen);
            if (Character.isLetterOrDigit(next) || next == '_' || next == '$' || next == '#') {
                return false;
            }
        }
        return true;
    }

    private int skipWhitespace(String sql, int index) {
        int len = sql.length();
        int i = index;
        while (i < len && Character.isWhitespace(sql.charAt(i))) {
            i++;
        }
        return i;
    }

    private int findMatchingParenthesis(String sql, int openIndex) {
        int len = sql.length();
        int depth = 0;
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = openIndex; i < len; i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inDouble) {
                if (inSingle && i + 1 < len && sql.charAt(i + 1) == '\'') {
                    i++;
                } else {
                    inSingle = !inSingle;
                }
                continue;
            }
            if (c == '"' && !inSingle) {
                if (inDouble && i + 1 < len && sql.charAt(i + 1) == '"') {
                    i++;
                } else {
                    inDouble = !inDouble;
                }
                continue;
            }
            if (inSingle || inDouble) {
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
                if (depth < 0) {
                    return -1;
                }
            }
        }
        return -1;
    }

    private List<String> splitTopLevelArguments(String input) {
        List<String> parts = new ArrayList<>();
        int len = input.length();
        int start = 0;
        int depth = 0;
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < len; i++) {
            char c = input.charAt(i);
            if (c == '\'' && !inDouble) {
                if (inSingle && i + 1 < len && input.charAt(i + 1) == '\'') {
                    i++;
                } else {
                    inSingle = !inSingle;
                }
                continue;
            }
            if (c == '"' && !inSingle) {
                if (inDouble && i + 1 < len && input.charAt(i + 1) == '"') {
                    i++;
                } else {
                    inDouble = !inDouble;
                }
                continue;
            }
            if (inSingle || inDouble) {
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(input.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(input.substring(start));
        return parts;
    }
}

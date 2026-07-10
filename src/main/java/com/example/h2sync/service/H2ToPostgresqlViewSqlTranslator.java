package com.example.h2sync.service;

import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Applies the small set of dialect changes normally needed by views produced by
 * the Oracle-to-H2 loader before those views are created in PostgreSQL.
 */
final class H2ToPostgresqlViewSqlTranslator {

    private final String sourceSchema;
    private final String targetSchema;

    H2ToPostgresqlViewSqlTranslator(String sourceSchema, String targetSchema) {
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
    }

    String translate(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("H2 view definition is blank");
        }

        String translated = stripSqlTerminator(sql);
        translated = replaceSchemaQualifier(translated);
        translated = replaceTokenOutsideQuotes(translated, "NVL", "COALESCE", true);
        translated = replaceTokenOutsideQuotes(translated, "IFNULL", "COALESCE", true);
        translated = replaceTokenOutsideQuotes(translated, "SYSDATE", "CURRENT_TIMESTAMP", false);
        translated = replaceTokenOutsideQuotes(translated, "SYSTIMESTAMP", "CURRENT_TIMESTAMP", false);
        translated = replaceTokenOutsideQuotes(translated, "MINUS", "EXCEPT", false);
        translated = translated.replaceAll("(?i)\\bCURRENT_TIMESTAMP\\s*\\(\\s*\\)", "CURRENT_TIMESTAMP")
                .replaceAll("(?i)\\bCURRENT_DATE\\s*\\(\\s*\\)", "CURRENT_DATE")
                .replaceAll("(?i)\\bCURRENT_TIME\\s*\\(\\s*\\)", "CURRENT_TIME");
        translated = rewriteCanonicalH2Functions(translated);

        // H2's Oracle mode exposes DUAL. PostgreSQL does not require it for
        // simple scalar SELECT statements.
        translated = translated.replaceAll(
                "(?i)\\s+FROM\\s+(?:\\\"?" + Pattern.quote(targetSchema) + "\\\"?\\.)?\\\"?DUAL\\\"?(?=\\s*$)",
                ""
        );
        return translated.trim();
    }

    private String rewriteCanonicalH2Functions(String sql) {
        StringBuilder out = new StringBuilder(sql.length() + 32);
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"') {
                i = appendQuoted(sql, i, out);
                continue;
            }
            if (!isIdentifierStart(c)) {
                out.append(c);
                i++;
                continue;
            }

            int tokenStart = i++;
            while (i < sql.length() && isIdentifierPart(sql.charAt(i))) i++;
            String token = sql.substring(tokenStart, i);
            String upper = token.toUpperCase(Locale.ROOT);
            int open = skipWhitespace(sql, i);
            if (open >= sql.length() || sql.charAt(open) != '(' || !isRewrittenFunction(upper)) {
                out.append(token);
                continue;
            }

            int close = findMatchingParenthesis(sql, open);
            if (close < 0) {
                out.append(token);
                continue;
            }
            List<String> args = splitTopLevelArguments(sql.substring(open + 1, close)).stream()
                    .map(String::trim)
                    .map(this::rewriteCanonicalH2Functions)
                    .toList();

            if ("LISTAGG".equals(upper) && args.size() == 2) {
                WithinGroup withinGroup = readWithinGroup(sql, close + 1);
                if (withinGroup != null) {
                    out.append("STRING_AGG(").append(args.get(0)).append(", ").append(args.get(1))
                            .append(" ORDER BY ")
                            .append(rewriteCanonicalH2Functions(withinGroup.orderBy()))
                            .append(')');
                    i = withinGroup.endIndex();
                    continue;
                }
            }

            String replacement = rewriteFunction(upper, args);
            if (replacement == null) {
                out.append(token).append(sql, i, close + 1);
            } else {
                out.append(replacement);
            }
            i = close + 1;
        }
        return out.toString();
    }

    private String rewriteFunction(String function, List<String> args) {
        if ("DATE_TRUNC".equals(function) && args.size() == 2) {
            String unit = args.get(0);
            if (unit.matches("(?i)[A-Z_]+")) {
                unit = "'" + unit.toLowerCase(Locale.ROOT) + "'";
            }
            return "DATE_TRUNC(" + unit + ", " + args.get(1) + ")";
        }
        if ("ADD_MONTHS".equals(function) && args.size() == 2) {
            return "(" + args.get(0) + " + (" + args.get(1) + ") * INTERVAL '1 month')";
        }
        if ("LAST_DAY".equals(function) && args.size() == 1) {
            return "(DATE_TRUNC('month', " + args.get(0) +
                    ") + INTERVAL '1 month - 1 day')::date";
        }
        if ("RAWTOHEX".equals(function) && args.size() == 1) {
            return "ENCODE(" + args.get(0) + ", 'hex')";
        }
        if ("HEXTORAW".equals(function) && args.size() == 1) {
            return "DECODE(" + args.get(0) + ", 'hex')";
        }
        if ("LENGTHB".equals(function) && args.size() == 1) {
            return "OCTET_LENGTH(" + args.get(0) + ")";
        }
        return null;
    }

    private boolean isRewrittenFunction(String token) {
        return token.equals("DATE_TRUNC") || token.equals("ADD_MONTHS") || token.equals("LAST_DAY")
                || token.equals("LISTAGG") || token.equals("RAWTOHEX") || token.equals("HEXTORAW")
                || token.equals("LENGTHB");
    }

    private WithinGroup readWithinGroup(String sql, int start) {
        int i = skipWhitespace(sql, start);
        if (!regionMatchesToken(sql, i, "WITHIN")) return null;
        i = skipWhitespace(sql, i + "WITHIN".length());
        if (!regionMatchesToken(sql, i, "GROUP")) return null;
        i = skipWhitespace(sql, i + "GROUP".length());
        if (i >= sql.length() || sql.charAt(i) != '(') return null;
        int close = findMatchingParenthesis(sql, i);
        if (close < 0) return null;
        String content = sql.substring(i + 1, close).trim();
        if (!content.regionMatches(true, 0, "ORDER", 0, "ORDER".length())) return null;
        int by = skipWhitespace(content, "ORDER".length());
        if (!content.regionMatches(true, by, "BY", 0, 2)) return null;
        return new WithinGroup(content.substring(by + 2).trim(), close + 1);
    }

    private boolean regionMatchesToken(String sql, int offset, String token) {
        if (offset < 0 || offset + token.length() > sql.length()
                || !sql.regionMatches(true, offset, token, 0, token.length())) {
            return false;
        }
        int end = offset + token.length();
        return (offset == 0 || !isIdentifierPart(sql.charAt(offset - 1)))
                && (end == sql.length() || !isIdentifierPart(sql.charAt(end)));
    }

    private int appendQuoted(String sql, int start, StringBuilder out) {
        char quote = sql.charAt(start);
        int i = start;
        out.append(quote);
        i++;
        while (i < sql.length()) {
            char current = sql.charAt(i);
            out.append(current);
            if (current == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    out.append(sql.charAt(i + 1));
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return i;
    }

    private int skipWhitespace(String sql, int index) {
        int i = index;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) i++;
        return i;
    }

    private int findMatchingParenthesis(String sql, int open) {
        int depth = 0;
        boolean singleQuoted = false;
        boolean doubleQuoted = false;
        for (int i = open; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !doubleQuoted) {
                if (singleQuoted && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') i++;
                else singleQuoted = !singleQuoted;
                continue;
            }
            if (c == '"' && !singleQuoted) {
                if (doubleQuoted && i + 1 < sql.length() && sql.charAt(i + 1) == '"') i++;
                else doubleQuoted = !doubleQuoted;
                continue;
            }
            if (singleQuoted || doubleQuoted) continue;
            if (c == '(') depth++;
            else if (c == ')' && --depth == 0) return i;
        }
        return -1;
    }

    private List<String> splitTopLevelArguments(String input) {
        List<String> args = new ArrayList<>();
        int start = 0;
        int depth = 0;
        boolean singleQuoted = false;
        boolean doubleQuoted = false;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '\'' && !doubleQuoted) {
                if (singleQuoted && i + 1 < input.length() && input.charAt(i + 1) == '\'') i++;
                else singleQuoted = !singleQuoted;
                continue;
            }
            if (c == '"' && !singleQuoted) {
                if (doubleQuoted && i + 1 < input.length() && input.charAt(i + 1) == '"') i++;
                else doubleQuoted = !doubleQuoted;
                continue;
            }
            if (singleQuoted || doubleQuoted) continue;
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                args.add(input.substring(start, i));
                start = i + 1;
            }
        }
        args.add(input.substring(start));
        return args;
    }

    private String stripSqlTerminator(String sql) {
        String out = sql.trim();
        while (out.endsWith(";") || out.endsWith("/")) {
            out = out.substring(0, out.length() - 1).trim();
        }
        return out;
    }

    private String replaceSchemaQualifier(String sql) {
        if (sourceSchema == null || sourceSchema.isBlank()) {
            return sql;
        }
        String quotedSource = Pattern.quote("\"" + sourceSchema + "\"");
        String quotedTarget = "\"" + targetSchema.replace("\"", "\"\"") + "\"";
        String result = sql.replaceAll(
                "(?i)" + quotedSource + "\\s*\\.",
                Matcher.quoteReplacement(quotedTarget + ".")
        );
        return result.replaceAll(
                "(?i)(?<![A-Z0-9_$#\"])(" + Pattern.quote(sourceSchema) + ")\\s*\\.",
                Matcher.quoteReplacement(quotedTarget + ".")
        );
    }

    private String replaceTokenOutsideQuotes(String sql, String source, String replacement, boolean functionOnly) {
        StringBuilder out = new StringBuilder(sql.length() + 16);
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"') {
                char quote = c;
                out.append(c);
                i++;
                while (i < sql.length()) {
                    char current = sql.charAt(i);
                    out.append(current);
                    if (current == quote) {
                        if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                            out.append(sql.charAt(i + 1));
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    i++;
                }
                continue;
            }

            if (isIdentifierStart(c)) {
                int start = i++;
                while (i < sql.length() && isIdentifierPart(sql.charAt(i))) {
                    i++;
                }
                String token = sql.substring(start, i);
                int next = i;
                while (next < sql.length() && Character.isWhitespace(sql.charAt(next))) {
                    next++;
                }
                if (token.toUpperCase(Locale.ROOT).equals(source)
                        && (!functionOnly || (next < sql.length() && sql.charAt(next) == '('))) {
                    out.append(replacement);
                } else {
                    out.append(token);
                }
                continue;
            }

            out.append(c);
            i++;
        }
        return out.toString();
    }

    private boolean isIdentifierStart(char c) {
        return Character.isLetter(c) || c == '_' || c == '$' || c == '#';
    }

    private boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
    }

    private record WithinGroup(String orderBy, int endIndex) {
    }
}

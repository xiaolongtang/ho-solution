package com.example.h2sync.controller;

import com.example.h2sync.dto.SqlRequest;
import com.example.h2sync.util.SqlUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.StringJoiner;

@RestController
@RequestMapping("/api/query")
public class QueryController {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public QueryController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Operation(
            summary = "Run a SELECT query on H2",
            description = "Accepts a single SELECT statement in the request body and streams results as CSV-like lines (comma-separated).",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    required = true,
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = SqlRequest.class))
            ),
            responses = {
                    @ApiResponse(responseCode = "200", description = "Query result streamed as text/plain"),
                    @ApiResponse(responseCode = "400", description = "Invalid SQL"),
            }
    )
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
    public void query(@Valid @RequestBody SqlRequest req, HttpServletResponse response) throws IOException {
        final String sql = req.getSql();
        if (!SqlUtils.isSafeSelect(sql)) {
            response.sendError(400, "Only a single SELECT statement is allowed.");
            return;
        }
        response.setContentType("text/plain; charset=UTF-8");
        jdbcTemplate.query(sql, rs -> {
            try {
                writeRow(rs, response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        response.getWriter().flush();
    }

    private void writeRow(ResultSet rs, HttpServletResponse response) throws SQLException, IOException {
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        StringJoiner joiner = new StringJoiner(",");
        for (int i = 1; i <= cols; i++) {
            Object val = rs.getObject(i);
            joiner.add(val == null ? "" : escape(val.toString()));
        }
        response.getWriter().write(joiner.toString());
        response.getWriter().write("\n");
    }

    private String escape(String s) {
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }
}

# H2 PostgreSQL Sync (Spring Boot 3.2.3)

**简体中文在下面 / Chinese version below**

## EN

This Spring Boot 3.2.3 service now focuses on three things:

1. **Runs an H2 database instance** and exposes it via **TCP** so that other Spring Boot services can connect over JDBC.
2. Provides a minimal **Swagger UI** API to run **SELECT-only** queries against the H2 instance and stream results as lines with comma-separated columns.
3. Publishes the existing H2 snapshot to **PostgreSQL** with full refresh, retries, failure logging and validation reporting.

The old Oracle → H2 loader is retained for historical compatibility, but `loader.enabled=false` by default and it is no longer part of the active workflow.

### Why full refresh (drop & reload) daily?
- It’s **simpler and safer** than incremental, with a much lower risk of logical errors.
- No middleware is used; only direct JDBC connections.

### Project layout
```
h2-oracle-sync/
  ├─ src/main/java/com/example/h2sync/...
  ├─ src/main/resources/application.yml
  ├─ scripts/start.sh
  ├─ scripts/backup.sh
  ├─ backups/
  └─ pom.xml
```

### Build & Run
Requirements: **JDK 17**, **Maven 3.9+**.

```bash
# 1) Configure PostgreSQL
#    - postgresql.url, postgresql.username, postgresql.password, postgresql.schema
#    - set postgresql.loader.enabled=true

# 2) Build
mvn -q -DskipTests package

# 3) Start the service
java -jar target/h2-oracle-sync-1.0.1.jar
```

H2 JDBC URL (for external services):
```
jdbc:h2:tcp://<host>:9092/./data/h2db;MODE=Oracle;DATABASE_TO_UPPER=false;AUTO_SERVER=TRUE
username: sa
password: (empty)
```

Swagger UI:
```
http://localhost:8080/swagger-ui/index.html
```

### SELECT API
`POST /api/query` with body:
```json
{ "sql": "select * from MY_TABLE where ROWNUM <= 5" }
```
Response is `text/plain`, one row per line, columns joined by comma (quoted as needed). Only a **single SELECT** is allowed—no `;`, no DDL/DML.

### Legacy Loader (Oracle → H2, disabled)
- This was a one-time migration and is disabled by `loader.enabled=false`.
- **Startup refresh**: automatically runs a full refresh once the application is ready, before the cron schedule kicks in.
- **Tables**: dropped and recreated from Oracle column metadata, then bulk-inserted (batched, streaming).
- **Views**: recreated as H2 views by translating the Oracle view SQL.
- **Sequences**: recreated in H2 using Oracle `INCREMENT BY` and **current/next** value (`last_number`).
- **Blacklist**: set in `loader.blacklist` (case-insensitive, supports `SCHEMA.NAME` form).
- **Multithreaded**: parallel copy per table/view (`loader.threads`).
- **Retries**: each object retried up to `loader.maxRetries` with exponential backoff.
- **Failure log**: H2 table `ETL_FAIL_LOG` records failures for manual compensation.

### Manual Full Refresh
- `POST /api/loader/full-refresh?reason=<optional>` runs the full loader on demand without restarting Spring Boot.
- The request blocks until the refresh finishes and returns `200 OK` on success, `409 CONFLICT` if another refresh is running, `503` when the loader is disabled.

### Full Refresh (H2 → PostgreSQL)

- Disabled by default. Configure the `postgresql.*` connection and set `postgresql.loader.enabled=true`.
- **Tables and data**: recreates columns and mapped PostgreSQL types, nullability, defaults, identity/generated columns, primary keys and unique constraints; rows are streamed in configurable batches.
- **H2 protection**: H2 full-table reads use `source-read-threads=1` by default; PostgreSQL-only constraint/index work uses a separate `target-threads` pool.
- **LOBs**: BLOB/CLOB values are streamed one row at a time instead of retaining a whole JDBC batch in heap.
- **Bulk-load order**: table rows are loaded before primary keys, unique constraints and indexes are created. The JDBC URL enables `reWriteBatchedInserts` for non-LOB rows.
- **Sequences**: recreates increment/range/cycle/cache settings and aligns PostgreSQL's next value with H2 `BASE_VALUE`.
- **Indexes**: recreates ordinary and unique indexes after data loading. Indexes already represented by primary-key/unique constraints are not duplicated.
- **Foreign keys**: recreates same-schema foreign keys after every table is loaded.
- **Views**: remaps the H2 source schema to the PostgreSQL target schema, translates common compatibility tokens (`NVL`, `IFNULL`, `SYSDATE`, `MINUS`), and retries views in dependency order.
- **Reliability**: supports per-object retries, case-insensitive blacklist entries and a PostgreSQL failure table named `H2_PG_ETL_FAIL_LOG`. Any terminal object failure makes the whole run fail after the report is printed.
- **Validation output**: logs an H2/PostgreSQL comparison report for table row counts, constraints, view/index/foreign-key presence and sequence next values.
- **Schedule**: defaults to 03:30 Asia/Shanghai.

Manual trigger:

```text
POST /api/postgresql-loader/full-refresh?reason=<optional>
```

The call blocks and returns `200 OK` after the refresh, `409 CONFLICT` if a PostgreSQL refresh is already running, `500` if any object ultimately fails, or `503` when `postgresql.loader.enabled=false`.

### Sample Loader (100-row snapshot)
- Disabled by default with `sample.loader.enabled=false` because it also depends on Oracle.
- `GET /api/sample-loader/refresh` builds a separate H2 database using `sample.loader.h2-url`.
- Each Oracle table contributes up to **100 rows** (or fewer if the table is smaller), while views and sequences are recreated one-to-one.
- Useful when you only need lightweight fixtures without cloning the full dataset. Triggered manually; no scheduler runs it automatically.

Schedule defaults to **02:30 Asia/Tokyo** daily. Adjust with `loader.cron` (Spring cron).

### Backup
Logical backup compressed to ZIP with `SCRIPT TO`:
```bash
java -jar target/h2-oracle-sync-1.0.1.jar --backup --backup.dir=backups --backup.file=h2-backup.zip
```
Restore:
```sql
RUNSCRIPT FROM 'backups/h2-backup.zip';
```

### Notes / Limitations
- Data type mapping covers the H2 types produced by the original import and common native H2 types. Tune `mapType()` for additional vendor-specific types.
- Oracle → H2 currently copies primary keys, unique constraints and nullability, but does not copy ordinary Oracle indexes or foreign keys. H2 → PostgreSQL copies the complete structure that exists in H2, including its ordinary indexes and same-schema foreign keys.
- PostgreSQL 12+ is recommended. Highly vendor-specific H2 view expressions may still need an additional rule in `H2ToPostgresqlViewSqlTranslator`.

---

## 中文 (CN)

这个 Spring Boot 3.2.3 服务现在聚焦于：

1. **启动一个 H2 数据库实例**，通过 **TCP** 对外提供 JDBC 连接（供其它 Spring Boot 服务连接）。
2. 提供一个极简 **Swagger UI** API（只允许 **SELECT**），把查询结果按“每行一条、列用逗号分隔”的文本流返回。
3. 把现有 H2 快照**全量发布到 PostgreSQL**，并提供重试、失败日志和迁移校验报告。

Oracle → H2 属于已经完成的一次性历史功能，代码继续保留，但默认配置为 `loader.enabled=false`，不再参与当前流程。

### 为何选择每日 **全量**（drop + reload）？
- **更简单、更稳妥**，出错概率低。
- **不使用任何中间件**，仅 JDBC。

### 构建与运行
环境：**JDK 17**、**Maven 3.9+**。

```bash
# 1）配置 H2 → PostgreSQL：
#    - postgresql.url / postgresql.username / postgresql.password / postgresql.schema
#    - postgresql.loader.enabled=true

# 2）打包
mvn -q -DskipTests package

# 3）启动
java -jar target/h2-oracle-sync-1.0.1.jar
```

对外 H2 JDBC：
```
jdbc:h2:tcp://<host>:9092/./data/h2db;MODE=Oracle;DATABASE_TO_UPPER=false;AUTO_SERVER=TRUE
用户名: sa
密码: （空）
```

Swagger：`http://localhost:8080/swagger-ui/index.html`

### SELECT 接口
`POST /api/query`，请求体：
```json
{ "sql": "select * from MY_TABLE where ROWNUM <= 5" }
```
返回 `text/plain`，**一行一条**，**列用逗号分隔**（必要时会加引号）。**只允许单条 SELECT**，禁止 `;` 与任何 DDL/DML。

### 历史装载（Oracle → H2，已停用）
- 这是已经完成的一次性迁移，默认 `loader.enabled=false`。
- **启动即刷新**：Spring Boot 完全就绪后会立即执行一次全量刷新，然后再按 cron 定时。
- **表**：根据 Oracle 列元数据在 H2 里**重建表结构**，然后批量写入（流式 + 批处理）。
- **视图**：把 Oracle 的视图 SQL 翻译后直接在 H2 中创建同名视图。
- **序列**：用 Oracle 的 `INCREMENT BY` 和 **当前/下一个值**（`last_number`）在 H2 里重建。
- **黑名单**：`loader.blacklist` 指定（大小写不敏感，支持 `SCHEMA.NAME`）。
- **多线程**：按表/视图并行（`loader.threads`）。
- **重试**：每个对象最多重试 `loader.maxRetries` 次，指数退避。
- **失败记录**：H2 表 `ETL_FAIL_LOG` 记录失败，便于人工补偿。

### 手动触发全量
- `POST /api/loader/full-refresh?reason=<可选说明>` 可以在不重启 Spring Boot 的情况下随时触发全量。
- 接口会在全量完成后返回：成功 `200 OK`，若已有任务在跑返回 `409 CONFLICT`，若 loader 被禁用则返回 `503`。

### 全量发布（H2 → PostgreSQL）

- 默认关闭；先配置 `postgresql.*`，再设置 `postgresql.loader.enabled=true`。
- **表和数据**：重建列及 PostgreSQL 类型、非空、默认值、自增/生成列、主键、唯一约束，然后按批次流式写入全部数据。
- **保护 H2**：H2 全表读取默认 `source-read-threads=1`；仅 PostgreSQL 侧的约束和索引使用独立的 `target-threads` 并行池。
- **LOB**：BLOB/CLOB 按行流式发送，不再把整个 JDBC 批次的 LOB 全部保留在堆内存中。
- **全量写入顺序**：先写数据，再创建主键、唯一约束和索引；非 LOB 数据启用 PostgreSQL `reWriteBatchedInserts`。
- **序列**：迁移步长、上下限、循环、缓存，并用 H2 `BASE_VALUE` 对齐 PostgreSQL 的下一个值。
- **索引**：数据完成后创建普通索引和唯一索引；主键/唯一约束已经隐含的索引不会重复创建。
- **外键**：所有表完成后创建同一 H2 schema 内的外键及更新/删除规则。
- **视图**：把 H2 source schema 映射到 PostgreSQL target schema，转换常见兼容语法（`NVL`、`IFNULL`、`SYSDATE`、`MINUS`），并按依赖关系重试。
- **可靠性**：对象级重试、大小写不敏感黑名单；失败写入 PostgreSQL 的 `H2_PG_ETL_FAIL_LOG`。任何对象最终失败，报告仍会输出，但整体任务返回失败。
- **最终校验**：日志输出 H2/PostgreSQL 对比报告，逐项检查表行数、主键/唯一约束、视图、索引、外键和序列下一个值。
- **定时任务**：默认是上海时区每天 03:30。

手动触发：

```text
POST /api/postgresql-loader/full-refresh?reason=<可选说明>
```

接口阻塞到执行完成：成功返回 `200`，已有任务运行返回 `409`，任何对象最终失败返回 `500`，`postgresql.loader.enabled=false` 返回 `503`。

### 100 条样例装载
- 该功能同样依赖 Oracle，默认 `sample.loader.enabled=false`，不会创建对应 Bean 和接口。
- `GET /api/sample-loader/refresh` 会按 `sample.loader.h2-url` 构建一个**独立**的 H2 数据库。
- 每张 Oracle 表最多取 **100 行**（如果不足 100 行则全部取），视图与序列也会对应创建。
- 适用于只需要轻量数据样本的场景，完全手动触发，不会随应用启动或定时任务自动执行。

默认每天 **东京时间 02:30** 执行，修改 `loader.cron` 可调整。

### 备份
使用 `SCRIPT TO` 生成压缩逻辑备份：
```bash
java -jar target/h2-oracle-sync-1.0.1.jar --backup --backup.dir=backups --backup.file=h2-backup.zip
```
恢复：
```sql
RUNSCRIPT FROM 'backups/h2-backup.zip';
```

### 注意事项
- 类型映射覆盖原迁移生成的 H2 类型及常见原生 H2 类型，额外的厂商特有类型可在 `mapType()` 中扩展。
- Oracle → H2 当前会复制主键、唯一约束和非空属性，但不复制 Oracle 普通索引和外键；H2 → PostgreSQL 会复制 H2 中实际存在的完整结构，包括普通索引和同 schema 外键。
- 建议使用 PostgreSQL 12+；极少数 H2/Oracle 特有视图表达式可能仍需在 `H2ToPostgresqlViewSqlTranslator` 增加转换规则。

---

## Scripts

- `scripts/start.sh` – build & start app
- `scripts/backup.sh` – create a compressed logical backup

Enjoy!

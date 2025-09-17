# H2 Oracle Sync (Spring Boot 3.2.3)

**简体中文在下面 / Chinese version below**

## EN

This Spring Boot 3.2.3 service does three things:

1. **Runs an H2 database instance** and exposes it via **TCP** so that other Spring Boot services can connect over JDBC.
2. Provides a minimal **Swagger UI** API to run **SELECT-only** queries against the H2 instance and stream results as lines with comma-separated columns.
3. Has a **daily scheduled job** that connects to **Oracle** and performs a **full refresh** into H2 (tables, views materialized as tables, and sequences), with retries, multithreading, and failure logging.

### Why full refresh (drop & reload) daily?
- It’s **simpler and safer** than incremental, with a much lower risk of logical errors.
- You said the H2 data may lag Oracle by one day—this matches a daily batch.
- No middleware is used; only direct JDBC connections.

If you later insist on incremental loads, you can extend `OracleLoaderService` to track high watermarks or use change tables—but that's not required now.

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
# 1) Configure Oracle in src/main/resources/application.yml
#    - oracle.url, oracle.username, oracle.password, oracle.schema
#    - loader.blacklist for objects to skip

# 2) Build
mvn -q -DskipTests package

# 3) Start the service (H2 TCP + REST + Scheduler)
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

### Daily Loader (Oracle → H2)
- **Tables**: dropped and recreated from Oracle column metadata, then bulk-inserted (batched, streaming).
- **Views**: **materialized** into tables named `VW_<VIEWNAME>`.
- **Sequences**: recreated in H2 using Oracle `INCREMENT BY` and **current/next** value (`last_number`).
- **Blacklist**: set in `loader.blacklist` (case-insensitive, supports `SCHEMA.NAME` form).
- **Multithreaded**: parallel copy per table/view (`loader.threads`).
- **Retries**: each object retried up to `loader.maxRetries` with exponential backoff.
- **Failure log**: H2 table `ETL_FAIL_LOG` records failures for manual compensation.

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
- Data type mapping is simplified but covers common Oracle types. Tune `mapType()` if needed.
- Primary keys, indexes, constraints are not copied in this minimal baseline.
- Views are materialized as tables to avoid dialect incompatibilities.
- `user_sequences.last_number` is the **next** value in Oracle; we align H2 to that to avoid duplicates.

---

## 中文 (CN)

这个 Spring Boot 3.2.3 服务包含：

1. **启动一个 H2 数据库实例**，通过 **TCP** 对外提供 JDBC 连接（供其它 Spring Boot 服务连接）。
2. 提供一个极简 **Swagger UI** API（只允许 **SELECT**），把查询结果按“每行一条、列用逗号分隔”的文本流返回。
3. 带有一个**每日定时任务**，连接 **Oracle**，把数据 **全量刷新** 到 H2（包含表、**视图按表物化**、序列），并且有重试、多线程和失败记录。

### 为何选择每日 **全量**（drop + reload）？
- **更简单、更稳妥**，出错概率低。
- 你的要求是 H2 相比 Oracle **延迟一天**，每日批量刚好匹配。
- **不使用任何中间件**，仅 JDBC。

如果以后一定要增量，再给 `OracleLoaderService` 增加高水位或变更表逻辑即可；目前不需要。

### 构建与运行
环境：**JDK 17**、**Maven 3.9+**。

```bash
# 1）修改配置：src/main/resources/application.yml
#    - oracle.url / oracle.username / oracle.password / oracle.schema
#    - loader.blacklist 黑名单列表

# 2）打包
mvn -q -DskipTests package

# 3）启动（H2 TCP + REST + 定时器）
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

### 每日装载（Oracle → H2）
- **表**：根据 Oracle 列元数据在 H2 里**重建表结构**，然后批量写入（流式 + 批处理）。
- **视图**：在 H2 中**物化为表**，命名为 `VW_<VIEWNAME>`。
- **序列**：用 Oracle 的 `INCREMENT BY` 和 **当前/下一个值**（`last_number`）在 H2 里重建。
- **黑名单**：`loader.blacklist` 指定（大小写不敏感，支持 `SCHEMA.NAME`）。
- **多线程**：按表/视图并行（`loader.threads`）。
- **重试**：每个对象最多重试 `loader.maxRetries` 次，指数退避。
- **失败记录**：H2 表 `ETL_FAIL_LOG` 记录失败，便于人工补偿。

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
- 类型映射做了适度简化，覆盖常见 Oracle 类型，必要时可在 `mapType()` 微调。
- 本版本不复制主键、索引、约束（如需请后续扩展）。
- 视图在 H2 中以**表**的形式物化，避免方言差异导致的失败。
- Oracle 的 `user_sequences.last_number` 表示**下一个**值；H2 也对齐到这个值，避免重复。

---

## Scripts

- `scripts/start.sh` – build & start app
- `scripts/backup.sh` – create a compressed logical backup

Enjoy!

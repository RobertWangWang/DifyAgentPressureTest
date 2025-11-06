# Dify Agent Pressure Test 平台

## 项目简介
Dify Agent Pressure Test 是一个围绕 Dify Agent 平台构建的压测与评估服务。项目基于 FastAPI 提供 RESTful 接口，结合 Celery 调度异步压测任务，通过 MySQL 持久化测试记录和数据集信息，并与 Redis、对象存储（S3/TOS）等外部资源协同工作。系统支持从数据集上传、压测任务创建、模型选择、结果下载到自动评分的全流程能力，帮助团队快速验证 Agent 配置在不同负载下的表现。 【F:app/main.py†L1-L27】【F:app/core/celery_app.py†L1-L53】【F:app/core/config.py†L1-L55】

## 功能概览
- **压测任务管理**：创建、查询、更新、删除压测任务，支持关联数据集和 Dify Agent 信息，并在 Celery 中异步执行。 【F:app/api/test_record_api.py†L89-L200】【F:app/services/test_tasks.py†L1-L99】
- **数据集管理**：上传 CSV/Excel 数据集，去重后存储到 TOS 并生成预览，提供查询、下载与逻辑删除接口。 【F:app/api/dataset_api.py†L1-L203】【F:app/models/dataset.py†L1-L72】
- **模型配置**：维护评测所需的大模型配置，支持查询、树形分组及连通性测试，压测任务可引用判分模型。 【F:app/api/provider_models_api.py†L1-L111】
- **结果获取**：提供压测结果与单次运行详情的下载接口，便于归档与分析。 【F:app/api/download_api.py†L1-L53】【F:app/api/single_run_result_api.py†L1-L132】
- **Prompt 与参数模板**：集中管理评测用的 Prompt 模板及 Agent 参数，便于动态生成任务配置。 【F:app/api/prompt_template_api.py†L1-L49】【F:app/services/test_record_services.py†L1-L200】

## 技术架构
| 组件 | 用途 |
| --- | --- |
| FastAPI | 暴露管理与查询接口。 【F:app/main.py†L1-L18】|
| Celery + Redis | 调度异步压测任务、分布式执行评分逻辑。 【F:app/core/celery_app.py†L1-L53】|
| MySQL/SQLAlchemy | 持久化测试记录、数据集、模型配置等结构化数据。 【F:app/core/database.py†L1-L49】【F:app/models/dataset.py†L1-L72】|
| 对象存储 (S3/TOS) | 存放上传的数据集文件并支持下载。 【F:app/api/dataset_api.py†L1-L203】|
| Dify 平台 API | 创建 Agent API Key、触发对话/工作流压测、查询模型列表。 【F:app/utils/pressure_test_util.py†L61-L200】【F:app/api/test_record_api.py†L129-L200】|

## 目录结构
```
├── app
│   ├── api                # FastAPI 路由（压测、数据集、模型、下载等接口）
│   ├── core               # 配置、数据库连接、Celery 初始化
│   ├── crud               # 与数据库交互的封装
│   ├── models             # SQLAlchemy ORM 模型
│   ├── schemas            # Pydantic 数据校验
│   ├── services           # 压测流程、Celery 任务、第三方服务封装
│   └── utils              # 日志、评分、上传等工具函数
├── sql                    # 初始化数据库的 DDL 脚本
├── docker-compose.yml     # 本地一键启动 web 与 celery 服务
├── requirements.txt       # Python 依赖列表
└── run.sh / Dockerfile    # 运行与部署脚本
```

## 核心模块说明
### 1. 压测任务生命周期
1. **创建任务**：`/test_records/create_record` 接口校验请求体、确认数据集存在，并基于 Dify API 自动补全 Agent 信息与 API Key。 【F:app/api/test_record_api.py†L129-L200】
2. **触发执行**：接口根据任务类型调度 `tasks.run_chatflow_test`、`tasks.run_chatflow_stream_test` 或 `tasks.run_workflow_test` Celery 任务。 【F:app/api/test_record_api.py†L273-L360】【F:app/services/test_tasks.py†L34-L99】
3. **并发压测**：`test_record_services` 中的封装负责对 Dify Agent 发送请求、统计耗时、计算 TPS，并调用判分模型生成得分。 【F:app/services/test_record_services.py†L1-L200】【F:app/utils/pressure_test_util.py†L61-L200】
4. **结果回写**：任务执行结束后更新 `bots_eval_test_records`，并可通过下载接口导出压测明细。 【F:app/crud/test_record_crud.py†L1-L120】【F:app/api/download_api.py†L1-L53】

### 2. 数据集管理
- 文件上传时按 MD5 去重，支持 CSV、Excel，并抽取前 3 行生成预览数据。 【F:app/api/dataset_api.py†L35-L121】
- 数据集元信息存储于 `bots_eval_datasets` 表，与压测任务通过 `dataset_uuid` 关联。 【F:app/models/dataset.py†L1-L72】【F:app/crud/test_record_crud.py†L1-L120】
- 提供分页查询、逻辑删除、S3 下载等接口，便于管理和溯源。 【F:app/api/dataset_api.py†L123-L200】

### 3. 模型与判分
- `/provider_models/query` 根据服务商与模型名筛选候选模型，并通过 `llm_connection_test` 校验连通性。 【F:app/api/provider_models_api.py†L53-L111】【F:app/services/provider_model_services.py†L1-L160】
- Celery 压测任务会读取会话中缓存的模型信息，驱动大模型对压测结果进行自动评分。 【F:app/services/test_tasks.py†L34-L99】【F:app/utils/pressure_test_util.py†L135-L200】

## 环境准备
1. **基础依赖**：Python 3.10+、MySQL 8.0+、Redis 6+、S3 兼容对象存储（可使用 MinIO）、Docker（可选，用于容器化部署）。
2. **安装 Python 包**：
   ```bash
   pip install -r requirements.txt
   ```
3. **准备数据库**：执行 `sql/v1.0.0_agent_eval.sql` 初始化所需表结构，并在 `.env.prod` 中填入数据库连接信息。 【F:sql/v1.0.0_agent_eval.sql†L1-L200】【F:app/core/config.py†L1-L55】
4. **配置环境变量**：复制 `app/config/.env.prod`（需自行创建）并设置以下关键变量：
   - `DB_USERNAME` / `DB_PASSWORD` / `DB_HOST` / `DB_PORT` / `DB_DATABASE`
   - `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASSWORD`
   - `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_BUCKET`
   - `TARGET_DIFY_API_URL`、`CONCURRENCY` 等 Dify 调用所需参数。

## 启动方式
### 方案一：本地直接运行
```bash
# 启动 FastAPI
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# 启动 Celery worker（另开终端）
celery -A app.core.celery_app.celery_app worker -l info --concurrency=4 --prefetch-multiplier=1 -Q default
```
FastAPI 默认监听 `http://127.0.0.1:8000`，可通过 `/docs` 查看交互式接口文档。

### 方案二：Docker Compose
```bash
docker compose up --build
```
Compose 会同时启动 web（FastAPI）与 celery 两个服务，并挂载本地代码目录方便开发调试。 【F:docker-compose.yml†L1-L38】

## 常用接口清单
| 功能 | 方法 & 路径 | 说明 |
| --- | --- | --- |
| 上传数据集 | `POST /datasets/upload` | 上传 CSV/Excel 并返回数据集 UUID。 【F:app/api/dataset_api.py†L35-L121】|
| 查询数据集 | `GET /datasets/list` | 分页列出当前用户或 Agent 的数据集。 【F:app/api/dataset_api.py†L123-L165】|
| 创建压测任务 | `POST /test_records/create_record` | 生成压测任务并准备 Dify API Key。 【F:app/api/test_record_api.py†L129-L200】|
| 启动压测 | `POST /test_records/run` 等（视业务约定） | 触发 Celery 执行压测。 【F:app/api/test_record_api.py†L273-L360】|
| 下载压测报告 | `GET /downloads/{uuid}` | 导出压测结果或数据集原始文件。 【F:app/api/download_api.py†L1-L53】|
| 查询模型树 | `GET /provider_models/tree` | 按服务商分组展示有效模型。 【F:app/api/provider_models_api.py†L97-L111】|

> 注：部分接口依赖登录态或上游系统回调，具体参数可参考 `app/schemas` 下的 Pydantic 定义。

## 压测评分机制
- 评分阶段会根据任务类型选择流式或阻塞式调用 Dify Agent，并在 `pressure_test_util` 中通过第三方模型进行比对评分。 【F:app/utils/pressure_test_util.py†L61-L200】
- 若数据集中包含标准答案（`ref_answer`），系统会调用指定判分模型获取得分；否则默认给予满分。 【F:app/utils/pressure_test_util.py†L171-L200】
- 评估结果（耗时、TPS、得分、原始回答等）会写入数据库并用于后续报表导出。 【F:app/services/test_record_services.py†L120-L210】

## 日志与监控
- 全局日志基于 `loguru` 输出，关键操作会打印在控制台并可按需重定向到文件。 【F:app/utils/logger.py†L1-L62】
- Celery 自带心跳任务 `celery.health_check`，可用于探测 worker 是否存活。 【F:app/core/celery_app.py†L39-L53】

## 开发建议
1. 修改数据库模型后记得同步更新 SQL 脚本与 Pydantic Schema，保持三者一致。 【F:app/models/dataset.py†L1-L72】【F:app/schemas/dataset_schema.py†L1-L51】
2. 对接新的判分模型时，在 `app/utils/provider_models_util.py` 中补充调用封装，并在 `provider_models` 表新增记录。 【F:app/utils/provider_models_util.py†L1-L200】【F:app/api/provider_models_api.py†L53-L111】
3. 上传大文件时注意调整 `ThreadPoolExecutor` 的 `max_workers` 或 `CONCURRENCY`，以免耗尽系统资源。 【F:app/api/dataset_api.py†L1-L49】【F:app/utils/pressure_test_util.py†L1-L33】

---
如需进一步了解接口详情，可参考 `app/api` 下的源码或直接访问 FastAPI 自动生成的文档。

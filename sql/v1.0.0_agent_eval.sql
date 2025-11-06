SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;
USE `intelligent`;

create table bots_eval_datasets
(
    uuid         varchar(36)  not null comment '数据集唯一 UUID'
        primary key,
    agent_id     varchar(64)  not null comment '所属的 dify agent ID',
    filename     varchar(255) not null comment '原始文件名',
    file_md5     varchar(64)  not null comment '文件 MD5 值，用于去重',
    file_suffix  varchar(16)  null comment '文件后缀名，如 .csv/.xlsx',
    tos_key      varchar(255) not null comment '上传至 TOS 的对象 Key',
    tos_url      varchar(512) not null comment '上传至 TOS 的完整下载 URL',
    preview_rows json         null comment '文件前 3 行内容 JSON',
    uploaded_by  varchar(64)  null comment '上传者用户名',
    created_at   datetime     not null comment '上传时间',
    is_deleted   tinyint(1)   not null comment '是否逻辑删除',
    constraint uuid
        unique (uuid)
)
    comment '上传数据集文件信息表' collate = utf8mb4_unicode_ci;

create table bots_eval_prompt_template
(
    record_id   int auto_increment
        primary key,
    uuid        varchar(36)              not null,
    created_at  datetime default (now()) not null,
    deleted_at  tinyint(1)               not null,
    content     text                     not null,
    prompt_name varchar(255)             not null,
    constraint uuid
        unique (uuid)
)
    comment '模型厂商提供，支持中文模糊搜索' collate = utf8mb4_unicode_ci;

INSERT INTO bots_eval_prompt_template (record_id, uuid, created_at, deleted_at, content, prompt_name) VALUES (1, '2187d1c9-a990-4771-80e4-0ce2abacb683', '2025-10-24 11:57:39', 0, '你是一名语义相似度评估员。

现在给出两段文本 A 和 B，请根据它们在语义上的接近程度打一个 0–100 的分数。
评分标准如下：
- 0 分：语义完全不同，没有任何关联；
- 100 分：语义完全相同；
- 50 分：部分相似，有部分语义重合但主要意思不同。

请只输出一个整数分数（不需要解释）。

文本A：{gen_text}
文本B：{ref_text}

相似度评分（0–100）：

以json的格式返回你的结果，json的格式如下：
{"score":《你给出的分数》}

注意，返回格式只能是json，不要输出你的思考内容', '问答正确性评估');
INSERT INTO bots_eval_prompt_template (record_id, uuid, created_at, deleted_at, content, prompt_name) VALUES (2, 'bab0267f-2f88-40c3-b3e9-b813b1386864', '2025-10-24 11:57:46', 0, '你是一名专业的语义相似度评估员，负责评估大语言模型生成文本的语义质量。

现在给出两段文本 A（模型生成结果）和 B（参考答案）。
请根据它们在**语义上的接近程度**，打一个 0–100 的分数。

评分标准如下：
- 0 分：语义完全不同，内容无关；
- 100 分：语义完全相同；
- 50 分：部分相似，有一定语义重合，但主要意思不同。

请严格按照语义一致性评分，不考虑语法、长度或表达方式的差异。

⚠️ 输出要求：
- 只输出一个整数分数；
- 结果必须以 JSON 格式返回；
- 不要输出除 JSON 外的任何内容（包括解释或思考过程）。

文本A（模型输出）：{gen_text}
文本B（参考答案）：{ref_text}

请返回：
{"score": 《你给出的整数分数》}
', '回复质量评估');
INSERT INTO bots_eval_prompt_template (record_id, uuid, created_at, deleted_at, content, prompt_name) VALUES (3, '24c0e9d6-f567-4cac-aefb-d9a7ceaa77b9', '2025-10-24 11:57:52', 0, '你是一名“智能体人设遵循性评估员”。

现在给出两段文本 A 和 B，请判断文本 A（模型输出）与文本 B（人设描述或参考示例）在“人设一致性”上的符合程度，并给出一个 0–100 的分数。

评分标准如下：
- 0 分：完全不符合人设，语气、态度、行为方式等均不一致；
- 100 分：完全符合人设，与参考人设在语气、身份、风格、价值观上高度一致；
- 50 分：部分符合人设，部分特征一致但整体风格或态度偏离明显。

请仅依据人设一致性评分，而非内容相似度或语义相关性。

请只输出一个整数分数（不需要解释）。

文本A（模型输出）：{gen_text}
文本B（人设参考）：{ref_text}

人设遵循评分（0–100）：

以 JSON 格式返回你的结果，格式如下：
{"score": 《你给出的分数》}

注意：
- 返回格式只能是 JSON；
- 不要输出任何解释、推理过程或其他内容。
', '智能体人设遵循评估');
INSERT INTO bots_eval_prompt_template (record_id, uuid, created_at, deleted_at, content, prompt_name) VALUES (4, '8fb79227-be11-4086-8415-ef4e8027c60d', '2025-10-24 11:58:03', 0, '你是一名专业的【任务类型】评估员，负责执行“【任务名称】”的质量评估。

请根据给出的两段文本 A 和 B，评估 A（模型生成结果）相对于 B（参考标准）在【评估维度】上的符合程度，并给出一个 0–100 的分数。

评分标准如下（可根据任务调整）：
- 0 分：完全不符合，内容或行为严重偏离；
- 100 分：完全符合，内容或表现与参考一致；
- 50 分：部分符合，有一定相似或相关性，但存在明显偏差。

评估时，请主要考虑以下方面（根据任务调整）：
- 【维度1】（如：语义一致性 / 礼貌性 / 逻辑性）
- 【维度2】（如：完整性 / 准确性 / 风格一致性）
- 【维度3】（如：语气 / 情感 / 专业程度）

请只输出一个整数分数（不需要解释）。

文本A（模型输出）：{gen_text}
文本B（参考答案或标准文本）：{ref_text}

评分结果（0–100）：

请严格以 JSON 格式返回结果，格式如下：
{"score": 《你给出的分数》}

注意：
- 返回格式必须是 JSON；
- 不要输出任何思考、解释或附加文本。
', '通用任务提示词');
INSERT INTO bots_eval_prompt_template (record_id, uuid, created_at, deleted_at, content, prompt_name) VALUES (5, '47ddbafb-4655-41f0-b0c5-a6007704896e', '2025-10-24 11:57:58', 0, '你是一名“客服回复质量评估员”。

现在给出两段文本 A 和 B，请评估文本 A（客服回复）相对于文本 B（标准或期望回复）的**质量匹配程度**，并根据整体质量打一个 0–100 的分数。

评分标准如下：
- 0 分：客服回复质量极差，与参考答案完全不符，语气或内容严重偏离；
- 100 分：客服回复质量极高，内容完整、语气得当、与参考答案完全一致；
- 50 分：客服回复部分达标，语义相似但存在明显遗漏、误导或语气问题。

请主要从以下方面进行综合判断：
- 回复是否准确解决了用户问题；
- 回复是否保持礼貌、专业的客服语气；
- 回复内容是否完整、与参考答案一致或接近。

请只输出一个整数分数（不需要解释）。

文本A（客服回复）：{gen_text}
文本B（参考答案）：{ref_text}

客服回复质量评分（0–100）：

请以 JSON 格式返回你的结果，格式如下：
{"score": 《你给出的分数》}

注意：
- 返回格式必须是 JSON；
- 不要输出任何解释、理由或思考过程。
', '客服回复质量评估');

create table bots_eval_single_run_result
(
    record_id              varchar(36)              not null comment '主键ID（UUID）'
        primary key,
    chatflow_query         varchar(1024)            null comment 'chatflow专用query',
    test_params            json                     null comment '评测参数（JSON）',
    input_task_uuid        varchar(64)              not null comment '输入任务UUID',
    input_time_consumption float                    null comment '耗时（秒）',
    input_score            float                    null comment '得分',
    input_tps              float                    null comment '吞吐量（TPS）',
    input_generated_answer text                     null comment '生成的答案',
    is_deleted             tinyint(1)               not null comment '软删除标志',
    create_time            datetime default (now()) not null comment '创建时间（UTC）'
)
    comment '单条运行的测试结果，支持中文模糊搜索' collate = utf8mb4_unicode_ci;

create index idx_create_time
    on bots_eval_single_run_result (create_time);

create index idx_input_task_uuid
    on bots_eval_single_run_result (input_task_uuid);

create table bots_eval_test_records
(
    uuid                      varchar(36)                                                              not null comment '测试记录唯一 UUID'
        primary key,
    created_at                datetime default (now())                                                 not null comment '创建时间',
    is_deleted                tinyint(1)                                                               not null comment '是否逻辑删除',
    dataset_uuid              varchar(36)                                                              null comment '关联数据集 UUID（外键）',
    filename                  varchar(255)                                                             not null comment '评测文件名',
    status                    enum ('INIT', 'RUNNING', 'CANCELLED', 'FAILED', 'SUCCESS', 'EXPERIMENT', 'PENDING') not null comment '评测任务状态',
    agent_type                enum ('CHATFLOW', 'WORKFLOW')                                            not null comment '智能体类别',
    task_name                 varchar(256)                                                             not null comment '评测任务名称',
    agent_name                varchar(256)                                                             null comment '智能体名称',
    judge_prompt              varchar(2048)                                                            not null comment '评测提示词',
    judge_model               varchar(256)                                                             null comment '评测模型名称',
    judge_model_provider_name varchar(256)                                                             null comment '模型供应商',
    duration                  int                                                                      null comment '任务耗时',
    result                    json                                                                     null comment '评测结果',
    concurrency               int                                                                      not null comment '并发数',
    dify_api_url              varchar(512)                                                             not null comment 'Dify API URL',
    dify_bearer_token         varchar(512)                                                             not null comment 'Dify Bearer Token',
    dify_test_agent_id        varchar(256)                                                             not null comment 'Dify 测试 Agent ID',
    dify_api_key              varchar(256)                                                             null comment 'Dify API Key',
    dify_account_id           varchar(64)                                                              null comment 'Dify Account ID',
    dify_username             varchar(256)                                                             not null comment 'Dify 用户名',
    success_count             int                                                                      not null comment '成功次数',
    failure_count             int                                                                      not null comment '失败次数',
    dataset_tos_key           varchar(512)                                                             null comment 'TOS Key（兼容历史）',
    dataset_tos_url           varchar(1024)                                                            null comment 'TOS URL（兼容历史）',
    dataset_file_md5          varchar(64)                                                              null comment '文件 MD5（兼容历史）',
    constraint uuid
        unique (uuid),
    constraint test_records_ibfk_1
        foreign key (dataset_uuid) references bots_eval_datasets (uuid)
            on delete set null
)
    comment '评测任务记录表（引用 Dataset 表）' collate = utf8mb4_unicode_ci;


create index dataset_uuid
    on bots_eval_test_records (dataset_uuid);


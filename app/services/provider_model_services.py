from typing import List

from app.models.provider_model import ProviderModel
from app.utils.logger import logger
from app.utils.provider_models import (
    call_volcengine_ark,
    call_aliyun_dashscope,
    call_openai_compatible
)

def llm_connection_test(candidate_models: List[ProviderModel]):

    for candidate_model in candidate_models:

        candidate_model_name = candidate_model.model_name
        candidate_model_provider_name = candidate_model.provider_name
        candidate_model_config = candidate_model.config
        if "aliyun" in candidate_model_provider_name or "bailian" in candidate_model_provider_name:
            result = call_aliyun_dashscope(candidate_model_config)
        elif "openai" in candidate_model_provider_name:
            result = call_openai_compatible(candidate_model_config)
        elif ("volcengine" in candidate_model_provider_name or
              "doubao" in candidate_model_provider_name or
              "ark.cn-beijing" in (candidate_model_config.get("endpointId") or "")):
            result = call_volcengine_ark(candidate_model_config)
        else:
            logger.warning(f" 未识别 provider={candidate_model_provider_name}, 跳过模型 {candidate_model_name}")
            continue
        if result.get('status') == 200:
            logger.info(
                f"Provider={candidate_model_provider_name} | Model={candidate_model_name} | status={result.get('status')} | text={result.get('text')}")
            return candidate_model.id
    return ""
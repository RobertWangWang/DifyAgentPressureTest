from fastapi import FastAPI
from app.api.test_record_api import router as test_record_router
from app.api.provider_models_api import router as provider_models_router
from app.api.single_run_result_api import router as single_run_result_router
from app.api.download_api import router as download_router
from app.api.prompt_template_api import router as prompt_router
from app.api.dataset_api import router as dataset_router
from starlette.middleware.sessions import SessionMiddleware
from app.core.database import init_db
app = FastAPI(title="Test Record Management")
app.add_middleware(SessionMiddleware, secret_key="super-secret-key")
app.include_router(test_record_router)
app.include_router(provider_models_router)
app.include_router(single_run_result_router)
app.include_router(download_router)
app.include_router(prompt_router)
app.include_router(dataset_router)

init_db()
# 打印所有路由
def print_routes():
    for route in app.routes:
        # route.path, route.name, route.methods 等属性
        print(f"Route: path={route.path}, name={route.name}, methods={route.methods}")

# 在模块导入时就打印（启动时）
print_routes()

FROM ccr.ccs.tencentyun.com/itqm-base-images/python:3.10-slim-bookworm

# 1️⃣ 启用 BuildKit 缓存
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 2️⃣ 优先拷贝 requirements.txt （可缓存）
COPY ./requirements.txt /app/requirements.txt

# 3️⃣ 安装依赖，利用缓存 mount
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip -i https://mirrors.ustc.edu.cn/pypi/web/simple/ && \
    pip install -r /app/requirements.txt -i https://mirrors.ustc.edu.cn/pypi/web/simple/ && \
    pip install aiomysql==0.2.0 pymysql==1.1.1 -i https://mirrors.ustc.edu.cn/pypi/web/simple/ && \
    mkdir -p /app/logs

# 4️⃣ 再拷贝项目源码（变化最多的层放最后）
COPY ./app ./app

# 5️⃣ 设置 PYTHONPATH
ENV PYTHONPATH=.

EXPOSE 8000
ENTRYPOINT []


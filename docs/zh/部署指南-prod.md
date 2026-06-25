# Prod 部署说明（prod-cd + docker compose，正式发布版）

push `main` 分支后，[.github/workflows/prod-cd.yml](../../.github/workflows/prod-cd.yml) 自动：
构建镜像 → 推 GHCR（`latest` + `sha-<sha>`）→ SSH 到 **prod 服务器** 同步 compose → `pull && up -d --wait`。

prod-cd **只做构建与部署**，不生成/修改任何密钥或配置。`.env` 与 `prod-config.yaml` 由人一次性准备在服务器上，之后归你维护。

> 与 dev 的关系：dev 跟 `dev` 分支、镜像 `dev-latest`、服务器 `/opt/kawaru-dev`；prod 跟 `main` 分支、镜像 `latest`、**独立的另一台 prod 服务器** `/opt/kawaru-prod`。两套互不影响。详见文末对照表。

## 架构（Celery 分离模式）

```
mariadb(127.0.0.1:13306) ─┐
redis(127.0.0.1:16379)  ──┼─ app(8000)
                          ├─ worker (celery -Q default)
                          └─ beat   (celery 调度)
```

- `enable_embedded_celery: false`，app 不再 fork worker/beat；worker、beat 各自独立容器。
- 端口：app `8000`；**mariadb / redis 只绑回环 `127.0.0.1`**（正式环境不对公网开放，这是与 dev 的关键区别）。
- 如需从外部检测 DB/Redis，走 SSH 隧道，不要直接放开端口。

## 服务器一次性准备（项目目录：/opt/kawaru-prod）

```bash
# 1) 目录
sudo mkdir -p /opt/kawaru-prod/data/{mariadb,redis,app,logs/{app,worker,beat}}
cd /opt/kawaru-prod

# 2) app/worker/beat 以 appuser(默认 uid 1000) 运行，给可写目录授权
#    确认 uid：docker run --rm ghcr.io/<owner>/kawaru:latest id appuser
sudo chown -R 1000:1000 /opt/kawaru-prod/data/app /opt/kawaru-prod/data/logs

# 3) 放配置（从仓库复制模板，改成真实值）
#    .env             → /opt/kawaru-prod/.env
#    prod-config.yaml → /opt/kawaru-prod/prod-config.yaml（文件名/路径不能变）
#    .env 至少填：SECRET_KEY(openssl rand -hex 32)、DATABASE_PASSWORD、REDIS_PASSWORD、
#               CONFIG_PATH=/app/config/prod-config.yaml、FC_CALLBACK_TOKEN(非空)、真实 OSS/SMTP 凭证
#    （APP_IMAGE 不在 .env 里，手动跑 compose 命令时按下文 export）
#
# 不需要手动 docker login ghcr.io —— prod-cd 每次部署会用 GHCR_PULL_USER/TOKEN 自动登录
```

之后每次 push `main` 自动滚动升级，数据不丢。

## 落盘清单（bind mount → 本地）

以下数据都存储到宿主机器硬盘上，不在容器里面。

| 内容 | 容器内 | 宿主机 |
|---|---|---|
| MariaDB | `/var/lib/mysql` | `data/mariadb` |
| Redis(AOF) | `/data` | `data/redis` |
| 本地存储 `local/ temp_chunks/ origin/ processed/` | `/app/data` | `data/app` |
| app 日志 | `/app/kawaru_logs` | `data/logs/app` |
| worker 日志 | `/app/kawaru_logs` | `data/logs/worker` |
| beat 日志 | `/app/kawaru_logs` | `data/logs/beat` |

> 日志按服务拆分到不同目录：避免 app/worker/beat 三个进程同时写同名文件导致 `RotatingFileHandler` 轮转冲突。

## 常用运维

```bash
cd /opt/kawaru-prod
# 查看配置 / 密码
cat .env
# 看日志（按服务拆分）
# 注意：docker-compose.prod.yml 里 image 引用了 ${APP_IMAGE} 变量（不在 .env 里），
# 直接跑 compose 命令会报 "required variable APP_IMAGE is missing a value"，
# 需先在当前 shell 导出该变量：
export APP_IMAGE=ghcr.io/neonexusx/kawaru:latest
docker compose -f docker-compose.prod.yml -p kawaru-prod logs -f worker
# 改了 .env / prod-config.yaml 后手动生效
docker compose -f docker-compose.prod.yml -p kawaru-prod up -d
# 外部检测 DB / Redis（prod 只绑回环，需先开 SSH 隧道再连本地端口）
# ssh -L 13306:127.0.0.1:13306 -L 16379:127.0.0.1:16379 <PROD_SSH_USER>@<服务器IP>
mysql -h 127.0.0.1 -P 13306 -uroot -p
redis-cli -h 127.0.0.1 -p 16379 -a <REDIS_PASSWORD> ping
# 密码在 .env 里
# 确认 worker 已连上 broker（一次性，勿放进 healthcheck）
docker compose -f docker-compose.prod.yml -p kawaru-prod exec -T worker \
  celery -A celery_runtime.entrypoint:celery_app inspect ping --timeout 30
```

## healthcheck 说明

只给 `app`(`/docs`) + `mariadb` + `redis` 做 healthcheck；`worker`/`beat` **不做**：
- beat 是调度器，不响应 control 命令，`inspect ping` 永不通过；
- worker 的 `inspect ping` 每次会冷启动整套 app 而超时。

二者存活由 `restart: unless-stopped` 兜底。`docker compose up --wait` 因此只等 app 健康 + worker/beat 起来，不会卡死。

## GitHub Secrets

| 名称 | 用途 |
|---|---|
| `PROD_SSH_HOST` / `PROD_SSH_USER` / `PROD_SSH_KEY` | **独立 prod 服务器**（22 端口），与 dev 的 `DEV_SSH_*` 分开 |
| `GHCR_PULL_USER` | GitHub 用户名（小写），prod-cd SSH 到服务器后用它登录 GHCR 拉私有镜像（与 dev 共用） |
| `GHCR_PULL_TOKEN` | PAT（classic，scope 只勾 `read:packages`），与 `GHCR_PULL_USER` 配套（与 dev 共用） |

DB/Redis/OSS/SMTP 等业务凭证都在服务器 `.env` 里，**不进 GitHub Secrets**。

## dev / prod 对照

| 维度 | dev | prod（正式发布） |
|---|---|---|
| 触发分支 | `dev` | `main` |
| Workflow | [dev-cd.yml](../../.github/workflows/dev-cd.yml) | [prod-cd.yml](../../.github/workflows/prod-cd.yml) |
| 镜像 tag | `dev-latest` + `dev-<sha>` | `latest` + `sha-<sha>` |
| 服务器 secrets | `DEV_SSH_*` | `PROD_SSH_*` |
| 服务器目录 | `/opt/kawaru-dev` | `/opt/kawaru-prod` |
| compose project | `kawaru-dev` | `kawaru-prod` |
| compose 文件 | `docker-compose.dev.yml` | `docker-compose.prod.yml` |
| 配置文件 | `dev-config.yaml` | `prod-config.yaml` |
| DB/Redis 端口 | 暴露 `0.0.0.0:13306/16379`（便于外部检测） | 仅 `127.0.0.1:13306/16379`（不对公网开放） |
| 并发抢占 | `cancel-in-progress: true` | `cancel-in-progress: false`（不中断发布） |

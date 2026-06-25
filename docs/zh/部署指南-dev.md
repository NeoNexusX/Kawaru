# Dev 部署说明（dev-cd + docker compose）

push `dev` 分支后，[.github/workflows/dev-cd.yml](../../.github/workflows/dev-cd.yml) 自动：
构建镜像 → 推 GHCR（`dev-latest` + `dev-<sha>`）→ SSH 到 dev 服务器同步 compose → `pull && up -d --wait`。

dev-cd **只做构建与部署**，不生成/修改任何密钥或配置。`.env` 与 `dev-config.yaml` 由人一次性准备在服务器上，之后归你维护。

## 架构（Celery 分离模式）

```
mariadb(13306) ─┐
redis(16379)  ──┼─ app(8000)
                ├─ worker (celery -Q default)
                └─ beat   (celery 调度)
```

- `enable_embedded_celery: false`，app 不再 fork worker/beat；worker、beat 各自独立容器。

- 端口：app `8000`；mariadb 暴露 `13306`、redis 暴露 `16379`（非默认，便于外部检测）。

- 可以使用panel来查看对应的容器情况：

  https://testpanel.neonexus.top/containers/container

  

  ![image-20260625111959965](https://neonexus-picture.oss-ap-southeast-1.aliyuncs.com/test/image-20260625111959965.png)

## 服务器一次性准备（项目目录：/opt/kawaru-dev）

```bash
# 1) 目录
sudo mkdir -p /opt/kawaru-dev/data/{mariadb,redis,app,logs/{app,worker,beat}}
cd /opt/kawaru-dev

# 2) app/worker/beat 以 appuser(默认 uid 1000) 运行，给可写目录授权
#    确认 uid：docker run --rm ghcr.io/<owner>/kawaru:dev-latest id appuser
sudo chown -R 1000:1000 /opt/kawaru-dev/data/app /opt/kawaru-dev/data/logs

# 3) 放配置（从仓库复制模板，改成真实值）
.env 
还有config放到对应的目录，不能变化
```

之后每次 push `dev` 自动滚动升级，数据不丢。

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
cd /opt/kawaru-dev
# 查看配置 / 密码
cat .env
# 看日志（按服务拆分）
# 注意：docker-compose.dev.yml 里 image 引用了 ${APP_IMAGE} 变量（不在 .env 里），
# 直接跑 compose 命令会报 "required variable APP_IMAGE is missing a value"，
# 需先在当前 shell 导出该变量：
export APP_IMAGE=ghcr.io/neonexusx/kawaru:dev-latest
docker compose -f docker-compose.dev.yml -p kawaru-dev logs -f worker
# 改了 .env / dev-config.yaml 后手动生效
docker compose -f docker-compose.dev.yml -p kawaru-dev up -d
# 外部检测 DB / Redis
mysql -h <服务器IP> -P 13306 -uroot -p
redis-cli -h <服务器IP> -p 16379 -a <REDIS_PASSWORD> ping
#密码在env里面
# 确认 worker 已连上 broker（一次性，勿放进 healthcheck）
docker compose -f docker-compose.dev.yml -p kawaru-dev exec -T worker \
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
| `DEV_SSH_HOST` / `DEV_SSH_USER` / `DEV_SSH_KEY` | 独立 dev 服务器（22 端口） |
| `GHCR_PULL_USER` | GitHub 用户名（小写），dev-cd SSH 到服务器后用它登录 GHCR 拉私有镜像 |
| `GHCR_PULL_TOKEN` | PAT（classic，scope 只勾 `read:packages`），与 `GHCR_PULL_USER` 配套；过期前到 GitHub 重新生成并更新此 secret |

DB/Redis/OSS/SMTP 等业务凭证都在服务器 `.env` 里，**不进 GitHub Secrets**。

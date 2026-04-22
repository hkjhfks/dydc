# douyin_check pipeline

这是一个抖音归档流水线：
- 监控作者主页
- 下载新作品
- 上传到 Google Drive
- 推送企业微信通知

并且现在内置了 Web 控制台，可在前端维护作者列表（新增、查看、删除）。
在 Web 模式下：
- 新增/更新作者后会立即触发一轮下载上传
- 后台会按随机间隔持续执行监控任务（默认 55~65 分钟）

## 1) 前置依赖

### Python 依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### douyin-downloader（下载引擎）

```bash
git clone https://github.com/jiji262/douyin-downloader .tools/douyin-downloader
pip install -r .tools/douyin-downloader/requirements.txt
```

### rclone（上传 Google Drive）

安装并配置 remote（示例名称 `gdrive`）：
- 安装: https://rclone.org/install/
- 配置: https://rclone.org/drive/

验证：

```bash
rclone version
rclone lsd gdrive:
```

### 企业微信机器人 Webhook

在企业微信群添加机器人，拿到 webhook URL。

## 2) 配置

复制配置：

```bash
cp pipeline.example.yml pipeline.yml
```

编辑 `pipeline.yml`：
- `storage.rclone_remote`：你的 rclone remote（如 `gdrive`）
- `storage.base_dir`：云盘总目录（如 `DouyinArchive`）
- `wecom.webhook_url`：企业微信 webhook
- `authors[]`：可手工维护，也可以通过 Web 控制台维护

Cookie（建议放在不入库目录，如 `secrets/`）：
- `secrets/douyin_cookies.yml` 结构见 `pipeline.example.yml` 注释

## 3) 运行

### 检查配置与依赖

```bash
python3 -m pipeline --doctor
```

### 执行一轮

```bash
python3 -m pipeline --once
```

### 循环执行（每 10 分钟）

```bash
python3 -m pipeline --loop --interval 600
```

### 启动 Web 控制台

```bash
python3 -m pipeline --web --host 127.0.0.1 --port 8765
```

打开：`http://127.0.0.1:8765`

前端支持：
- 输入抖音主页链接后点击“自动获取”，自动回填抖音号和名字；也支持继续手动修改
- 输入抖音主页链接、抖音号、名字并新增/更新作者
- 列表展示当前作者
- 删除作者（删除后后续任务不再执行该作者）
- 查看后台任务状态（最近一轮成功/失败、触发原因）
- 手动点击“立即执行一轮”

说明：
- 自动获取依赖 `.tools/douyin-downloader` 及其 Python 依赖
- 如果作者未公开抖音号，页面会先回填 `sec_uid`，你可以再手动改成想要保存的标识

如需调整后台持续监控随机区间：

```bash
python3 -m pipeline --web --web-interval-min 3300 --web-interval-max 3900
```

## 4) 上传目录与本地清理

当前上传目录结构为：

```text
gdrive:DouyinArchive/<抖音号-名字>/xxx.mp4
```

其中 `xxx.mp4` 保持下载时原始文件名。

上传成功后会立即删除本地对应视频目录，减少硬盘占用。

## 5) 首次同步通知说明

新增作者后，默认首轮只发摘要通知，不逐条刷作品链接。

如果希望首次同步也逐条推送，把 `wecom.push_on_first_sync` 设为 `true`。

## 合规提醒

请在合法合规、尊重平台规则与版权前提下使用。

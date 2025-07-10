import os
import sys
import json
import time
import threading
import hashlib
import requests
import shutil
import urllib.parse
import logging
import traceback
from flask import Flask, request, jsonify
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ------------------ 配置日志 ------------------
logging.basicConfig(
    level=logging.INFO,  # 日志级别，可根据需要调整为 DEBUG, WARNING 等
    format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SyncNode")

# ------------------ Flask应用实例 ------------------
app = Flask(__name__)

# ------------------ 工具函数 ------------------

def file_md5(path: str) -> str:
    """
    计算指定文件的MD5值，方便文件变化检测
    :param path: 文件路径
    :return: 文件MD5字符串，异常返回None
    """
    hash_md5 = hashlib.md5()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    except Exception as e:
        logger.error(f"计算MD5失败: {path}，异常: {e}")
        return None
    return hash_md5.hexdigest()

def safe_path(base: str, path: str) -> str:
    """
    生成安全的绝对路径，防止目录穿越攻击
    :param base: 共享根目录
    :param path: 访问相对路径
    :return: 组合后的绝对路径
    :raises Exception: 如果路径越权则抛出异常
    """
    combined = os.path.normpath(os.path.join(base, path))
    base = os.path.abspath(base)
    if not combined.startswith(base):
        raise Exception(f"不允许越权访问路径: {path}")
    return combined

# ------------------ 文件系统事件处理 ------------------

class SyncHandler(FileSystemEventHandler):
    """
    Watchdog事件处理类，监听本地文件变化，
    并通过HTTP调用同步操作到对端
    """

    def __init__(self, config: dict):
        self.config = config
        self.recent_synced = {}  # 记录已同步文件的MD5，避免重复同步
        self.debounce_timers = {}  # 防抖计时器，避免频繁操作触发多次同步
        self.name = config['name']

    def should_skip(self, path: str) -> bool:
        """
        判断文件是否应该跳过同步（不存在或未变更）
        :param path: 文件绝对路径
        :return: True-跳过，False-需要同步
        """
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        if not os.path.exists(path):
            return True
        md5 = file_md5(path)
        if md5 is None:
            return True
        return self.recent_synced.get(filename) == md5

    def record_sync(self, path: str):
        """
        记录文件的MD5到recent_synced，表示已同步
        :param path: 文件绝对路径
        """
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        md5 = file_md5(path)
        if md5:
            self.recent_synced[filename] = md5

    def debounce_sync(self, path: str):
        """
        使用防抖机制，延迟同步操作，避免频繁修改多次触发同步
        :param path: 文件绝对路径
        """
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        # 取消之前的计时器
        if filename in self.debounce_timers:
            self.debounce_timers[filename].cancel()

        def do_sync():
            if not self.should_skip(path):
                self.sync_file(path)
                self.record_sync(path)
            self.debounce_timers.pop(filename, None)

        timer = threading.Timer(0.2, do_sync)  # 延迟0.2秒执行同步
        self.debounce_timers[filename] = timer
        timer.start()

    def sync_file(self, path: str):
        """
        同步文件到对端
        :param path: 文件绝对路径
        """
        filename = os.path.relpath(path, self.config['shared_folder']).replace("\\", "/")
        encoded = urllib.parse.quote(filename)
        url = f"http://{self.config['peer_ip']}:{self.config['peer_port']}/upload/{encoded}"
        for attempt in range(3):  # 最多重试3次
            try:
                with open(path, 'rb') as f:
                    resp = requests.post(url, files={'file': f}, timeout=10)
                if resp.status_code == 200:
                    logger.info(f"[{self.name}] 同步文件成功: {filename} → 对端")
                    return
                else:
                    logger.warning(f"[{self.name}] 同步文件响应异常({resp.status_code}): {filename}")
            except Exception:
                logger.error(f"[{self.name}] 同步文件失败第{attempt+1}次:\n{traceback.format_exc()}")
                time.sleep(1)
        logger.error(f"[{self.name}] 同步文件失败，超过最大重试次数: {filename}")

    def sync_delete(self, filename: str):
        """
        同步删除文件/文件夹到对端
        :param filename: 文件/文件夹相对路径
        """
        encoded = urllib.parse.quote(filename)
        url = f"http://{self.config['peer_ip']}:{self.config['peer_port']}/delete/{encoded}"
        try:
            resp = requests.post(url, timeout=10)
            if resp.status_code == 200:
                logger.info(f"[{self.name}] 同步删除成功: {filename} → 对端")
            else:
                logger.warning(f"[{self.name}] 同步删除响应异常({resp.status_code}): {filename}")
        except Exception:
            logger.error(f"[{self.name}] 同步删除失败:\n{traceback.format_exc()}")

    def sync_mkdir(self, foldername: str):
        """
        同步创建文件夹到对端
        :param foldername: 文件夹相对路径
        """
        encoded = urllib.parse.quote(foldername)
        url = f"http://{self.config['peer_ip']}:{self.config['peer_port']}/mkdir/{encoded}"
        try:
            resp = requests.post(url, timeout=10)
            if resp.status_code == 200:
                logger.info(f"[{self.name}] 同步创建文件夹成功: {foldername} → 对端")
            else:
                logger.warning(f"[{self.name}] 同步创建文件夹响应异常({resp.status_code}): {foldername}")
        except Exception:
            logger.error(f"[{self.name}] 同步创建文件夹失败:\n{traceback.format_exc()}")

    # 事件处理

    def on_created(self, event):
        rel_path = os.path.relpath(event.src_path, self.config['shared_folder']).replace("\\", "/")
        if event.is_directory:
            self.sync_mkdir(rel_path)
        else:
            self.debounce_sync(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self.debounce_sync(event.src_path)

    def on_deleted(self, event):
        rel_path = os.path.relpath(event.src_path, self.config['shared_folder']).replace("\\", "/")
        self.sync_delete(rel_path)

    def on_moved(self, event):
        old_rel = os.path.relpath(event.src_path, self.config['shared_folder']).replace("\\", "/")
        new_rel = os.path.relpath(event.dest_path, self.config['shared_folder']).replace("\\", "/")
        self.sync_delete(old_rel)
        if event.is_directory:
            self.sync_mkdir(new_rel)
        else:
            self.debounce_sync(event.dest_path)

# ------------------ Flask接口 ------------------

@app.route('/upload/<path:filename>', methods=['POST'])
def upload(filename):
    """
    接收对端上传的文件并保存
    """
    try:
        filepath = safe_path(app.config['shared_folder'], filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        request.files['file'].save(filepath)
        logger.info(f"[{app.config['name']}] 接收到文件: {filename}")
        return 'OK'
    except Exception:
        logger.error(f"[{app.config['name']}] 上传文件异常:\n{traceback.format_exc()}")
        return '上传失败', 400

@app.route('/delete/<path:filename>', methods=['POST'])
def delete_file_or_dir(filename):
    """
    接收对端删除文件或文件夹的请求
    """
    try:
        filepath = safe_path(app.config['shared_folder'], filename)
        logger.info(f"[{app.config['name']}] 请求删除: {filename}")
        if os.path.isfile(filepath):
            os.remove(filepath)
            logger.info(f"[{app.config['name']}] 删除文件成功: {filename}")
        elif os.path.isdir(filepath):
            shutil.rmtree(filepath)
            logger.info(f"[{app.config['name']}] 删除文件夹成功: {filename}")
        else:
            logger.warning(f"[{app.config['name']}] 路径不存在，无法删除: {filename}")
        return 'OK'
    except Exception:
        logger.error(f"[{app.config['name']}] 删除异常:\n{traceback.format_exc()}")
        return '删除失败', 400
@app.route("/ping")
def ping():
    return f"{app.config['name']} alive"

@app.route('/mkdir/<path:foldername>', methods=['POST'])
def mkdir(foldername):
    """
    接收对端创建文件夹请求
    """
    try:
        folderpath = safe_path(app.config['shared_folder'], foldername)
        os.makedirs(folderpath, exist_ok=True)
        logger.info(f"[{app.config['name']}] 创建文件夹成功: {foldername}")
        return 'OK'
    except Exception:
        logger.error(f"[{app.config['name']}] 创建文件夹异常:\n{traceback.format_exc()}")
        return '创建文件夹失败', 400

@app.route('/list_files', methods=['GET'])
def list_files():
    """
    返回当前共享目录所有文件的相对路径和MD5，供对端拉取同步时比对
    """
    file_list = []
    for root, _, files in os.walk(app.config['shared_folder']):
        for f in files:
            filepath = os.path.join(root, f)
            relpath = os.path.relpath(filepath, app.config['shared_folder']).replace("\\", "/")
            md5 = file_md5(filepath)
            if md5:
                file_list.append({"path": relpath, "md5": md5})
    return jsonify({"files": file_list})

@app.route('/download/<path:filename>', methods=['GET'])
def download(filename):
    """
    供对端下载指定文件内容
    """
    try:
        filepath = safe_path(app.config['shared_folder'], filename)
        if not os.path.isfile(filepath):
            return "文件不存在", 404
        return open(filepath, 'rb').read()
    except Exception:
        logger.error(f"[{app.config['name']}] 文件下载异常:\n{traceback.format_exc()}")
        return '下载失败', 400

# ------------------ 主同步拉取逻辑 ------------------

def pull_loop(config: dict):
    """
    循环拉取对端文件列表并比对，自动下载缺失或更新的文件
    """
    peer_url = f"http://{config['peer_ip']}:{config['peer_port']}"
    while True:
        try:
            r = requests.get(f"{peer_url}/list_files", timeout=5, proxies={"http": None, "https": None})
            if r.status_code == 200:
                peer_files = r.json().get("files", [])
                local_md5 = {}
                # 遍历本地文件，计算MD5
                for root, _, files in os.walk(config['shared_folder']):
                    for f in files:
                        p = os.path.join(root, f)
                        rel = os.path.relpath(p, config['shared_folder']).replace("\\", "/")
                        md5 = file_md5(p)
                        if md5:
                            local_md5[rel] = md5
                # 对比文件MD5，不同则拉取下载
                for f in peer_files:
                    path, md5 = f['path'], f['md5']
                    if local_md5.get(path) != md5:
                        encoded = urllib.parse.quote(path)
                        url = f"{peer_url}/download/{encoded}"
                        try:
                            resp = requests.get(url, timeout=10, proxies={"http": None, "https": None})
                            if resp.status_code == 200:
                                local_path = os.path.join(config['shared_folder'], path)
                                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                                with open(local_path, 'wb') as fw:
                                    fw.write(resp.content)
                                logger.info(f"[{config['name']}] 拉取并更新文件: {path}")
                            else:
                                logger.warning(f"[{config['name']}] 拉取文件响应异常({resp.status_code}): {path}")
                        except Exception:
                            logger.error(f"[{config['name']}] 拉取文件异常:\n{traceback.format_exc()}")
            else:
                logger.warning(f"[{config['name']}] 拉取文件列表响应异常({r.status_code})")
        except Exception:
            logger.error(f"[{config['name']}] 拉取异常:\n{traceback.format_exc()}")
        time.sleep(config.get('pull_interval', 10))  # 拉取间隔，默认10秒

# ------------------ 配置文件校验 ------------------

def validate_config(cfg: dict):
    """
    校验配置文件必填项，缺失则退出
    """
    required = ['name', 'shared_folder', 'my_port', 'peer_ip', 'peer_port']
    for k in required:
        if k not in cfg:
            logger.error(f"配置文件缺少必需字段: {k}")
            sys.exit(1)

# ------------------ 启动函数 ------------------

def run_flask_app():
    """
    启动Flask服务，监听HTTP接口
    """
    app.run(host='0.0.0.0', port=app.config['my_port'], debug=False, use_reloader=False)

# ------------------ 主程序入口 ------------------

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logger.error("用法: python sync_node_full.py config.json")
        sys.exit(1)

    with open(sys.argv[1], 'r', encoding='utf-8') as f:
        config = json.load(f)

    validate_config(config)

    shared_folder = os.path.abspath(config['shared_folder'])
    os.makedirs(shared_folder, exist_ok=True)

    # 保存配置到Flask app.config，方便接口访问
    app.config.update({
        'shared_folder': shared_folder,
        'my_port': config['my_port'],
        'name': config['name']
    })

    # 启动Flask HTTP服务
    threading.Thread(target=run_flask_app, daemon=True).start()

    # 启动Watchdog监听本地文件变更
    observer = Observer()
    observer.schedule(SyncHandler(config), path=shared_folder, recursive=True)
    observer.start()

    # 启动后台拉取线程，周期性从对端拉取文件列表同步文件
    threading.Thread(target=pull_loop, args=(config,), daemon=True).start()

    logger.info(f"[{config['name']}] 服务已启动，目录: {shared_folder}, 端口: {config['my_port']}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(f"[{config['name']}] 退出中，停止监听...")
        observer.stop()
    observer.join()
    logger.info(f"[{config['name']}] 服务已停止")

import logging
import os
import shutil
import subprocess
import threading
import queue
import time
from urllib.parse import urljoin
import concurrent.futures

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# 基本配置
class Config:
    def __init__(self, task_id):
        self.task_id = str(task_id)
        self.base_dir = f"task_{self.task_id}"

        # 目录设置
        self.log_dir = os.path.join(self.base_dir, "logs")
        self.download_path = os.path.join(self.base_dir, "ts_files")
        self.key_dir = os.path.join(self.base_dir, "keys")
        self.decrypted_dir = os.path.join(self.base_dir, "decrypted")
        self.output_dir = os.path.join(self.base_dir, "output")

        # 创建必要的目录
        for directory in [self.log_dir, self.download_path, self.key_dir, self.decrypted_dir, self.output_dir]:
            os.makedirs(directory, exist_ok=True)

        # 设置日志
        self.log_file = os.path.join(self.log_dir, f"task_{self.task_id}.log")
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(f"task_{self.task_id}")
        logger.setLevel(logging.INFO)

        # 文件处理器
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_formatter = logging.Formatter("%(asctime)s - [Task %(name)s] - %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formatter)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("%(asctime)s - [Task %(name)s] - %(message)s", "%Y-%m-%d %H:%M:%S")
        console_handler.setFormatter(console_formatter)

        # 清除已有的处理器
        if logger.handlers:
            logger.handlers.clear()

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger


# 任务状态常量
class TaskStatus:
    PENDING = "等待中"
    DOWNLOADING = "正在下载"
    DECRYPTING = "正在解密"
    MERGING = "正在合并"
    COMPLETED = "已完成"
    FAILED = "失败"
    CLEANING = "清理中"


class DownloadTask:
    def __init__(self, task_id, m3u8_url):
        self.task_id = task_id
        self.m3u8_url = m3u8_url
        self.status = TaskStatus.PENDING
        self.progress = 0
        self.config = Config(task_id)
        self.logger = self.config.logger
        self.key_info = None
        self.segment_count = 0
        self.downloaded_segments = 0

    def create_session(self):
        """配置请求会话，增加连接池和重试机制"""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=50,
            max_retries=retries
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def download_m3u8_segments(self):
        """下载M3U8文件并获取所有TS片段"""
        self.status = TaskStatus.DOWNLOADING
        self.logger.info(f"开始下载 M3U8: {self.m3u8_url}")

        session = self.create_session()

        try:
            response = session.get(self.m3u8_url)
            response.raise_for_status()

            m3u8_content = response.text
            ts_urls = []

            # 解析M3U8内容
            for line in m3u8_content.splitlines():
                line = line.strip()
                # 查找加密信息
                if line.startswith("#EXT-X-KEY"):
                    self.key_info = line
                    self.logger.info(f"找到加密信息: {self.key_info}")

                # 找出TS文件URL
                if line and not line.startswith("#"):
                    ts_urls.append(urljoin(self.m3u8_url, line))

            self.segment_count = len(ts_urls)
            self.logger.info(f"找到 {self.segment_count} 个TS文件链接")

            # 使用线程池并行下载
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self.download_segment, session, url, i): i for i, url in enumerate(ts_urls)}

                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result:  # 如果下载成功
                            self.downloaded_segments += 1
                            self.progress = int((self.downloaded_segments / self.segment_count) * 100)
                    except Exception as e:
                        self.logger.error(f"下载过程中出错: {e}")

            # # 检查下载完成情况
            # downloaded_files = [f for f in os.listdir(self.config.download_path)
            #                    if f.startswith("segment_") and os.path.getsize(os.path.join(self.config.download_path, f)) > 0]

            # if len(downloaded_files) < self.segment_count:
            #     self.logger.warning(f"有 {self.segment_count - len(downloaded_files)} 个文件未成功下载")
            #     # 重试下载失败的文件
            #     self.retry_failed_downloads(ts_urls, downloaded_files)

            self.logger.info("所有TS文件下载完成")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"下载M3U8文件失败: {e}")
            self.status = TaskStatus.FAILED
            return False

    def download_segment(self, session, ts_url, index):
        """下载单个TS片段"""
        ts_filename = os.path.join(self.config.download_path, f"segment_{index + 1:04d}.ts")

        # 如果文件已存在且大小大于0，跳过下载
        if os.path.exists(ts_filename) and os.path.getsize(ts_filename) > 0:
            return True

        try:
            # 使用流式下载，减少内存占用
            ts_response = session.get(ts_url, stream=True, timeout=(5, 15))
            ts_response.raise_for_status()

            with open(ts_filename, 'wb') as f:
                for chunk in ts_response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                    if chunk:
                        f.write(chunk)

            self.logger.info(f"片段 {index + 1}/{self.segment_count} 下载完成")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"下载片段 {index + 1} 失败: {e}")
            # 如果下载失败，创建空文件标记稍后重试
            with open(ts_filename, 'wb') as f:
                pass
            return False

    # def retry_failed_downloads(self, ts_urls, downloaded_files):
    #     """重试下载失败的文件"""
    #     session = self.create_session()

    #     # 获取已下载文件的索引
    #     downloaded_indices = set()
    #     for filename in downloaded_files:
    #         try:
    #             # 从"segment_0001.ts"格式的文件名提取索引
    #             index = int(filename.replace("segment_", "").replace(".ts", "")) - 1
    #             downloaded_indices.add(index)
    #         except ValueError:
    #             continue

    #     # 找出未下载的文件
    #     failed_downloads = [(i, url) for i, url in enumerate(ts_urls) if i not in downloaded_indices]

    #     if failed_downloads:
    #         self.logger.info(f"重试下载 {len(failed_downloads)} 个失败的文件")

    #         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #             futures = {executor.submit(self.download_segment, session, url, i): i for i, url in failed_downloads}

    #             for future in concurrent.futures.as_completed(futures):
    #                 try:
    #                     future.result()
    #                 except Exception as e:
    #                     self.logger.error(f"重试下载过程中出错: {e}")

    def extract_key_uri(self):
        """从#EXT-X-KEY行提取密钥URI"""
        if not self.key_info:
            return None

        parts = self.key_info.split(',')
        for part in parts:
            if part.startswith('URI='):
                uri = part.split('=', 1)[1].strip('"\'')
                return uri

        return None

    def extract_iv(self):
        """从#EXT-X-KEY行提取IV值"""
        if not self.key_info:
            return "00000000000000000000000000000000"  # 默认IV

        parts = self.key_info.split(',')
        for part in parts:
            if part.startswith('IV='):
                iv = part.split('=', 1)[1]
                if iv.startswith('0x'):
                    iv = iv[2:]  # 移除0x前缀
                return iv

        return "00000000000000000000000000000000"  # 默认IV

    def download_key(self, key_uri):
        """下载解密密钥"""
        key_file = os.path.join(self.config.key_dir, "encrypt.key")

        session = self.create_session()

        try:
            key_response = session.get(key_uri)
            key_response.raise_for_status()

            with open(key_file, 'wb') as f:
                f.write(key_response.content)

            self.logger.info(f"密钥已下载并保存到 {key_file}")
            return key_file

        except requests.exceptions.RequestException as e:
            self.logger.error(f"下载密钥失败: {e}")
            return None

    def decrypt_ts_files(self, key_file, iv):
        """解密所有TS文件"""
        self.status = TaskStatus.DECRYPTING
        self.logger.info("开始解密TS文件...")

        if not os.path.exists(key_file):
            self.logger.error(f"{key_file} 不存在！")
            self.status = TaskStatus.FAILED
            return False

        try:
            # 读取密钥
            with open(key_file, 'rb') as f:
                key_content = f.read()

            # 转换为hex格式
            key_hex = key_content.hex()

            # 查找TS源文件
            ts_files = sorted([f for f in os.listdir(self.config.download_path)
                               if f.startswith("segment_") and f.endswith(".ts")])

            if not ts_files:
                self.logger.error("没有找到任何TS文件")
                self.status = TaskStatus.FAILED
                return False

            total_files = len(ts_files)
            self.logger.info(f"开始解密 {total_files} 个TS文件")

            # 使用线程池并行解密
            with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = {executor.submit(self.decrypt_single_file, filename, key_hex, iv): filename
                           for filename in ts_files}

                decrypted_count = 0
                for future in concurrent.futures.as_completed(futures):
                    try:
                        if future.result():  # 如果解密成功
                            decrypted_count += 1
                            self.progress = int((decrypted_count / total_files) * 100)
                    except Exception as e:
                        self.logger.error(f"解密过程中出错: {e}")

            self.logger.info("所有TS文件解密完成！")
            return True

        except Exception as e:
            self.logger.error(f"解密过程出错: {e}")
            self.status = TaskStatus.FAILED
            return False

    def decrypt_single_file(self, filename, key_hex, iv):
        """解密单个TS文件"""
        input_file = os.path.join(self.config.download_path, filename)
        output_file = os.path.join(self.config.decrypted_dir, filename)

        # 检查文件大小，跳过空文件
        if os.path.getsize(input_file) == 0:
            self.logger.warning(f"跳过空文件: {filename}")
            return False

        try:
            # 使用subprocess调用openssl进行解密
            cmd = [
                "openssl", "aes-128-cbc", "-d",
                "-in", input_file,
                "-out", output_file,
                "-nosalt",
                "-iv", iv,
                "-K", key_hex
            ]

            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True

        except subprocess.CalledProcessError as e:
            self.logger.error(f"解密失败: {filename}, 错误: {e}")
            return False

    def copy_to_decrypted(self):
        """如果不需要解密，将文件复制到解密目录"""
        self.logger.info("未找到加密信息，复制文件到解密目录")

        ts_files = [f for f in os.listdir(self.config.download_path)
                    if f.startswith("segment_") and f.endswith(".ts")]

        # 使用线程池并行复制
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for ts_file in ts_files:
                input_path = os.path.join(self.config.download_path, ts_file)
                # 跳过空文件
                if os.path.getsize(input_path) == 0:
                    continue

                output_path = os.path.join(self.config.decrypted_dir, ts_file)
                futures.append(
                    executor.submit(shutil.copy, input_path, output_path)
                )

            # 等待所有复制任务完成
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"复制文件失败: {e}")

        return True

    def merge_ts_files(self):
        """使用ffmpeg合并TS文件"""
        self.status = TaskStatus.MERGING
        self.logger.info("开始合并TS文件...")

        # 生成文件列表
        file_list = os.path.join(self.config.base_dir, "file_list.txt")
        ts_files = sorted([f for f in os.listdir(self.config.decrypted_dir)
                           if f.startswith("segment_") and f.endswith(".ts")])

        if not ts_files:
            self.logger.error("没有找到任何解密后的TS文件")
            self.status = TaskStatus.FAILED
            return False

        # 检查文件大小，跳过空文件
        valid_ts_files = []
        for ts_file in ts_files:
            file_path = os.path.join(self.config.decrypted_dir, ts_file)
            if os.path.getsize(file_path) > 0:
                valid_ts_files.append(ts_file)
            else:
                self.logger.warning(f"跳过空文件: {ts_file}")

        if not valid_ts_files:
            self.logger.error("没有有效的TS文件可以合并")
            self.status = TaskStatus.FAILED
            return False

        # 使用绝对路径
        with open(file_list, 'w', encoding='utf-8') as f:
            for ts_file in valid_ts_files:
                # 使用绝对路径并确保路径分隔符正确
                file_path = os.path.abspath(os.path.join(self.config.decrypted_dir, ts_file))
                file_path = file_path.replace('\\', '/')  # 确保路径格式正确
                f.write(f"file '{file_path}'\n")

        # 合并解密后的TS文件
        output_video = os.path.join(self.config.output_dir, f"task_{self.task_id}_output.mp4")

        # 检查输出文件是否已存在，如果存在则删除
        if os.path.exists(output_video):
            try:
                os.remove(output_video)
                self.logger.info(f"已删除现有的输出文件: {output_video}")
            except Exception as e:
                self.logger.error(f"无法删除现有的输出文件: {e}")

        try:
            # 移除 -loglevel quiet 以便查看详细错误
            process = subprocess.run(
                ["ffmpeg", "-f", "concat", "-safe", "0", "-i", file_list, "-c", "copy", output_video],
                check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE
            )
            self.logger.info(f"TS文件已合并为 {output_video}")

            # 检查输出文件是否创建且大小大于0
            if os.path.exists(output_video) and os.path.getsize(output_video) > 0:
                self.logger.info(f"成功生成输出文件，大小: {os.path.getsize(output_video) / (1024 * 1024):.2f} MB")
            else:
                self.logger.error("输出文件创建失败或大小为0")
                self.status = TaskStatus.FAILED
                return False

            # 清理临时文件
            try:
                os.remove(file_list)
            except Exception as e:
                self.logger.warning(f"无法删除临时文件列表: {e}")

            self.status = TaskStatus.COMPLETED
            return True

        except subprocess.CalledProcessError as e:
            self.logger.error(f"合并失败: {e}")
            # 记录ffmpeg的具体错误输出
            if e.stderr:
                self.logger.error(f"ffmpeg错误输出: {e.stderr.decode('utf-8', errors='replace')}")
            self.status = TaskStatus.FAILED
            return False

    def clean_files(self):
        """清理临时文件"""
        self.status = TaskStatus.CLEANING
        self.logger.info("开始清理临时文件...")

        # 清理TS原文件
        for directory in [self.config.download_path, self.config.decrypted_dir]:
            for file in os.listdir(directory):
                file_path = os.path.join(directory, file)
                if os.path.isfile(file_path):
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        self.logger.error(f"删除文件 {file_path} 失败: {e}")

        self.logger.info("临时文件清理完成")

    def execute(self):
        """执行下载任务的全过程"""
        try:
            # 下载M3U8和TS片段
            if not self.download_m3u8_segments():
                return False

            # 处理加密（如果有）
            if self.key_info:
                key_uri = self.extract_key_uri()
                iv = self.extract_iv()

                if key_uri:
                    # 下载密钥
                    key_file = self.download_key(urljoin(self.m3u8_url, key_uri))

                    if key_file:
                        # 解密TS文件
                        if not self.decrypt_ts_files(key_file, iv):
                            return False
                    else:
                        self.logger.error("获取密钥失败")
                        self.status = TaskStatus.FAILED
                        return False
                else:
                    self.logger.info("未找到加密信息，假设文件未加密")
                    # 将原始文件复制到解密目录
                    if not self.copy_to_decrypted():
                        return False
            else:
                self.logger.info("未找到加密信息，假设文件未加密")
                # 将原始文件复制到解密目录
                if not self.copy_to_decrypted():
                    return False

            # 合并文件
            return self.merge_ts_files()

        except Exception as e:
            self.logger.error(f"任务执行过程中出错: {e}")
            self.status = TaskStatus.FAILED
            return False


class TaskManager:
    def __init__(self):
        self.tasks = {}
        self.task_counter = 0
        self.task_queue = queue.Queue()
        self.max_concurrent_tasks = 50  # 最大并发任务数
        self.active_tasks = 0
        self.lock = threading.Lock()

        # 设置主日志
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - [Manager] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[
                logging.FileHandler("manager.log", encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("Manager")

        # 启动任务处理线程
        self.worker_thread = threading.Thread(target=self._process_tasks)
        self.worker_thread.daemon = True
        self.worker_thread.start()

    def add_task(self, m3u8_url):
        """添加新的下载任务"""
        with self.lock:
            task_id = self.task_counter
            self.task_counter += 1

            task = DownloadTask(task_id, m3u8_url)
            self.tasks[task_id] = task
            self.task_queue.put(task_id)

            self.logger.info(f"新任务已添加: Task ID {task_id} - {m3u8_url}")
            return task_id

    def add_batch_tasks(self, url_file_path):
        """从文件中批量添加下载任务

        参数:
        url_file_path (str): 包含M3U8 URL的文本文件路径，每行一个URL

        返回:
        list: 添加的任务ID列表
        """
        added_task_ids = []

        try:
            with open(url_file_path, 'r', encoding='utf-8') as f:
                urls = f.readlines()

            # 过滤空行并移除空白字符
            urls = [url.strip() for url in urls if url.strip()]

            if not urls:
                self.logger.warning(f"文件 {url_file_path} 中没有找到有效的URL")
                return added_task_ids

            self.logger.info(f"从 {url_file_path} 中找到 {len(urls)} 个URL")

            # 添加每个URL作为新任务
            for url in urls:
                task_id = self.add_task(url)
                added_task_ids.append(task_id)

            self.logger.info(f"成功添加 {len(added_task_ids)} 个批量任务")
            return added_task_ids

        except Exception as e:
            self.logger.error(f"批量添加任务失败: {e}")
            return added_task_ids

    def _process_tasks(self):
        """处理任务队列的工作线程"""
        while True:
            # 检查是否可以启动新任务
            with self.lock:
                if self.active_tasks >= self.max_concurrent_tasks:
                    time.sleep(1)
                    continue

            try:
                # 获取下一个任务
                task_id = self.task_queue.get(block=True, timeout=1)

                with self.lock:
                    self.active_tasks += 1

                # 在新线程中执行任务
                task_thread = threading.Thread(target=self._execute_task, args=(task_id,))
                task_thread.daemon = True
                task_thread.start()

            except queue.Empty:
                # 队列为空，等待新任务
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"处理任务时出错: {e}")
                with self.lock:
                    self.active_tasks -= 1

    def _execute_task(self, task_id):
        """执行单个任务"""
        try:
            task = self.tasks[task_id]
            self.logger.info(f"开始执行任务 {task_id}")

            # 执行任务
            result = task.execute()

            if result:
                self.logger.info(f"任务 {task_id} 已完成")
            else:
                self.logger.error(f"任务 {task_id} 失败")

        except Exception as e:
            self.logger.error(f"执行任务 {task_id} 时出错: {e}")
        finally:
            with self.lock:
                self.active_tasks -= 1

    def get_task_status(self, task_id):
        """获取任务状态"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            return {
                "task_id": task_id,
                "status": task.status,
                "progress": task.progress,
                "url": task.m3u8_url
            }
        return None

    def get_all_task_status(self):
        """获取所有任务的状态"""
        status_list = []
        for task_id, task in self.tasks.items():
            status_list.append({
                "task_id": task_id,
                "status": task.status,
                "progress": task.progress,
                "url": task.m3u8_url
            })
        return status_list

    def clean_task(self, task_id):
        """清理任务临时文件"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.clean_files()
            return True
        return False


def display_menu():
    """显示主菜单"""
    print("\n" + "=" * 50)
    print("M3U8多任务下载器")
    print("=" * 50)
    print("1. 添加新下载任务")
    print("2. 批量添加下载任务")
    print("3. 查看所有任务状态")
    print("4. 清理已完成任务的临时文件")
    print("5. 退出程序")
    print("=" * 50)


def main():
    # 创建任务管理器
    manager = TaskManager()

    while True:
        display_menu()
        choice = input("请选择操作 (1-5): ")

        if choice == '1':
            m3u8_url = input("请输入M3U8视频URL: ").strip()
            if m3u8_url:
                task_id = manager.add_task(m3u8_url)
                print(f"已添加任务，ID: {task_id}")
            else:
                print("URL不能为空！")

        elif choice == '2':
            file_path = input("请输入包含M3U8 URL的文件路径 (每行一个URL): ").strip()
            if os.path.exists(file_path):
                task_ids = manager.add_batch_tasks(file_path)
                if task_ids:
                    print(f"已成功添加 {len(task_ids)} 个任务，ID范围: {min(task_ids)} - {max(task_ids)}")
                else:
                    print("未添加任何任务，请检查文件内容")
            else:
                print(f"文件 {file_path} 不存在！")

        elif choice == '3':
            status_list = manager.get_all_task_status()
            if status_list:
                print("\n当前任务状态:")
                print("-" * 80)
                print(f"{'ID':<5} {'状态':<10} {'进度':<10} {'URL':<50}")
                print("-" * 80)

                for status in status_list:
                    # 截断过长的URL，以确保格式正常
                    url = status['url']
                    if len(url) > 45:
                        url = url[:42] + "..."
                    print(f"{status['task_id']:<5} {status['status']:<10} {status['progress']:>3}% {url}")
            else:
                print("当前没有任务")

        elif choice == '4':
            task_id = input("请输入要清理的任务ID (输入 'all' 清理所有已完成任务): ")
            if task_id.lower() == 'all':
                status_list = manager.get_all_task_status()
                cleaned_count = 0

                for status in status_list:
                    if status['status'] == TaskStatus.COMPLETED:
                        manager.clean_task(status['task_id'])
                        cleaned_count += 1

                print(f"已清理 {cleaned_count} 个任务的临时文件")
            else:
                try:
                    task_id = int(task_id)
                    if manager.clean_task(task_id):
                        print(f"任务 {task_id} 的临时文件已清理")
                    else:
                        print(f"任务 {task_id} 不存在")
                except ValueError:
                    print("无效的任务ID")

        elif choice == '5':
            print("正在退出程序...")
            break

        else:
            print("无效选择，请重试")

        # 短暂暂停，让用户查看结果
        time.sleep(1)


if __name__ == "__main__":
    main()
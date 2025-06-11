#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/11 10:59
# @Author     : @MrNanko
# @File       : task
# @Software   : PyCharm
# @Description:

import asyncio
import functools
import logging
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
import threading
import re
import importlib
import pkgutil
import inspect

logger = logging.getLogger(__name__)

# 全局任务管理器
class TaskManager:
    """任务管理器"""

    def __init__(self):
        self._tasks = {}
        self._running = False
        self._loop = None
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._loop_thread = None
        self._lock = threading.Lock()  # 添加线程锁保护

    def get_current_loop(self):
        """获取当前可用的事件循环"""
        try:
            # 优先返回当前运行的循环
            return asyncio.get_running_loop()
        except RuntimeError:
            # 如果没有运行的循环，返回管理器的循环
            return self._loop

    def start_background_loop(self):
        """仅在必要时启动后台循环"""
        with self._lock:  # 线程安全的启动检查
            # 检查是否真的需要后台循环
            try:
                current_loop = asyncio.get_running_loop()
                logger.info("检测到运行中的循环，不需要后台循环")
                self._loop = current_loop
                self._running = True
                return
            except RuntimeError:
                pass

            if self._loop_thread and self._loop_thread.is_alive():
                logger.info("后台循环已存在，无需重复创建")
                return

            def run_background_loop():
                """在后台线程中运行事件循环"""
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    self._loop = loop
                    self._running = True

                    logger.info("后台事件循环已启动")
                    loop.run_forever()

                except Exception as e:
                    logger.error(f"后台循环运行出错: {e}")
                finally:
                    logger.info("后台事件循环已停止")
                    self._running = False
                    self._loop = None  # 清理循环引用

            self._loop_thread = threading.Thread(target=run_background_loop, daemon=True)
            self._loop_thread.start()

            # 更安全的等待循环启动
            max_wait = 50  # 最多等待5秒
            wait_count = 0
            while not self._running and wait_count < max_wait:
                time.sleep(0.1)
                wait_count += 1

            if not self._running:
                raise RuntimeError("后台循环启动超时")

    def add_task(self, task_id: str, task):
        """添加任务"""
        with self._lock:
            # 检查任务ID冲突
            if task_id in self._tasks:
                logger.warning(f"任务ID {task_id} 已存在，将替换旧任务")
                self.remove_task(task_id)

            self._tasks[task_id] = task
            logger.debug(f"任务 {task_id} 已添加到管理器")

    def remove_task(self, task_id: str):
        """移除任务"""
        with self._lock:
            task = self._tasks.pop(task_id, None)
            if task:
                try:
                    if hasattr(task, 'cancel') and hasattr(task, 'done'):
                        if not task.done():
                            task.cancel()
                            logger.debug(f"已取消Task任务: {task_id}")
                    elif hasattr(task, 'result'):
                        # Future 对象
                        if not task.done():
                            task.cancel()
                            logger.debug(f"已取消Future任务: {task_id}")
                except Exception as e:
                    logger.warning(f"取消任务 {task_id} 时出错: {e}")

    def cancel_all_tasks(self):
        """取消所有任务"""
        with self._lock:
            logger.info(f"正在取消 {len(self._tasks)} 个任务")

            for task_id, task in list(self._tasks.items()):  # 避免迭代时修改字典
                try:
                    if hasattr(task, 'cancel') and hasattr(task, 'done'):
                        if not task.done():
                            task.cancel()
                    elif hasattr(task, 'result'):
                        if not task.done():
                            task.cancel()
                except Exception as e:
                    logger.warning(f"取消任务 {task_id} 时出错: {e}")

            self._tasks.clear()

            # 更安全的循环停止
            if self._loop and not self._loop.is_closed() and self._running:
                try:
                    if self._loop.is_running():
                        self._loop.call_soon_threadsafe(self._loop.stop)
                        # 等待循环停止
                        if self._loop_thread and self._loop_thread.is_alive():
                            self._loop_thread.join(timeout=2.0)
                except Exception as e:
                    logger.warning(f"停止事件循环时出错: {e}")
                finally:
                    self._running = False

    def get_task_count(self):
        """获取任务数量"""
        with self._lock:
            return len(self._tasks)

    def get_task_info(self):
        """获取详细任务信息"""
        with self._lock:
            task_info = {}
            for task_id, task in self._tasks.items():
                try:
                    if hasattr(task, 'done'):
                        status = "completed" if task.done() else "running"
                        if task.done() and hasattr(task, 'exception'):
                            exception = task.exception()
                            if exception:
                                status = f"failed: {exception}"
                    else:
                        status = "unknown"

                    task_info[task_id] = {
                        "status": status,
                        "type": type(task).__name__
                    }
                except Exception as e:
                    task_info[task_id] = {"status": f"error: {e}", "type": "unknown"}

            return task_info

    def cleanup(self):
        """清理已完成的任务"""
        with self._lock:
            completed_tasks = []
            for task_id, task in self._tasks.items():
                try:
                    if hasattr(task, 'done') and task.done():
                        completed_tasks.append(task_id)
                    elif hasattr(task, 'result'):
                        if task.done():
                            completed_tasks.append(task_id)
                except Exception as e:
                    logger.warning(f"检查任务 {task_id} 状态时出错: {e}")
                    completed_tasks.append(task_id)  # 有问题的任务也清理掉

            for task_id in completed_tasks:
                del self._tasks[task_id]
                logger.debug(f"已清理完成的任务: {task_id}")

            return len(completed_tasks)


class DecoratorRegistry:
    """装饰器注册表"""

    def __init__(self):
        self._registry = {
            'schedule_cron': [],
            'schedule_interval': [],
            'schedule_delay': [],
            'schedule_at': []
        }
        self._function_mapping = {}

    def register_function(self, decorator_type: str, func: Callable, config: Dict = None):
        """注册装饰器函数"""
        func_info = {
            'name': func.__name__,
            'function': func,
            'module': func.__module__,
            'config': config or {},
        }

        if decorator_type not in self._registry:
            self._registry[decorator_type] = []

        self._registry[decorator_type].append(func_info)
        self._function_mapping[func.__name__] = func
        logger.debug(f"注册装饰器函数: {decorator_type}.{func.__name__}")

    def get_functions_by_type(self, decorator_type: str) -> List[Dict]:
        """获取指定类型的所有函数"""
        return self._registry.get(decorator_type, [])

    def get_all_functions(self) -> Dict[str, List[Dict]]:
        """获取所有注册的函数"""
        return self._registry.copy()

    def get_summary(self) -> Dict:
        """获取注册摘要"""
        summary = {}
        total_functions = 0

        for decorator_type, functions in self._registry.items():
            count = len(functions)
            total_functions += count
            summary[decorator_type] = {
                'count': count,
                'functions': [f['name'] for f in functions]
            }

        summary['total'] = total_functions
        return summary


class DecoratorDiscovery:
    """装饰器函数自动发现器"""

    def __init__(self, package_name: str = None):
        self.package_name = package_name
        self.registry = registry

    def discover_handlers_package(self, handlers_package: str = "handlers"):
        """自动发现handlers包中所有使用装饰器的函数"""
        discovered_functions = []

        try:
            if self.package_name and not handlers_package.startswith(self.package_name):
                full_package_name = f"{self.package_name}.{handlers_package}"
            else:
                full_package_name = handlers_package

            logger.info(f"开始扫描包: {full_package_name}")

            try:
                package = importlib.import_module(full_package_name)
            except ImportError as e:
                logger.error(f"无法导入包 {full_package_name}: {e}")
                return discovered_functions

            if hasattr(package, '__path__'):
                package_path = package.__path__
            else:
                logger.warning(f"包 {full_package_name} 没有 __path__ 属性")
                return discovered_functions

            for importer, modname, ispkg in pkgutil.iter_modules(package_path, full_package_name + "."):
                if ispkg:
                    continue

                try:
                    logger.debug(f"扫描模块: {modname}")
                    module = importlib.import_module(modname)
                    module_functions = self._scan_module_for_decorators(module)
                    discovered_functions.extend(module_functions)

                except ImportError as e:
                    logger.warning(f"无法导入模块 {modname}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"扫描模块 {modname} 时出错: {e}")
                    continue

            logger.info(f"发现 {len(discovered_functions)} 个装饰器函数")
            return discovered_functions

        except Exception as e:
            logger.error(f"扫描包 {handlers_package} 时出错: {e}")
            return discovered_functions

    def _scan_module_for_decorators(self, module) -> List[Dict]:
        """扫描模块中的装饰器函数"""
        found_functions = []

        for name, obj in inspect.getmembers(module):
            if inspect.isfunction(obj):
                decorator_info = self._get_decorator_info(obj)
                if decorator_info:
                    found_functions.append({
                        'name': name,
                        'function': obj,
                        'module': module.__name__,
                        'decorator_type': decorator_info['type'],
                        'config': decorator_info.get('config', {})
                    })
                    logger.debug(f"发现装饰器函数: {module.__name__}.{name} ({decorator_info['type']})")

        return found_functions

    def _get_decorator_info(self, func) -> Optional[Dict]:
        """获取函数的装饰器信息"""
        if hasattr(func, 'task_id') and hasattr(func, 'cron_expression'):
            return {
                'type': 'schedule_cron',
                'config': {
                    'task_id': func.task_id,
                    'cron_expression': func.cron_expression
                }
            }

        if hasattr(func, 'task_id') and hasattr(func, '_schedule_interval'):
            return {
                'type': 'schedule_interval',
                'config': getattr(func, '_schedule_config', {})
            }

        return None

    def auto_start_discovered_functions(self, decorator_types: List[str] = None) -> Dict:
        """自动启动发现的装饰器函数"""
        if decorator_types is None:
            decorator_types = ['schedule_cron', 'schedule_interval', 'schedule_delay', 'schedule_at']

        started_tasks = {}
        failed_tasks = {}

        all_functions = self.registry.get_all_functions()

        for decorator_type in decorator_types:
            if decorator_type not in all_functions:
                continue

            started_tasks[decorator_type] = []
            failed_tasks[decorator_type] = []

            functions = all_functions[decorator_type]
            logger.info(f"启动 {len(functions)} 个 {decorator_type} 任务")

            for func_info in functions:
                try:
                    func = func_info['function']
                    func_name = func_info['name']

                    logger.info(f"启动任务: {func_name} ({decorator_type})")
                    task_result = func()

                    started_tasks[decorator_type].append({
                        'name': func_name,
                        'module': func_info['module'],
                        'task_result': task_result
                    })

                    logger.info(f"任务启动成功: {func_name}")

                except Exception as e:
                    logger.error(f"启动任务失败: {func_info['name']}, 错误: {e}")
                    failed_tasks[decorator_type].append({
                        'name': func_info['name'],
                        'module': func_info['module'],
                        'error': str(e)
                    })

        return {
            'started': started_tasks,
            'failed': failed_tasks,
            'total_started': sum(len(tasks) for tasks in started_tasks.values()),
            'total_failed': sum(len(tasks) for tasks in failed_tasks.values())
        }

# ================ CronParser 类 ================

class CronParser:
    """Cron表达式解析器"""

    # 星期名称映射
    WEEKDAY_NAMES = {
        'sun': 0, 'sunday': 0,
        'mon': 1, 'monday': 1,
        'tue': 2, 'tuesday': 2,
        'wed': 3, 'wednesday': 3,
        'thu': 4, 'thursday': 4,
        'fri': 5, 'friday': 5,
        'sat': 6, 'saturday': 6
    }

    # 月份名称映射
    MONTH_NAMES = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }

    def __init__(self, cron_expression: str):
        """初始化Cron解析器"""
        self.cron_expression = cron_expression.strip()
        self.fields = self._parse_expression()

    def _parse_expression(self) -> dict:
        """解析Cron表达式"""
        parts = self.cron_expression.split()

        if len(parts) == 5:
            # 5位格式: 分 时 日 月 周
            minute, hour, day, month, weekday = parts
            second = "0"  # 默认0秒
        elif len(parts) == 6:
            # 6位格式: 秒 分 时 日 月 周
            second, minute, hour, day, month, weekday = parts
        else:
            raise ValueError(f"无效的Cron表达式: {self.cron_expression}")

        return {
            'second': self._parse_field(second, 0, 59),
            'minute': self._parse_field(minute, 0, 59),
            'hour': self._parse_field(hour, 0, 23),
            'day': self._parse_field(day, 1, 31),
            'month': self._parse_field(month, 1, 12, self.MONTH_NAMES),
            'weekday': self._parse_field(weekday, 0, 6, self.WEEKDAY_NAMES)
        }

    def _parse_field(self, field: str, min_val: int, max_val: int,
                     name_mapping: dict = None) -> set:
        """解析单个字段"""
        if field == '*':
            return set(range(min_val, max_val + 1))

        values = set()

        # 处理逗号分隔的多个值
        for part in field.split(','):
            part = part.strip()

            # 处理名称映射
            if name_mapping:
                for name, value in name_mapping.items():
                    part = re.sub(rf'\b{name}\b', str(value), part, flags=re.IGNORECASE)

            # 处理范围和步长
            if '/' in part:
                range_part, step = part.split('/', 1)
                step = int(step)
            else:
                range_part = part
                step = 1

            # 处理范围
            if '-' in range_part:
                start, end = range_part.split('-', 1)
                start = int(start)
                end = int(end)
            elif range_part == '*':
                start = min_val
                end = max_val
            else:
                start = end = int(range_part)

            # 验证范围
            if start < min_val or end > max_val or start > end:
                raise ValueError(f"字段值超出范围: {part}")

            # 添加值
            values.update(range(start, end + 1, step))

        return values

    def get_next_run_time(self, current_time: datetime = None) -> datetime:
        """计算下次运行时间 - 修复版本"""
        if current_time is None:
            current_time = datetime.now()

        # 修复关键问题：总是从下一秒开始搜索，不管秒字段包含什么
        next_time = current_time.replace(microsecond=0) + timedelta(seconds=1)

        # 最多尝试4年（防止无限循环）
        max_attempts = 4 * 365 * 24 * 60 * 60  # 改为秒数，更精确
        attempts = 0

        while attempts < max_attempts:
            if self._matches_time(next_time):
                return next_time

            # 递增到下一秒
            next_time += timedelta(seconds=1)
            attempts += 1

        raise ValueError(f"无法找到匹配的执行时间: {self.cron_expression}")

    def _matches_time(self, dt: datetime) -> bool:
        """检查时间是否匹配Cron表达式"""
        # 检查基本时间字段
        if (dt.second not in self.fields['second'] or
                dt.minute not in self.fields['minute'] or
                dt.hour not in self.fields['hour'] or
                dt.month not in self.fields['month']):
            return False

        # 检查日期和星期（只要其中一个匹配即可）
        day_match = dt.day in self.fields['day']
        weekday_match = dt.weekday() in self._convert_weekday(self.fields['weekday'])

        # 如果day和weekday都不是*，则需要其中一个匹配
        day_is_wildcard = self.fields['day'] == set(range(1, 32))
        weekday_is_wildcard = self.fields['weekday'] == set(range(0, 7))

        if day_is_wildcard and weekday_is_wildcard:
            return True
        elif day_is_wildcard:
            return weekday_match
        elif weekday_is_wildcard:
            return day_match
        else:
            return day_match or weekday_match

    def _convert_weekday(self, cron_weekdays: set) -> set:
        """
        转换星期格式
        Cron: 0=Sunday, 1=Monday, ..., 6=Saturday
        Python: 0=Monday, 1=Tuesday, ..., 6=Sunday
        """
        python_weekdays = set()
        for cron_day in cron_weekdays:
            if cron_day == 0:  # Sunday
                python_weekdays.add(6)
            else:  # Monday-Saturday
                python_weekdays.add(cron_day - 1)
        return python_weekdays


# ================ 装饰器辅助函数 ================

def _create_task_smartly(coro, task_id: str):
    """创建任务 - 所有装饰器通用的策略"""
    task = None

    try:
        # 策略1: 尝试使用当前运行的事件循环（异步环境）
        loop = asyncio.get_running_loop()
        logger.debug(f"检测到运行中的事件循环，直接使用")
        task = loop.create_task(coro)

        # 更新任务管理器状态
        task_manager._loop = loop
        task_manager._running = True

    except RuntimeError:
        # 策略2: 没有运行的循环，检查是否已有后台循环
        logger.debug(f"没有运行的事件循环，检查后台循环状态")

        if task_manager._running and task_manager._loop and not task_manager._loop.is_closed():
            # 使用现有的后台循环
            logger.debug(f"使用现有后台循环")
            future = asyncio.run_coroutine_threadsafe(coro, task_manager._loop)
            task = future
        else:
            # 策略3: 创建新的后台循环（仅作为最后手段）
            logger.info(f"创建新的后台事件循环")
            task_manager.start_background_loop()
            future = asyncio.run_coroutine_threadsafe(coro, task_manager._loop)
            task = future

    return task

def _execute_function_smartly(func, args, kwargs):
    """执行函数 - 处理同步/异步函数"""
    if asyncio.iscoroutinefunction(func):
        return func(*args, **kwargs)
    else:
        # 获取当前循环来执行同步函数
        try:
            loop = asyncio.get_running_loop()
            return loop.run_in_executor(task_manager._executor, lambda: func(*args, **kwargs))
        except RuntimeError:
            # 如果没有运行循环，直接执行
            return func(*args, **kwargs)

# ================ 错误处理辅助函数 ================

async def _handle_error_smartly(on_error: Callable, error: Exception, func: Callable, args, kwargs):
    """错误处理"""
    if asyncio.iscoroutinefunction(on_error):
        await on_error(error, func, args, kwargs)
    else:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(task_manager._executor,
                                       lambda: on_error(error, func, args, kwargs))
        except RuntimeError:
            on_error(error, func, args, kwargs)

# ================ schedule_interval 装饰器 ================

def schedule_interval(seconds: int = None, minutes: int = None, hours: int = None,
                      immediate: bool = False, task_id: str = None,
                      max_instances: int = 1, on_error: Callable = None):

    def decorator(func: Callable):
        # 计算总间隔时间（秒）
        total_seconds = (seconds or 0) + (minutes or 0) * 60 + (hours or 0) * 3600
        if total_seconds <= 0:
            raise ValueError("必须指定正数的时间间隔")

        actual_task_id = task_id or f"interval_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            instance_count = 0

            async def interval_task():
                nonlocal instance_count

                logger.info(f"间隔任务已启动: {actual_task_id}")
                logger.debug(f"间隔: {total_seconds}秒")

                # 立即执行
                if immediate:
                    try:
                        if asyncio.iscoroutinefunction(func):
                            await func(*args, **kwargs)
                        else:
                            result = await _execute_function_smartly(func, args, kwargs)
                            if asyncio.iscoroutine(result):
                                await result

                        logger.info(f"任务 {actual_task_id} 立即执行完成")
                    except Exception as e:
                        logger.error(f"任务 {actual_task_id} 立即执行失败: {e}")
                        if on_error:
                            try:
                                await _handle_error_smartly(on_error, e, func, args, kwargs)
                            except Exception:
                                pass

                # 定时循环执行
                while True:
                    await asyncio.sleep(total_seconds)

                    # 检查并发实例数
                    if instance_count >= max_instances:
                        logger.warning(f"任务 {actual_task_id} 超过最大并发数 {max_instances}，跳过本次执行")
                        continue

                    instance_count += 1

                    try:
                        execution_start = datetime.now()
                        logger.debug(f"执行间隔任务 {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                        if asyncio.iscoroutinefunction(func):
                            await func(*args, **kwargs)
                        else:
                            result = await _execute_function_smartly(func, args, kwargs)
                            if asyncio.iscoroutine(result):
                                await result

                        execution_end = datetime.now()
                        execution_time = (execution_end - execution_start).total_seconds()
                        logger.info(
                            f"间隔任务完成: {execution_end.strftime('%H:%M:%S')} (耗时: {execution_time:.2f}秒)")

                    except Exception as e:
                        logger.error(f"间隔任务 {actual_task_id} 执行失败: {e}")
                        if on_error:
                            try:
                                await _handle_error_smartly(on_error, e, func, args, kwargs)
                            except Exception as err_e:
                                logger.error(f"错误处理函数执行失败: {err_e}")
                    finally:
                        instance_count -= 1

            # 使用任务创建策略
            task = _create_task_smartly(interval_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"间隔任务已加入调度: {actual_task_id}, 间隔: {total_seconds}秒")
            return task

        # 添加任务控制方法
        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id

        # 注册到装饰器注册表
        registry.register_function('schedule_interval', wrapper, {
            'task_id': actual_task_id,
            'seconds': seconds or 0,
            'minutes': minutes or 0,
            'hours': hours or 0,
            'total_seconds': total_seconds,
            'immediate': immediate,
            'max_instances': max_instances
        })

        return wrapper

    return decorator


# ================ schedule_delay 装饰器 ================

def schedule_delay(seconds: int = None, minutes: int = None, hours: int = None,
                   task_id: str = None, on_error: Callable = None):

    def decorator(func: Callable):
        total_seconds = (seconds or 0) + (minutes or 0) * 60 + (hours or 0) * 3600
        if total_seconds <= 0:
            raise ValueError("必须指定正数的延迟时间")

        actual_task_id = task_id or f"delay_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            async def delay_task():
                logger.info(f"延迟任务已启动: {actual_task_id}, 延迟: {total_seconds}秒")

                await asyncio.sleep(total_seconds)

                try:
                    execution_start = datetime.now()
                    logger.debug(f"执行延迟任务 {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                    if asyncio.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        result = await _execute_function_smartly(func, args, kwargs)
                        if asyncio.iscoroutine(result):
                            result = await result

                    execution_end = datetime.now()
                    execution_time = (execution_end - execution_start).total_seconds()
                    logger.info(f"延迟任务完成: {execution_end.strftime('%H:%M:%S')} (耗时: {execution_time:.2f}秒)")
                    return result

                except Exception as e:
                    logger.error(f"延迟任务 {actual_task_id} 执行失败: {e}")
                    if on_error:
                        try:
                            await _handle_error_smartly(on_error, e, func, args, kwargs)
                        except Exception:
                            pass
                    raise

            # 使用任务创建策略
            task = _create_task_smartly(delay_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"延迟任务已加入调度: {actual_task_id}")
            return task

        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id

        # 注册到装饰器注册表
        registry.register_function('schedule_delay', wrapper, {
            'task_id': actual_task_id,
            'seconds': seconds or 0,
            'minutes': minutes or 0,
            'hours': hours or 0,
            'total_seconds': total_seconds
        })

        return wrapper

    return decorator


# ================ schedule_at 装饰器 ================

def schedule_at(hour: int, minute: int = 0, second: int = 0,
                task_id: str = None, on_error: Callable = None):

    def decorator(func: Callable):
        if not (0 <= hour <= 23) or not (0 <= minute <= 59) or not (0 <= second <= 59):
            raise ValueError("时间参数超出有效范围")

        actual_task_id = task_id or f"daily_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            async def daily_task():
                logger.info(f"每日定时任务已启动: {actual_task_id}")
                logger.debug(f"执行时间: {hour:02d}:{minute:02d}:{second:02d}")

                while True:
                    # 计算下次执行时间
                    now = datetime.now()
                    next_run = now.replace(hour=hour, minute=minute, second=second, microsecond=0)

                    # 如果今天的时间已过，则安排到明天
                    if next_run <= now:
                        next_run += timedelta(days=1)

                    # 等待到执行时间
                    wait_seconds = (next_run - now).total_seconds()
                    logger.debug(f"任务 {actual_task_id} 将在 {next_run} 执行 (等待 {wait_seconds:.1f} 秒)")

                    # 可中断的睡眠
                    sleep_interval = min(wait_seconds, 60)  # 最多睡眠60秒
                    total_slept = 0

                    while total_slept < wait_seconds:
                        await asyncio.sleep(sleep_interval)
                        total_slept += sleep_interval

                        # 检查是否被取消
                        if asyncio.current_task().cancelled():
                            logger.info(f"任务 {actual_task_id} 被取消")
                            return

                        remaining = wait_seconds - total_slept
                        sleep_interval = min(remaining, 60)

                    # 执行任务
                    try:
                        execution_start = datetime.now()
                        logger.info(f"执行每日任务 {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                        if asyncio.iscoroutinefunction(func):
                            await func(*args, **kwargs)
                        else:
                            result = await _execute_function_smartly(func, args, kwargs)
                            if asyncio.iscoroutine(result):
                                await result

                        execution_end = datetime.now()
                        execution_time = (execution_end - execution_start).total_seconds()
                        logger.info(
                            f"每日任务完成: {execution_end.strftime('%H:%M:%S')} (耗时: {execution_time:.2f}秒)")

                    except Exception as e:
                        logger.error(f"每日任务 {actual_task_id} 执行失败: {e}")
                        if on_error:
                            try:
                                await _handle_error_smartly(on_error, e, func, args, kwargs)
                            except Exception:
                                pass

            # 使用任务创建策略
            task = _create_task_smartly(daily_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"每日定时任务已加入调度: {actual_task_id}, 执行时间: {hour:02d}:{minute:02d}:{second:02d}")
            return task

        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id

        # 注册到装饰器注册表
        registry.register_function('schedule_at', wrapper, {
            'task_id': actual_task_id,
            'hour': hour,
            'minute': minute,
            'second': second,
            'time_string': f"{hour:02d}:{minute:02d}:{second:02d}"
        })

        return wrapper

    return decorator


# ================ schedule_cron 装饰器 ================

def schedule_cron(cron_expression: str, task_id: str = None,
                  on_error: Callable = None, max_instances: int = 1):

    def decorator(func: Callable):
        # 解析Cron表达式
        try:
            parser = CronParser(cron_expression)
            logger.info(f"Cron表达式解析成功: {cron_expression}")
            logger.debug(f"解析后的字段: {parser.fields}")
        except Exception as e:
            raise ValueError(f"无效的Cron表达式 '{cron_expression}': {e}")

        actual_task_id = task_id or f"cron_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            instance_count = 0

            async def cron_task():
                nonlocal instance_count

                logger.info(f"Cron任务已启动: {actual_task_id}")
                logger.debug(f"表达式: {cron_expression}")

                while True:
                    try:
                        # 计算下次执行时间
                        current_time = datetime.now()
                        next_run = parser.get_next_run_time(current_time)
                        wait_seconds = (next_run - current_time).total_seconds()

                        if wait_seconds <= 0:
                            logger.warning(f"等待时间异常: {wait_seconds}，重新计算")
                            await asyncio.sleep(1)  # 避免忙等待
                            continue

                        logger.debug(f"任务 {actual_task_id} 下次执行: {next_run} (等待 {wait_seconds:.1f}秒)")

                        # 可中断的睡眠
                        sleep_interval = min(wait_seconds, 60)  # 最多睡眠60秒
                        total_slept = 0

                        while total_slept < wait_seconds:
                            await asyncio.sleep(sleep_interval)
                            total_slept += sleep_interval

                            # 检查是否被取消
                            if asyncio.current_task().cancelled():
                                logger.info(f"任务 {actual_task_id} 被取消")
                                return

                            remaining = wait_seconds - total_slept
                            sleep_interval = min(remaining, 60)

                        # 检查并发实例数
                        if instance_count >= max_instances:
                            logger.warning(f"任务 {actual_task_id} 超过最大并发数，跳过执行")
                            continue

                        instance_count += 1

                        try:
                            # 执行任务
                            execution_start = datetime.now()
                            logger.info(f"执行Cron任务 {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                            if asyncio.iscoroutinefunction(func):
                                await func(*args, **kwargs)
                            else:
                                result = await _execute_function_smartly(func, args, kwargs)
                                if asyncio.iscoroutine(result):
                                    await result

                            execution_end = datetime.now()
                            execution_time = (execution_end - execution_start).total_seconds()
                            logger.info(
                                f"Cron任务完成: {execution_end.strftime('%H:%M:%S')} (耗时: {execution_time:.2f}秒)")

                        except Exception as e:
                            logger.error(f"Cron任务执行失败: {e}")
                            if on_error:
                                try:
                                    await _handle_error_smartly(on_error, e, func, args, kwargs)
                                except Exception as err_e:
                                    logger.error(f"错误处理失败: {err_e}")
                        finally:
                            instance_count -= 1

                    except asyncio.CancelledError:
                        logger.info(f"Cron任务 {actual_task_id} 被取消")
                        break
                    except Exception as e:
                        logger.error(f"Cron调度失败: {e}")
                        await asyncio.sleep(5)  # 出错后等待更长时间

            # 使用任务创建策略
            task = _create_task_smartly(cron_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"Cron任务已加入调度: {actual_task_id}")
            return task

        # 添加控制方法
        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id
        wrapper.cron_expression = cron_expression

        # 注册到装饰器注册表
        registry.register_function('schedule_cron', wrapper, {
            'task_id': actual_task_id,
            'cron_expression': cron_expression,
            'max_instances': max_instances
        })

        return wrapper

    return decorator


def auto_discover_and_start_tasks(package_name: str = None, handlers_package: str = "handlers",
                                  decorator_types: List[str] = None) -> Dict:
    """
    自动发现并启动装饰器任务的便捷函数

    Args:
        package_name: 主包名（如 "sfme"）
        handlers_package: handlers包名（如 "handlers"）
        decorator_types: 要启动的装饰器类型

    Returns:
        启动结果字典
    """
    if package_name:
        discovery.package_name = package_name

    # 发现装饰器函数
    logger.info("开始自动发现装饰器函数...")
    discovered = discovery.discover_handlers_package(handlers_package)

    if not discovered:
        logger.warning("未发现任何装饰器函数")
        return {'started': {}, 'failed': {}, 'total_started': 0, 'total_failed': 0}

    # 显示发现的函数
    logger.info(f"发现的装饰器函数:")
    for func_info in discovered:
        logger.info(f"  - {func_info['module']}.{func_info['name']} ({func_info['decorator_type']})")

    # 自动启动发现的函数
    logger.info("开始启动发现的装饰器函数...")
    result = discovery.auto_start_discovered_functions(decorator_types)

    logger.info(f"启动结果: 成功 {result['total_started']} 个, 失败 {result['total_failed']} 个")
    return result


def get_registry_summary() -> Dict:
    """获取注册表摘要"""
    return registry.get_summary()


def stop_all_tasks():
    """停止所有任务"""
    task_manager.cancel_all_tasks()
    logger.info("所有任务已停止")

# 全局任务管理器实例
task_manager = TaskManager()
registry = DecoratorRegistry()
discovery = DecoratorDiscovery()

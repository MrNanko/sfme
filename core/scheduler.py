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

class TaskManager:
    """Task Manager for managing asynchronous tasks and Telethon integration"""

    def __init__(self):
        self._tasks = {}
        self._running = False
        self._loop = None
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._loop_thread = None
        self._lock = threading.Lock()

        # Telethon's client and mode
        self._telethon_client = None
        self._telethon_mode = False

    # Use telethon client to enable Telethon events loop
    def enable_telethon_events_loop(self, telethon_client):
        # Check if the client is already set
        with self._lock:
            self._telethon_client = telethon_client
            self._telethon_mode = True
            logger.info("TaskManager Telethon events loop enabled")

    def get_current_loop(self):
        """
        Get the most appropriate event loop based on context

        Priority order:
        1. Current running event loop (if available)
        2. Telethon client's event loop (if Telethon mode is enabled)
        3. TaskManager's own event loop

        Returns:
            asyncio.AbstractEventLoop: The most appropriate event loop
        """
        try:
            # Try to get the current running event loop first
            current_loop = asyncio.get_running_loop()

            # If Telethon mode is enabled, verify loop compatibility
            if self._telethon_mode and self._telethon_client and self._telethon_client.loop:
                if current_loop == self._telethon_client.loop:
                    logger.debug("Using Telethon's event loop (same as current loop)")
                else:
                    logger.debug("Current loop differs from Telethon's loop, using current loop anyway")

            return current_loop

        except RuntimeError:
            # No running event loop, fall back to alternatives

            # If Telethon mode is enabled and its loop available, use that
            if self._telethon_mode and self._telethon_client and self._telethon_client.loop:
                logger.debug("No running event loop, falling back to Telethon's event loop")
                return self._telethon_client.loop

            # Last resort: use TaskManager's own event loop
            logger.debug("Using TaskManager's own event loop")
            return self._loop

    def start_background_loop(self):
        with self._lock:
            # If Telethon mode is enabled, try to use its event loop
            if self._telethon_mode and self._telethon_client:
                return self._start_with_telethon_support()

            try:
                current_loop = asyncio.get_running_loop()
                logger.info("Detected a running event loop, no need for background loop")
                self._loop = current_loop
                self._running = True
                return
            except RuntimeError:
                pass

            if self._loop_thread and self._loop_thread.is_alive():
                logger.info("Background loop already exists, no need to create again")
                return

            def run_background_loop():
                """Run the event loop in a background thread"""
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    self._loop = loop
                    self._running = True

                    logger.info("Background event loop started")
                    loop.run_forever()

                except Exception as e:
                    logger.error(f"Error running background loop: {e}")
                finally:
                    logger.info("Background event loop stopped")
                    self._running = False
                    self._loop = None

            self._loop_thread = threading.Thread(target=run_background_loop, daemon=True)
            self._loop_thread.start()

            # Wait for the loop to start
            max_wait = 50
            wait_count = 0
            while not self._running and wait_count < max_wait:
                time.sleep(0.1)
                wait_count += 1

            if not self._running:
                raise RuntimeError("Background loop startup timeout")

    def _start_with_telethon_support(self):
        try:
            if not self._telethon_client.loop:
                logger.warning("Telethon client event loop unavailable, using standard mode")
                self._telethon_mode = False
                return self.start_background_loop()

            telethon_loop = self._telethon_client.loop
            if telethon_loop.is_running():
                logger.info("Using Telethon's event loop")
                self._loop = telethon_loop
                self._running = True
            else:
                logger.warning("Telethon loop not running, falling back to standard mode")
                self._telethon_mode = False
                return self.start_background_loop()

        except Exception as e:
            logger.error(f"Failed to start with Telethon support: {e}, falling back to standard mode")
            self._telethon_mode = False
            return self.start_background_loop()

    def add_task(self, task_id: str, task):
        """Add task"""
        with self._lock:
            if task_id in self._tasks:
                logger.warning(f"Task ID {task_id} already exists, will replace old task")
                self.remove_task(task_id)

            self._tasks[task_id] = task
            logger.debug(f"Task {task_id} has been added to the manager")

    def remove_task(self, task_id: str):
        """Remove a task"""
        with self._lock:
            task = self._tasks.pop(task_id, None)
            if task:
                try:
                    # Check if the task supports cancellation and completion status (async loop tasks)
                    if hasattr(task, 'cancel') and hasattr(task, 'done'):
                        if not task.done():
                            task.cancel()
                            logger.debug(f"Canceled async loop Task: {task_id}")
                    # For Futures, check if the task supports result retrieval and is still pending
                    elif hasattr(task, 'result'):
                        if not task.done():
                            task.cancel()
                            logger.debug(f"Canceled Future task in async loop: {task_id}")
                except Exception as e:
                    logger.warning(f"Error occurred while canceling task {task_id}: {e}")

    def cancel_all_tasks(self):
        """Cancel all tasks"""
        with self._lock:
            logger.info(f"Cancelling {len(self._tasks)} tasks")

            for task_id, task in list(self._tasks.items()):
                try:
                    # Cancel async loop tasks if they are still running
                    if hasattr(task, 'cancel') and hasattr(task, 'done'):
                        if not task.done():
                            task.cancel()
                    # Cancel future tasks if still pending
                    elif hasattr(task, 'result'):
                        if not task.done():
                            task.cancel()
                except Exception as e:
                    logger.warning(f"Error occurred when cancelling task {task_id}: {e}")

            self._tasks.clear()

            # Stop own event loop only when not in Telethon mode
            if not self._telethon_mode:
                if self._loop and not self._loop.is_closed() and self._running:
                    try:
                        if self._loop.is_running():
                            self._loop.call_soon_threadsafe(self._loop.stop)
                            if self._loop_thread and self._loop_thread.is_alive():
                                self._loop_thread.join(timeout=2.0)
                    except Exception as e:
                        logger.warning(f"Error occurred when stopping the event loop: {e}")
                    finally:
                        self._running = False
            else:
                logger.debug("Telethon mode enabled; keeping event loop running")
                self._running = False

    def get_task_count(self):
        """Get the number of tasks."""
        with self._lock:
            return len(self._tasks)

    def get_task_info(self):
        """Get detailed task information."""
        with self._lock:
            task_info = {}
            for task_id, task in self._tasks.items():
                try:
                    # Check if the task has a 'done' method to determine its completion status
                    if hasattr(task, 'done'):
                        status = "completed" if task.done() else "running"
                        # Check if the completed task raised an exception
                        if task.done() and hasattr(task, 'exception'):
                            exception = task.exception()
                            if exception:
                                status = f"failed: {exception}"
                    else:
                        status = "unknown"

                    task_info[task_id] = {
                        "status": status,
                        "type": type(task).__name__,
                        "telethon_mode": self._telethon_mode
                    }
                except Exception as e:
                    # Handle errors encountered while retrieving task info
                    task_info[task_id] = {"status": f"error: {e}", "type": "unknown"}

            return task_info

    def cleanup(self):
        """Clean up completed tasks"""
        with self._lock:
            completed_tasks = []
            for task_id, task in self._tasks.items():
                try:
                    # Check if the task is an asyncio Task or a Future, and determine if it's completed.
                    if hasattr(task, 'done') and task.done():
                        completed_tasks.append(task_id)
                    elif hasattr(task, 'result'):
                        # Some Future objects use 'result' method for status checks.
                        if task.done():
                            completed_tasks.append(task_id)
                except Exception as e:
                    # If an error occurs when checking task status, log a warning and mark the task for cleanup.
                    logger.warning(f"Error occurred while checking task {task_id} status: {e}")
                    completed_tasks.append(task_id)

            # Remove identified completed tasks from the task registry.
            for task_id in completed_tasks:
                del self._tasks[task_id]
                logger.debug(f"Cleaned up completed task: {task_id}")

            # Return the count of tasks that were cleaned up.
            return len(completed_tasks)


class DecoratorRegistry:
    """Decorator Registry"""

    def __init__(self):
        # Initialize registry categories for different scheduling decorators
        self._registry = {
            'schedule_cron': [],
            'schedule_interval': [],
            'schedule_delay': [],
            'schedule_at': []
        }
        # Mapping from function names to their callable objects for quick access
        self._function_mapping = {}

    def register_function(self, decorator_type: str, func: Callable, config: Dict = None):
        """Register a decorated function"""
        # Information about the function, including its metadata and optional configuration
        func_info = {
            'name': func.__name__,
            'function': func,
            'module': func.__module__,
            'config': config or {},
        }

        # Ensure the decorator type has a dedicated list in the registry
        if decorator_type not in self._registry:
            self._registry[decorator_type] = []

        # Add function metadata to the appropriate decorator category
        self._registry[decorator_type].append(func_info)
        # Map function name to its callable for easy retrieval later
        self._function_mapping[func.__name__] = func
        logger.debug(f"Registered decorated function: {decorator_type}.{func.__name__}")

    def get_functions_by_type(self, decorator_type: str) -> List[Dict]:
        """Retrieve all functions registered under a specific decorator type"""
        return self._registry.get(decorator_type, [])

    def get_all_functions(self) -> Dict[str, List[Dict]]:
        """Retrieve all registered functions"""
        return self._registry.copy()

    def get_summary(self) -> Dict:
        """Get a summary of all registered functions"""
        summary = {}
        total_functions = 0

        # Summarize count and names of functions for each decorator type
        for decorator_type, functions in self._registry.items():
            count = len(functions)
            total_functions += count
            summary[decorator_type] = {
                'count': count,
                'functions': [f['name'] for f in functions]
            }

        # Add the total number of registered functions to the summary
        summary['total'] = total_functions
        return summary


class DecoratorDiscovery:
    """Decorator Function Auto-Discovery"""

    def __init__(self, package_name: str = None):
        self.package_name = package_name
        self.registry = registry

    def discover_handlers_package(self, handlers_package: str = "handlers"):
        """Automatically discover all decorated functions within the handlers package"""
        discovered_functions = []

        try:
            # Determine full package name for accurate importing
            if self.package_name and not handlers_package.startswith(self.package_name):
                full_package_name = f"{self.package_name}.{handlers_package}"
            else:
                full_package_name = handlers_package

            logger.info(f"Starting scan of package: {full_package_name}")

            # Attempt to import the specified package
            try:
                package = importlib.import_module(full_package_name)
            except ImportError as e:
                logger.error(f"Failed to import package {full_package_name}: {e}")
                return discovered_functions

            # Ensure the imported object is a valid package with '__path__'
            if hasattr(package, '__path__'):
                package_path = package.__path__
            else:
                logger.warning(f"Package {full_package_name} lacks a '__path__' attribute")
                return discovered_functions

            # Iterate through modules in the package
            for importer, modname, ispkg in pkgutil.iter_modules(package_path, full_package_name + "."):
                if ispkg:
                    continue  # Skip sub-packages to avoid deeper recursion

                try:
                    logger.debug(f"Scanning module: {modname}")
                    module = importlib.import_module(modname)
                    module_functions = self._scan_module_for_decorators(module)
                    discovered_functions.extend(module_functions)

                except ImportError as e:
                    logger.warning(f"Could not import module {modname}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error occurred while scanning module {modname}: {e}")
                    continue

            logger.info(f"Discovered {len(discovered_functions)} decorated functions")
            return discovered_functions

        except Exception as e:
            logger.error(f"Error occurred while scanning package {handlers_package}: {e}")
            return discovered_functions

    def _scan_module_for_decorators(self, module) -> List[Dict]:
        """Scan module for functions using decorators"""
        found_functions = []

        # Iterate over all functions in the module
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
                    logger.debug(f"Found decorated function: {module.__name__}.{name} ({decorator_info['type']})")

        return found_functions

    def _get_decorator_info(self, func) -> Optional[Dict]:
        """Retrieve decorator details from function attributes"""
        # Identify functions scheduled by cron expression
        if hasattr(func, 'task_id') and hasattr(func, 'cron_expression'):
            return {
                'type': 'schedule_cron',
                'config': {
                    'task_id': func.task_id,
                    'cron_expression': func.cron_expression
                }
            }

        # Identify interval-scheduled functions
        if hasattr(func, 'task_id') and hasattr(func, '_schedule_interval'):
            return {
                'type': 'schedule_interval',
                'config': getattr(func, '_schedule_config', {})
            }

        return None  # No recognizable decorator attributes found

    def auto_start_discovered_functions(self, decorator_types: List[str] = None) -> Dict:
        """Automatically start the discovered decorated functions"""
        if decorator_types is None:
            decorator_types = ['schedule_cron', 'schedule_interval', 'schedule_delay', 'schedule_at']

        started_tasks = {}
        failed_tasks = {}

        # Retrieve all functions from the registry
        all_functions = self.registry.get_all_functions()

        for decorator_type in decorator_types:
            if decorator_type not in all_functions:
                continue

            started_tasks[decorator_type] = []
            failed_tasks[decorator_type] = []

            functions = all_functions[decorator_type]
            logger.info(f"Starting {len(functions)} tasks of type {decorator_type}")

            for func_info in functions:
                try:
                    func = func_info['function']
                    func_name = func_info['name']

                    logger.info(f"Starting task: {func_name} ({decorator_type})")
                    task_result = func()  # Execute the function

                    started_tasks[decorator_type].append({
                        'name': func_name,
                        'module': func_info['module'],
                        'task_result': task_result
                    })

                    logger.info(f"Task started successfully: {func_name}")

                except Exception as e:
                    logger.error(f"Failed to start task: {func_info['name']}, Error: {e}")
                    failed_tasks[decorator_type].append({
                        'name': func_info['name'],
                        'module': func_info['module'],
                        'error': str(e)
                    })

        # Summarize results of task initiation
        return {
            'started': started_tasks,
            'failed': failed_tasks,
            'total_started': sum(len(tasks) for tasks in started_tasks.values()),
            'total_failed': sum(len(tasks) for tasks in failed_tasks.values())
        }


class CronParser:
    """Cron Expression Parser"""

    # Mapping of weekday names to numerical values
    WEEKDAY_NAMES = {
        'sun': 0, 'sunday': 0,
        'mon': 1, 'monday': 1,
        'tue': 2, 'tuesday': 2,
        'wed': 3, 'wednesday': 3,
        'thu': 4, 'thursday': 4,
        'fri': 5, 'friday': 5,
        'sat': 6, 'saturday': 6
    }

    # Mapping of month names to numerical values
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
        """Initialize the Cron parser."""
        self.cron_expression = cron_expression.strip()
        self.fields = self._parse_expression()

    def _parse_expression(self) -> dict:
        """Parse the Cron expression into individual time fields."""
        parts = self.cron_expression.split()

        if len(parts) == 5:
            # 5-field format: minute hour day month weekday
            minute, hour, day, month, weekday = parts
            second = "0"  # Default second to 0
        elif len(parts) == 6:
            # 6-field format: second minute hour day month weekday
            second, minute, hour, day, month, weekday = parts
        else:
            raise ValueError(f"Invalid Cron expression: {self.cron_expression}")

        # Parse individual fields, handling wildcards, ranges, and names
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
        """Parse individual field of a Cron expression."""
        if field == '*':
            return set(range(min_val, max_val + 1))

        values = set()

        # Process comma-separated values
        for part in field.split(','):
            part = part.strip()

            # Handle named fields (e.g., Jan, Mon)
            if name_mapping:
                for name, value in name_mapping.items():
                    part = re.sub(rf'\b{name}\b', str(value), part, flags=re.IGNORECASE)

            # Handle steps (e.g., */5)
            if '/' in part:
                range_part, step = part.split('/', 1)
                step = int(step)
            else:
                range_part = part
                step = 1

            # Handle ranges (e.g., 1-5)
            if '-' in range_part:
                start, end = range_part.split('-', 1)
                start = int(start)
                end = int(end)
            elif range_part == '*':
                start = min_val
                end = max_val
            else:
                start = end = int(range_part)

            # Validate ranges
            if start < min_val or end > max_val or start > end:
                raise ValueError(f"Field value out of range: {part}")

            # Add computed range values to set
            values.update(range(start, end + 1, step))

        return values

    def get_next_run_time(self, current_time: datetime = None) -> datetime:
        """Calculate the next run time for the Cron expression (fixed version)."""
        if current_time is None:
            current_time = datetime.now()

        # Crucial fix: Always start searching from the next second
        next_time = current_time.replace(microsecond=0) + timedelta(seconds=1)

        # Prevent infinite loops by limiting search to 4 years (more precisely, in seconds)
        max_attempts = 4 * 365 * 24 * 60 * 60
        attempts = 0

        while attempts < max_attempts:
            if self._matches_time(next_time):
                return next_time

            # Increment time by one second for next check
            next_time += timedelta(seconds=1)
            attempts += 1

        raise ValueError(f"Could not find matching execution time: {self.cron_expression}")

    def _matches_time(self, dt: datetime) -> bool:
        """Check if a datetime matches the Cron expression."""
        # Check fundamental time components first
        if (dt.second not in self.fields['second'] or
                dt.minute not in self.fields['minute'] or
                dt.hour not in self.fields['hour'] or
                dt.month not in self.fields['month']):
            return False

        # Evaluate day of month and weekday fields
        day_match = dt.day in self.fields['day']
        weekday_match = dt.weekday() in self._convert_weekday(self.fields['weekday'])

        # Determine wildcard status for day and weekday fields
        day_is_wildcard = self.fields['day'] == set(range(1, 32))
        weekday_is_wildcard = self.fields['weekday'] == set(range(0, 7))

        # Decide match logic based on wildcard status
        if day_is_wildcard and weekday_is_wildcard:
            return True  # Both fields are wildcards
        elif day_is_wildcard:
            return weekday_match
        elif weekday_is_wildcard:
            return day_match
        else:
            # Cron standard: match if either field matches when both are restricted
            return day_match or weekday_match

    def _convert_weekday(self, cron_weekdays: set) -> set:
        """
        Convert Cron weekday numbering to Python's:
        Cron: 0=Sunday, 1=Monday, ..., 6=Saturday
        Python: 0=Monday, 1=Tuesday, ..., 6=Sunday
        """
        python_weekdays = set()
        for cron_day in cron_weekdays:
            if cron_day == 0:  # Sunday in Cron equals 6 in Python
                python_weekdays.add(6)
            else:
                python_weekdays.add(cron_day - 1)  # Adjust all other days
        return python_weekdays


# ================ Decorator Helper Functions ================

def _create_task(coro, task_id: str):
    """Task creation - prioritize Telethon event loop"""
    try:
        # Strategy 1: If Telethon mode is enabled, prefer using the Telethon event loop
        if task_manager._telethon_mode and task_manager._telethon_client:
            telethon_loop = task_manager._telethon_client.loop
            if telethon_loop and not telethon_loop.is_closed():
                try:
                    # Check if currently inside an event loop
                    current_loop = asyncio.get_running_loop()
                    if current_loop == telethon_loop:
                        # Directly create the task in the Telethon event loop
                        logger.debug(f"Creating task in Telethon event loop: {task_id}")
                        task = current_loop.create_task(coro)
                        task_manager._loop = current_loop
                        task_manager._running = True
                        return task
                    else:
                        # Cross-loop scheduling: submit the coroutine to Telethon loop
                        logger.debug(f"Scheduling task to Telethon loop from another loop: {task_id}")
                        future = asyncio.run_coroutine_threadsafe(coro, telethon_loop)
                        return future
                except RuntimeError:
                    # No current event loop, directly schedule to Telethon loop
                    logger.debug(f"Scheduling task to Telethon loop (no current loop): {task_id}")
                    future = asyncio.run_coroutine_threadsafe(coro, telethon_loop)
                    return future

        # Strategy 2: Try to use the currently running event loop
        try:
            current_loop = asyncio.get_running_loop()
            logger.debug(f"Creating task in current event loop: {task_id}")
            task = current_loop.create_task(coro)
            task_manager._loop = current_loop
            task_manager._running = True
            return task
        except RuntimeError:
            # No running event loop found in this context
            pass

        # Strategy 3: Use or create a background event loop
        if task_manager._running and task_manager._loop and not task_manager._loop.is_closed():
            logger.debug(f"Creating task in existing background event loop: {task_id}")
            future = asyncio.run_coroutine_threadsafe(coro, task_manager._loop)
            return future
        else:
            logger.info(f"Starting background event loop for task: {task_id}")
            task_manager.start_background_loop()
            future = asyncio.run_coroutine_threadsafe(coro, task_manager._loop)
            return future

    except Exception as e:
        # Log and raise if task creation fails
        logger.error(f"Failed to create task {task_id}: {e}")
        raise

def _execute_function(func, args, kwargs):
    """Execute function – handles synchronous or asynchronous functions"""
    # Check if the provided function is asynchronous (coroutine-based)
    if asyncio.iscoroutinefunction(func):
        # Directly return coroutine object for asynchronous execution
        return func(*args, **kwargs)
    else:
        # Obtain the current event loop to execute synchronous functions asynchronously
        try:
            loop = asyncio.get_running_loop()
            # Execute synchronous function in separate thread using executor
            return loop.run_in_executor(task_manager._executor, lambda: func(*args, **kwargs))
        except RuntimeError:
            # If no event loop is running, execute the synchronous function directly
            return func(*args, **kwargs)

# ================ Error Handling Helper Functions ================

async def _handle_error(on_error: Callable, error: Exception, func: Callable, args, kwargs):
    """Smart error handling"""
    # Check if the provided error handler is an asynchronous function
    if asyncio.iscoroutinefunction(on_error):
        # Directly await the coroutine-based error handler
        await on_error(error, func, args, kwargs)
    else:
        try:
            # Attempt to retrieve the current running event loop
            loop = asyncio.get_running_loop()
            # Run synchronous error handler in a separate thread using the executor
            await loop.run_in_executor(
                task_manager._executor,
                lambda: on_error(error, func, args, kwargs)
            )
        except RuntimeError:
            # No event loop running; directly call the synchronous error handler
            on_error(error, func, args, kwargs)

# ================ schedule_interval Decorator ================

def schedule_interval(seconds: int = None, minutes: int = None, hours: int = None,
                      immediate: bool = False, task_id: str = None,
                      max_instances: int = 1, on_error: Callable = None):

    def decorator(func: Callable):
        # Calculate the total interval time (in seconds)
        total_seconds = (seconds or 0) + (minutes or 0) * 60 + (hours or 0) * 3600
        if total_seconds <= 0:
            raise ValueError("A positive interval time must be specified")

        actual_task_id = task_id or f"interval_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            instance_count = 0

            async def interval_task():
                nonlocal instance_count

                logger.info(f"Interval task started: {actual_task_id}")
                logger.debug(f"Interval: {total_seconds} seconds")

                # Execute immediately if specified
                if immediate:
                    try:
                        if asyncio.iscoroutinefunction(func):
                            await func(*args, **kwargs)
                        else:
                            result = await _execute_function(func, args, kwargs)
                            if asyncio.iscoroutine(result):
                                await result

                        logger.info(f"Task {actual_task_id} executed immediately")
                    except Exception as e:
                        logger.error(f"Task {actual_task_id} immediate execution failed: {e}")
                        if on_error:
                            try:
                                await _handle_error(on_error, e, func, args, kwargs)
                            except Exception:
                                pass

                # Loop for scheduled interval execution
                while True:
                    await asyncio.sleep(total_seconds)

                    # Check the number of concurrent instances
                    if instance_count >= max_instances:
                        logger.warning(f"Task {actual_task_id} exceeds max instances {max_instances}, skipping execution")
                        continue

                    instance_count += 1

                    try:
                        execution_start = datetime.now()
                        logger.debug(f"Executing interval task {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                        if asyncio.iscoroutinefunction(func):
                            await func(*args, **kwargs)
                        else:
                            result = await _execute_function(func, args, kwargs)
                            if asyncio.iscoroutine(result):
                                await result

                        execution_end = datetime.now()
                        execution_time = (execution_end - execution_start).total_seconds()
                        logger.info(
                            f"Interval task completed: {execution_end.strftime('%H:%M:%S')} (Duration: {execution_time:.2f}s)")

                    except Exception as e:
                        logger.error(f"Interval task {actual_task_id} execution failed: {e}")
                        if on_error:
                            try:
                                await _handle_error(on_error, e, func, args, kwargs)
                            except Exception as err_e:
                                logger.error(f"Error handler execution failed: {err_e}")
                    finally:
                        instance_count -= 1

            # Use the smart task creation strategy
            task = _create_task(interval_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"Interval task added to scheduler: {actual_task_id}, interval: {total_seconds} seconds")
            return task

        # Add task control method
        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id

        # Register in the decorator registry
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


# ================ schedule_delay Decorator ================

def schedule_delay(seconds: int = None, minutes: int = None, hours: int = None,
                   task_id: str = None, on_error: Callable = None):

    def decorator(func: Callable):
        # Calculate the total delay time in seconds
        total_seconds = (seconds or 0) + (minutes or 0) * 60 + (hours or 0) * 3600
        if total_seconds <= 0:
            raise ValueError("A positive delay time must be specified")

        actual_task_id = task_id or f"delay_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            async def delay_task():
                logger.info(f"Delay task started: {actual_task_id}, delay: {total_seconds} seconds")

                await asyncio.sleep(total_seconds)

                try:
                    execution_start = datetime.now()
                    logger.debug(f"Executing delay task {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                    if asyncio.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        result = await _execute_function(func, args, kwargs)
                        if asyncio.iscoroutine(result):
                            result = await result

                    execution_end = datetime.now()
                    execution_time = (execution_end - execution_start).total_seconds()
                    logger.info(f"Delay task completed: {execution_end.strftime('%H:%M:%S')} (Duration: {execution_time:.2f}s)")
                    return result

                except Exception as e:
                    logger.error(f"Delay task {actual_task_id} execution failed: {e}")
                    if on_error:
                        try:
                            await _handle_error(on_error, e, func, args, kwargs)
                        except Exception:
                            pass
                    raise

            # Use smart task creation strategy
            task = _create_task(delay_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"Delay task added to scheduler: {actual_task_id}")
            return task

        # Add stop control and task_id attributes to the wrapper
        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id

        # Register in the decorator registry
        registry.register_function('schedule_delay', wrapper, {
            'task_id': actual_task_id,
            'seconds': seconds or 0,
            'minutes': minutes or 0,
            'hours': hours or 0,
            'total_seconds': total_seconds
        })

        return wrapper

    return decorator


# ================ schedule_at Decorator ================

def schedule_at(hour: int, minute: int = 0, second: int = 0,
                task_id: str = None, on_error: Callable = None):

    def decorator(func: Callable):
        # Validate the time parameters
        if not (0 <= hour <= 23) or not (0 <= minute <= 59) or not (0 <= second <= 59):
            raise ValueError("Time parameters are out of valid range")

        actual_task_id = task_id or f"daily_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            async def daily_task():
                logger.info(f"Daily scheduled task started: {actual_task_id}")
                logger.debug(f"Scheduled execution time: {hour:02d}:{minute:02d}:{second:02d}")

                while True:
                    # Calculate the next execution time
                    now = datetime.now()
                    next_run = now.replace(hour=hour, minute=minute, second=second, microsecond=0)

                    # If the scheduled time for today has passed, schedule for tomorrow
                    if next_run <= now:
                        next_run += timedelta(days=1)

                    # Wait until the execution time
                    wait_seconds = (next_run - now).total_seconds()
                    logger.debug(f"Task {actual_task_id} will run at {next_run} (waiting {wait_seconds:.1f} seconds)")

                    # Interruptible sleep: sleep in up to 60-second intervals to allow for cancellation checks
                    sleep_interval = min(wait_seconds, 60)  # Maximum 60 seconds per sleep
                    total_slept = 0

                    while total_slept < wait_seconds:
                        await asyncio.sleep(sleep_interval)
                        total_slept += sleep_interval

                        # Check if the task has been cancelled
                        if asyncio.current_task().cancelled():
                            logger.info(f"Task {actual_task_id} was cancelled")
                            return

                        remaining = wait_seconds - total_slept
                        sleep_interval = min(remaining, 60)

                    # Execute the task at the scheduled time
                    try:
                        execution_start = datetime.now()
                        logger.info(f"Executing daily task {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                        if asyncio.iscoroutinefunction(func):
                            await func(*args, **kwargs)
                        else:
                            result = await _execute_function(func, args, kwargs)
                            if asyncio.iscoroutine(result):
                                await result

                        execution_end = datetime.now()
                        execution_time = (execution_end - execution_start).total_seconds()
                        logger.info(
                            f"Daily task completed: {execution_end.strftime('%H:%M:%S')} (Duration: {execution_time:.2f}s)")

                    except Exception as e:
                        logger.error(f"Daily task {actual_task_id} execution failed: {e}")
                        if on_error:
                            try:
                                await _handle_error(on_error, e, func, args, kwargs)
                            except Exception:
                                pass

            # Use the smart task creation strategy
            task = _create_task(daily_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"Daily scheduled task added: {actual_task_id}, execution time: {hour:02d}:{minute:02d}:{second:02d}")
            return task

        # Add stop control and task_id attributes to the wrapper
        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id

        # Register in the decorator registry
        registry.register_function('schedule_at', wrapper, {
            'task_id': actual_task_id,
            'hour': hour,
            'minute': minute,
            'second': second,
            'time_string': f"{hour:02d}:{minute:02d}:{second:02d}"
        })

        return wrapper

    return decorator


# ================ schedule_cron Decorator ================

def schedule_cron(cron_expression: str, task_id: str = None,
                  on_error: Callable = None, max_instances: int = 1):

    def decorator(func: Callable):
        # Parse the Cron expression
        try:
            parser = CronParser(cron_expression)
            logger.info(f"Cron expression parsed successfully: {cron_expression}")
            logger.debug(f"Parsed fields: {parser.fields}")
        except Exception as e:
            raise ValueError(f"Invalid Cron expression '{cron_expression}': {e}")

        actual_task_id = task_id or f"cron_{func.__name__}_{id(func)}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            instance_count = 0

            async def cron_task():
                nonlocal instance_count

                logger.info(f"Cron task started: {actual_task_id}")
                logger.debug(f"Expression: {cron_expression}")

                while True:
                    try:
                        # Calculate next run time
                        current_time = datetime.now()
                        next_run = parser.get_next_run_time(current_time)
                        wait_seconds = (next_run - current_time).total_seconds()

                        if wait_seconds <= 0:
                            logger.warning(f"Abnormal wait time: {wait_seconds}, recalculating")
                            await asyncio.sleep(1)  # Avoid busy waiting
                            continue

                        logger.debug(f"Task {actual_task_id} will next run at: {next_run} (waiting {wait_seconds:.1f} seconds)")

                        # Interruptible sleep
                        sleep_interval = min(wait_seconds, 60)  # Sleep up to 60 seconds at a time
                        total_slept = 0

                        while total_slept < wait_seconds:
                            await asyncio.sleep(sleep_interval)
                            total_slept += sleep_interval

                            # Check if task has been cancelled
                            if asyncio.current_task().cancelled():
                                logger.info(f"Task {actual_task_id} was cancelled")
                                return

                            remaining = wait_seconds - total_slept
                            sleep_interval = min(remaining, 60)

                        # Check concurrent instance limit
                        if instance_count >= max_instances:
                            logger.warning(f"Task {actual_task_id} exceeds max instances, skipping execution")
                            continue

                        instance_count += 1

                        try:
                            # Execute the scheduled task
                            execution_start = datetime.now()
                            logger.info(f"Executing Cron task {actual_task_id}: {execution_start.strftime('%H:%M:%S')}")

                            if asyncio.iscoroutinefunction(func):
                                await func(*args, **kwargs)
                            else:
                                result = await _execute_function(func, args, kwargs)
                                if asyncio.iscoroutine(result):
                                    await result

                            execution_end = datetime.now()
                            execution_time = (execution_end - execution_start).total_seconds()
                            logger.info(
                                f"Cron task completed: {execution_end.strftime('%H:%M:%S')} (Duration: {execution_time:.2f}s)")

                        except Exception as e:
                            logger.error(f"Cron task execution failed: {e}")
                            if on_error:
                                try:
                                    await _handle_error(on_error, e, func, args, kwargs)
                                except Exception as err_e:
                                    logger.error(f"Error handler execution failed: {err_e}")
                        finally:
                            instance_count -= 1

                    except asyncio.CancelledError:
                        logger.info(f"Cron task {actual_task_id} was cancelled")
                        break
                    except Exception as e:
                        logger.error(f"Cron scheduling failed: {e}")
                        await asyncio.sleep(5)  # Wait longer after error

            # Use the smart task creation strategy
            task = _create_task(cron_task(), actual_task_id)
            task_manager.add_task(actual_task_id, task)

            logger.info(f"Cron task added to scheduler: {actual_task_id}")
            return task

        # Add control methods
        wrapper.stop = lambda: task_manager.remove_task(actual_task_id)
        wrapper.task_id = actual_task_id
        wrapper.cron_expression = cron_expression

        # Register to the decorator registry
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
    Convenience function for auto-discovering and starting decorator tasks

    Args:
        package_name: Main package name (e.g. "sfme")
        handlers_package: Handlers package name (e.g. "handlers")
        decorator_types: Types of decorators to start

    Returns:
        Dictionary of start results
    """
    if package_name:
        discovery.package_name = package_name

    # Discover decorator functions
    logger.info("Starting auto-discovery of decorator functions...")
    discovered = discovery.discover_handlers_package(handlers_package)

    if not discovered:
        logger.warning("No decorator functions discovered")
        return {'started': {}, 'failed': {}, 'total_started': 0, 'total_failed': 0}

    # Show discovered functions
    logger.info("Discovered decorator functions:")
    for func_info in discovered:
        logger.info(f"  - {func_info['module']}.{func_info['name']} ({func_info['decorator_type']})")

    # Auto start discovered functions
    logger.info("Starting discovered decorator functions...")
    result = discovery.auto_start_discovered_functions(decorator_types)

    logger.info(f"Start results: {result['total_started']} succeeded, {result['total_failed']} failed")
    return result


def get_registry_summary() -> Dict:
    """Get registry summary"""
    return registry.get_summary()


def stop_all_tasks():
    """Stop all tasks"""
    task_manager.cancel_all_tasks()
    logger.info("All tasks have been stopped")

# Global task manager instances
task_manager = TaskManager()
registry = DecoratorRegistry()
discovery = DecoratorDiscovery()

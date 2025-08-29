from __future__ import annotations
import logging
from time import sleep
import copy
import uuid
from ee import batch as ee_batch
from pathlib import Path
import prettytable
from typing import Literal
from collections.abc import Iterator
from observatorio_ipa.core.config import LOGGER_NAME

# Google Drive Issue
# https://community.latenode.com/t/getting-storage-quota-exceeded-error-403-with-google-drive-api-service-account/32433


logger = logging.getLogger(LOGGER_NAME)

VALID_EXPORT_TARGETS = ["gee", "gdrive", "storage"]

# this is export_status:[task_status]
# export_status is a high-level status for the overall export procedure
# task_status is the status of the GEE task
GEE_TASK_STATUS = {
    "EXCLUDED": [
        "EXCLUDED",
        "MOCK_CREATED",
        "MOCK_TASK_SKIPPED",
        "ALREADY_EXISTS",
        "NO_TASK_CREATED",
    ],
    "NOT_STARTED": ["PLANNED", "CREATED", "UNSUBMITTED"],
    "PENDING": ["SUBMITTED", "READY", "RUNNING"],
    "COMPLETED": ["COMPLETED", "CANCELED", "CANCEL_REQUESTED"],
    "FAILED": ["FAILED", "FAILED_TO_CREATE", "FAILED_TO_START"],
    "UNKNOWN": ["FAILED_TO_GET_STATUS", "UNKNOWN"],
}

GEE_TASK_VALID_STATUS = [
    status for statuses in GEE_TASK_STATUS.values() for status in statuses
]

# Shortcuts for groups of statuses
GEE_TASK_SKIP_STATUS = [
    s
    for k, statuses in GEE_TASK_STATUS.items()
    if k in ["EXCLUDED", "COMPLETED", "FAILED", "UNKNOWN"]
    for s in statuses
]

GEE_TASK_UNFINISHED_STATUS = GEE_TASK_STATUS["PENDING"]
GEE_TASK_FINISHED_STATUS = GEE_TASK_STATUS["COMPLETED"] + GEE_TASK_STATUS["FAILED"]

GEE_EXPORT_VALID_STATUS = [status for status in GEE_TASK_STATUS.keys()]
MAX_STATUS_UPDATE_FAILURES = 3


# TODO: Make some attributes like Name immutable
class ExportTask:
    f"""
    Represents an export task for Google Earth Engine (GEE) resources.
    The ExportTask class manages the lifecycle and status of an export operation,
    such as exporting images or tables to GEE assets or Google Drive. It provides
    methods to start the export, query its status, and handle errors during the
    export process.

    It also provides separate export_status and task_status where export_status is a
    super set of task_status.

    Attributes:
        type (Literal["image", "table"]): The type of export (image or table).
        name (str): The name of exported asset at the target.
        target (str): The export destination ({VALID_EXPORT_TARGETS}).
        path (str | Path): The path to the asset to be exported.
        storage_bucket (str | None): The bucket name for Google Cloud Storage exports.
        task (ee_batch.Task | None): The underlying Earth Engine batch task.
        status (str): The current status of the export task. superset of task_status.
        task_status (str): The current status of the GEE task per last query.
        error (str | None): Error message if the task fails.
        id (str): Unique identifier for the export task, generated if not provided  (e.g. uuid4).

    Methods:
        start_task() -> str:
            Starts the export task if it has not been started yet.
            Returns the current status after attempting to start.
        query_status() -> str:
            Queries and updates the status of the export task.
            Handles errors and updates the failure count.
            Returns the current status.
    """

    name: str
    _type: Literal["image", "table"]
    _target: str
    _task_status: str
    _export_status: str  # aka status
    _status_update_failures: int

    def __init__(
        self,
        type: Literal["image", "table"],
        name: str,
        target: str,
        path: str | Path,
        storage_bucket: str | None = None,
        task: ee_batch.Task | None = None,
        task_status: str | None = None,
        error: str | None = None,
        id: str | None = None,
    ) -> None:
        self.id = id or str(uuid.uuid4())
        self.type = type
        self.name = name
        self.target = target
        self.path = Path(path)
        self.storage_bucket = storage_bucket
        self._status_update_failures = 0
        self.task = task
        if not task_status:
            self.query_status()
        else:
            self.task_status = task_status
            self.error = error

    @property
    def type(self) -> Literal["image", "table"]:
        return self._type

    @type.setter
    def type(self, value: Literal["image", "table"]) -> None:
        if value not in ["image", "table"]:
            raise ValueError(f"Can't create ExportTask, invalid type: {value}.")
        self._type = value

    @property
    def target(self) -> str:
        return self._target

    @target.setter
    def target(self, value: str) -> None:
        if value not in VALID_EXPORT_TARGETS:
            raise ValueError(f"Can't create ExportTask, invalid target: {value}.")
        self._target = value

    @property
    def task_status(self) -> str:
        return self._task_status

    @task_status.setter
    def task_status(self, value) -> None:
        value = value.upper()
        if value not in GEE_TASK_VALID_STATUS:
            raise ValueError(f"Invalid task status: {value}.")
        self._task_status = value
        self.status = value  # call status setter

    @property
    def status(self) -> str:
        return self._export_status

    @status.setter
    def status(self, value: str) -> None:
        value = value.upper()
        for key, statuses in GEE_TASK_STATUS.items():
            if value in statuses:
                export_status = key
                break
        else:
            raise ValueError(f"Unknown export status: {value}")

        self._export_status = export_status

    def start_task(self) -> str:
        """
        Start the export task.
        """
        if self.task is None:
            logger.warning(f"Skipping {self.name} to {self.target} with no Task.")
            if (
                not self.task_status
                or self.task_status in GEE_TASK_STATUS["NOT_STARTED"]
            ):
                self.task_status = "NO_TASK_CREATED"
            return self.task_status

        try:
            if self.task_status in GEE_TASK_STATUS["NOT_STARTED"]:
                self.task.start()
                self.task_status = "SUBMITTED"
        except Exception as e:
            self.task_status = "FAILED_TO_START"
            self.error = str(e)
            logger.error(f"Failed to start task: {self.name} to {self.target}")
            logger.error(e)
        return self.task_status

    def query_status(self) -> str:

        # Skip if no task to track
        if self.task is None:
            if not getattr(self, "_task_status", None):
                self.task_status = "NO_TASK_CREATED"
            return self.task_status
        else:
            if not getattr(self, "_task_status", None):
                self.task_status = "CREATED"

        # if multiple status check fail, change status and stop checking
        if (
            self._status_update_failures >= MAX_STATUS_UPDATE_FAILURES
            and self.task_status != "FAILED_TO_GET_STATUS"
        ):
            logger.error(f"Task {self.name} to {self.target} failed to get status.")
            self.task_status = "FAILED_TO_GET_STATUS"
            return self.task_status

        try:
            if (
                self.task_status
                in GEE_TASK_STATUS["PENDING"] + GEE_TASK_STATUS["NOT_STARTED"]
            ):
                status = self.task.status()
                self.task_status = status["state"]
                self._status_update_failures = 0
                self.error = status.get("error_message", None)

        except Exception as e:
            self._status_update_failures += 1
            self.error = str(e)
            logger.error(e)
        finally:
            return self.task_status

    def __repr__(self) -> str:
        return f"ExportTask(type={self.type}, name={self.name}, target={self.target}, status={self.status}, task_status={self.task_status})"

    def __str__(self) -> str:
        return f"(type={self.type}, name={self.name}, target={self.target}, status={self.status}, task_status={self.task_status})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExportTask):
            return NotImplemented
        return (
            self.type == other.type
            and self.name == other.name
            and self.target == other.target
            and self.path == other.path
            #   and self.id == other.id
        )

    def __hash__(self) -> int:
        # Optional: implement if you want ExportTask to be usable in sets or as dict keys
        return hash((self.type, self.name, self.target, self.path))


# TODO: Convert Task List to a Set to assure no duplicates
class ExportTaskList:
    """
    A class to manage a list of export tasks.
    """

    def __init__(self, tasks: list[ExportTask] = []) -> None:
        self._tasks: list[ExportTask] = []
        if tasks:
            self.extend(tasks)

    def _validate(self, task: ExportTask) -> None:
        if not isinstance(task, ExportTask):
            raise TypeError("Only ExportTask instances are allowed.")

    def append(self, task: ExportTask) -> None:
        """Append a single ExportTask to the export tasks list."""
        self._validate(task)
        self._tasks.append(copy.deepcopy(task))

    def extend(self, tasks: list[ExportTask] | ExportTaskList) -> None:
        """
        Extend the export tasks list with multiple ExportTask instances or another ExportList's _tasks.
        """
        if isinstance(tasks, ExportTaskList):
            for task in tasks._tasks:
                self.append(task)
        else:
            for task in tasks:
                self.append(task)

    def clear(self):
        """Remove all tasks from the list."""
        self._tasks.clear()

    # TODO: add option to count just by type or target
    def count(
        self, name: str, type: str | None = None, target: str | None = None
    ) -> int:
        """
        Count the number of tasks with a specific name, type, and target.
        """

        _sum = 0
        for task in self._tasks:
            if (
                task.name == name
                and (type is None or task.type == type)
                and (target is None or task.target == target)
            ):
                _sum += 1

        return _sum

    def remove(self, name: str, type: str | None = None, target: str | None = None):
        """
        Remove tasks with a specific name, type, and target from the export tasks list.
        """
        self._tasks = [
            task
            for task in self._tasks
            if (
                task.name != name
                or (type is not None and task.type != type)
                or (target is not None and task.target != target)
            )
        ]

    def add_task(
        self,
        type: Literal["image", "table"],
        name: str,
        target: str,
        path: str | Path,
        storage_bucket: str | None = None,
        task: ee_batch.Task | None = None,
        task_status: str | None = None,
        error: str | None = None,
        id: str | None = None,
    ) -> None:
        f"""
        Creates and adds a new export task to the export tasks list.

        Args:
            type (Literal["image", "table"]): The type of export (image or table).
            name (str): The name of exported asset at the target.
            target (str): The export destination ({VALID_EXPORT_TARGETS}).
            path (str | Path ): The path to the asset to be exported.
            storage_bucket (str | None): The bucket name for Google Cloud Storage exports.
            task (ee_batch.Task | None): The underlying Earth Engine batch task.
            task_status (str | None): A status for the GEE task.
            error (str | None): The error message if the export task failed.
            id (str | None): Unique identifier for the export task, generated if not provided (e.g. uuid4).

        """
        self._tasks.append(
            ExportTask(
                type=type,
                name=name,
                target=target,
                path=path,
                storage_bucket=storage_bucket,
                task=task,
                task_status=task_status,
                error=error,
                id=id,
            )
        )

    def summary(self, target: str | None = None) -> dict[str, int]:
        f"""Count the number of tasks in each status.
        Args:
            filter (str): Filter the tasks by target. Can be {VALID_EXPORT_TARGETS}.
                If None, all tasks are included.

        returns:
            dict: A dictionary with the count of tasks in each status.
        """
        target = target.lower() if target else None
        filter_target = VALID_EXPORT_TARGETS
        if target is None:
            _filter = filter_target
        elif target in filter_target:
            _filter = [target]
        else:
            raise ValueError(
                f"Invalid filter: {target}. Must be one of {filter_target}."
            )

        status_list = [t.status for t in self._tasks if t.target in _filter]

        status_dict: dict[str, int] = {}
        for status in set(status_list):
            status_dict[status] = len([s for s in status_list if s == status])

        return status_dict

    def pretty_summary(self, target: str | None = None) -> str:
        f"""Count the number of tasks in each status and returns values in a pretty table.

        Args:
            filter (str): Filter the tasks by target. Can be {VALID_EXPORT_TARGETS}.
                If None, all tasks are included.

        Returns:
            str: A string representation of the pretty table.
        """

        status_dict = self.summary(target=target)

        if not list(status_dict.keys()):
            return "No Export tasks"

        # Create table
        table = prettytable.PrettyTable()
        table.set_style(prettytable.TableStyle.MSWORD_FRIENDLY)
        table.field_names = ["Status", "Count"]
        table.align["Status"] = "l"

        # Add rows
        rows = [[k, v] for k, v in status_dict.items()]
        table.add_rows(rows)
        return table.get_string()

    def start_exports(self) -> dict[str, int]:
        """
        Start all export tasks.

        Process will skip all tasks that are not dictionaries or do not have the required keys.
        required keys: ["task", "image", "target"]

        Returns:
            dict: Summary of export tasks with their Export status.
        """
        logger.debug("Starting export tasks...")

        ####### START TASKS #######
        skipped_tasks = 0
        for i, task in enumerate(self._tasks):

            # Skip tasks with "bad" status or mock tasks
            current_status = task.task_status
            if current_status in GEE_TASK_STATUS["NOT_STARTED"]:
                task.start_task()
            else:
                skipped_tasks += 1
                logger.info(
                    f"Skipping task: {task.target} - {task.name} with status {current_status}"
                )

        logger.info(
            f"Started {len(self._tasks) - skipped_tasks} export tasks. Skipped {skipped_tasks} tasks."
        )
        # print(
        #     f"Started {len(self._tasks) - skipped_tasks} export tasks. Skipped {skipped_tasks} tasks."
        # )
        return self.summary()

    def query_status(self) -> dict[str, int]:
        """
        Track export tasks querying status at specified time intervals.

        Args:
            sleep_time (int): Time in seconds to sleep between checking task status.

        Returns:
            dict: Summary of export tasks with their statuses.

        raises:
            TypeError: If export_tasks is not a list of ExportTask objects.
        """

        logger.debug("Querying status of export tasks...")

        for i, task in enumerate(self._tasks):

            status = task.query_status()

            if status in GEE_TASK_FINISHED_STATUS:
                logger.info(
                    f"Task {task.name} to {task.target} finished with status: {status}"
                )

        return self.summary()

    def track_exports(self, sleep_time: int = 60) -> dict[str, int]:
        """
        Track export tasks querying status at specified time intervals.

        Args:
            sleep_time (int): Time in seconds to sleep between checking task status.

        Returns:
            dict: Summary of export tasks with their statuses.

        raises:
            TypeError: If export_tasks is not a list of ExportTask objects.
        """

        logger.debug("Tracking export tasks...")

        finished_tasks = []
        continue_tracking = True
        while continue_tracking:
            continue_tracking = False
            for i, task in enumerate(self._tasks):
                # Skip previously "finished" tasks to avoid logging multiple times
                if i not in finished_tasks:
                    if task.task_status not in GEE_TASK_UNFINISHED_STATUS:
                        finished_tasks.append(i)
                        continue

                    status = task.query_status()
                    if status in GEE_TASK_UNFINISHED_STATUS:
                        continue_tracking = True
                        continue

                    elif status in GEE_TASK_FINISHED_STATUS:
                        logger.info(
                            f"Task {task.name} to {task.target} finished with status: {status}"
                        )
                        finished_tasks.append(i)
                    else:
                        logger.warning(
                            f"Task {task.name} to {task.target} finished with unknown status: {status}"
                        )
                        finished_tasks.append(i)

            if continue_tracking:
                sleep(sleep_time)

        return self.summary()

    def __str__(self) -> str:

        summary = "\n".join([str(task) for task in self._tasks])
        return f"{summary}"

    def __repr__(self) -> str:
        # TODO: Find a better way to __repr__ instead of printing all items
        return f"ExportList(export_tasks={self._tasks})"

    def __getitem__(self, index: int) -> ExportTask:
        return self._tasks[index]

    def __setitem__(self, index: int, value: ExportTask) -> None:
        self._validate(value)
        self._tasks[index] = value

    def __delitem__(self, index) -> None:
        del self._tasks[index]

    def __len__(self) -> int:
        return len(self._tasks)

    def __iter__(self) -> Iterator[ExportTask]:
        return iter(self._tasks)

    def __add__(self, other: ExportTaskList) -> ExportTaskList:
        if not isinstance(other, ExportTaskList):
            return NotImplemented
        return ExportTaskList(self._tasks + other._tasks)

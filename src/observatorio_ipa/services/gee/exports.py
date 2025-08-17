import ee
import logging
from time import sleep
import copy

logger = logging.getLogger(__name__)

GEE_TASK_FINISHED_STATUS = ["COMPLETED", "FAILED", "CANCELLED", "UNSUBMITTED"]
GEE_TASK_UNFINISHED_STATUS = ["SUBMITTED", "READY", "RUNNING"]
SKIP_TASK_STATUS = [
    "FAILED_TO_CREATE",
    "FAILED_TO_START",
    "MOCK_CREATED",
    "MOCK_TASK_SKIPPED",
]


def track_exports(export_tasks: list, sleep_time: int = 60):
    """
    Start and track export tasks in the export_tasks list.

    Process will skip all tasks that are not dictionaries or do not have the required keys.
    required keys: ["task", "image", "target"]

    Args:
        export_tasks (list): List of dictionaries containing the export tasks.
        sleep_time (int): Time in seconds to sleep between checking task status.

    Returns:
        list: List of dictionaries containing the export tasks with updated status.

    raises:
        TypeError: If export_tasks is not a list of dictionaries.
    """

    logger.debug("Starting export tasks...")
    export_tasks = copy.deepcopy(export_tasks)

    if not isinstance(export_tasks, list):
        raise TypeError("export_tasks must be a list of dictionaries")

    skipped_tasks = 0
    clean_export_tasks = []
    for task in export_tasks:
        # Skip if item is not a dictionary
        if not isinstance(task, dict):
            skipped_tasks += 1
            logger.error(f"skipping task - not a dict: {task}")
            continue
        # Skip if dictionary does not have the right keys
        required_keys = ["task", "image", "target"]
        if not all(
            [key in required_keys for key in task.keys() if key in required_keys]
        ):
            logger.error(f"skipping task - missing keys: {task}")
            skipped_tasks += 1
            continue
        try:
            current_status = task.get("status", "pending").upper()
            if current_status in SKIP_TASK_STATUS:
                logger.info(
                    f"Skipping task: {task['image']} with status {current_status}"
                )
            if current_status == "MOCK_CREATED":
                task["status"] = "mock_task_skipped"
            else:
                task["task"].start()
                task["status"] = "started"
        except Exception as e:
            task["status"] = "failed_to_start"
            task["error"] = str(e)
            logger.error(f"Failed to start task: {task['image']} to {task['target']}")
            logger.error(e)
        clean_export_tasks.append(task)

    # Track tasks
    finished_tasks = []
    continue_export_tracking = True
    while continue_export_tracking:
        continue_export_tracking = False
        for i, task in enumerate(clean_export_tasks):
            if i not in finished_tasks:
                # skip failed exports and starts
                if task.get("status", "pending").upper() in SKIP_TASK_STATUS:
                    finished_tasks.append(i)
                    continue
                try:
                    status = task["task"].status()
                    status = status["state"]
                except Exception as e:
                    status = "FAILED_TO_GET_STATUS"
                    task["error"] = str(e)
                    logger.error(e)
                task["status"] = status.lower()

                if status in GEE_TASK_UNFINISHED_STATUS:
                    continue_export_tracking = True
                elif status in GEE_TASK_FINISHED_STATUS:
                    logger.info(
                        f"Task {task['image']} to {task['target']} finished with status: {status.lower()}"
                    )
                    finished_tasks.append(i)
                else:
                    logger.warning(
                        f"Task {task['image']} to {task['target']} finished with unknown status: {status.lower()}"
                    )
                    finished_tasks.append(i)

        if continue_export_tracking:
            sleep(sleep_time)

    return clean_export_tasks

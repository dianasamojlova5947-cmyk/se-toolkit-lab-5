import httpx
from sqlalchemy import select, func
from datetime import datetime

from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            auth=(settings.autochecker_email, settings.autochecker_password)
        )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch items: {response.text}")

    return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    params = {"limit": 500}
    
    if since:
        params["since"] = since.isoformat()

    all_logs = []

    async with httpx.AsyncClient() as client:
    while True:
        response = await client.get(
            url,
            params=params,
            auth=(settings.autochecker_email, settings.autochecker_password)
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch logs: {response.text}")

        data = response.json()
        logs = data["logs"]

        if not logs:
            break

        all_logs.extend(logs)

        if not data["has_more"]:
            break

        params["since"] = logs[-1]["submitted_at"]

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    # labs
    for item in items:
        if item["type"] == "lab":
        result = await session.execute(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == item["title"]
            )
        )
        lab = result.scalar_one_or_none()

        if not lab:
            lab = ItemRecord(type="lab", title=item["title"])
            session.add(lab)
            await session.flush()
            new_items += 1

        lab_map[item["lab"]] = lab

    # tasks
    for item in items:
        if item["type"] == "task":
        lab = lab_map.get(item["lab"])
        if not lab:
            continue

        result = await session.execute(
            select(ItemRecord).where(
                ItemRecord.title == item["title"],
                ItemRecord.parent_id == lab.id
            )
        )
        task = result.scalar_one_or_none()

        if not task:
            task = ItemRecord(
                type="task",
                title=item["title"],
                parent_id=lab.id
            )
            session.add(task)
            new_items += 1

    await session.commit()

    return new_items


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    new_items = 0
    lab_map = {}

    new_logs = 0

    # map short ids -> titles
    lookup = {}
    for item in items_catalog:
    key = (item["lab"], item["task"])
    lookup[key] = item["title"]

    for log in logs:

    # learner
    result = await session.execute(
        select(Learner).where(
            Learner.external_id == log["student_id"]
        )
    )
    learner = result.scalar_one_or_none()

    if not learner:
        learner = Learner(
            external_id=log["student_id"],
            student_group=log["group"]
        )
        session.add(learner)
        await session.flush()

    # item title
    title = lookup.get((log["lab"], log["task"]))
    if not title:
        continue

    result = await session.execute(
        select(ItemRecord).where(ItemRecord.title == title)
    )
    item = result.scalar_one_or_none()

    if not item:
        continue

    # idempotency
    result = await session.execute(
        select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
    )
    exists = result.scalar_one_or_none()

    if exists:
        continue

    interaction = InteractionLog(
        external_id=log["id"],
        learner_id=learner.id,
        item_id=item.id,
        kind="attempt",
        score=log["score"],
        checks_passed=log["passed"],
        checks_total=log["total"],
        created_at=datetime.fromisoformat(
            log["submitted_at"].replace("Z", "+00:00")
        )
    )

    session.add(interaction)
    new_logs += 1

    await session.commit()

    return new_logs


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    # Step 1: items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: last timestamp
    result = await session.execute(
    select(func.max(InteractionLog.created_at))
)

    last_sync = result.scalar_one_or_none()

    # Step 3: logs
    logs = await fetch_logs(last_sync)
    new_records = await load_logs(logs, items, session)

    # total count
    result = await session.execute(
    select(func.count()).select_from(InteractionLog)
)

    total_records = result.scalar_one()

    return {
    "new_records": new_records,
    "total_records": total_records
}

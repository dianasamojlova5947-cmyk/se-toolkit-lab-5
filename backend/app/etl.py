"""ETL pipeline for syncing Autochecker data into the database."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from .models.interaction import InteractionLog
from .models.item import ItemRecord
from .models.learner import Learner
from .settings import settings

TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def _auth() -> tuple[str, str]:
    return settings.autochecker_email, settings.autochecker_password


def _base_url() -> str:
    return settings.autochecker_api_url.rstrip("/")


def _parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return timestamp.astimezone(UTC).replace(tzinfo=None)


def _format_since(value: datetime) -> str:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return aware.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _catalog_indexes(
    items: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    labs: dict[str, dict[str, Any]] = {}
    tasks: dict[tuple[str, str], dict[str, Any]] = {}

    for item in items:
        lab_id = item.get("lab")
        task_id = item.get("task")
        item_type = item.get("type")

        if item_type == "lab" and lab_id:
            labs[str(lab_id)] = item
        elif item_type == "task" and lab_id and task_id:
            tasks[(str(lab_id), str(task_id))] = item

    return labs, tasks


async def _get_item(
    session: AsyncSession,
    *,
    type: str,
    title: str,
    parent_id: int | None,
) -> ItemRecord | None:
    statement = select(ItemRecord).where(
        ItemRecord.type == type,
        ItemRecord.title == title,
    )
    if parent_id is None:
        statement = statement.where(ItemRecord.parent_id.is_(None))
    else:
        statement = statement.where(ItemRecord.parent_id == parent_id)

    result = await session.exec(statement)
    return result.first()


async def fetch_items() -> list[dict[str, Any]]:
    """Fetch the lab/task catalog from the Autochecker API."""

    async with httpx.AsyncClient(
        auth=_auth(), base_url=_base_url(), timeout=TIMEOUT
    ) as client:
        response = await client.get("/api/items")
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: str | None = None) -> list[dict[str, Any]]:
    """Fetch check logs from the Autochecker API with pagination."""

    logs: list[dict[str, Any]] = []
    params: dict[str, Any] = {"limit": 100}
    if since is not None:
        params["since"] = since

    async with httpx.AsyncClient(
        auth=_auth(), base_url=_base_url(), timeout=TIMEOUT
    ) as client:
        while True:
            response = await client.get("/api/logs", params=params)
            response.raise_for_status()
            data = response.json()
            page = data.get("logs", [])
            logs.extend(page)

            if not data.get("has_more") or not page:
                break

            params["since"] = page[-1]["submitted_at"]

    return logs


async def load_items(
    items: list[dict[str, Any]], session: AsyncSession
) -> dict[str, int]:
    """Insert labs and tasks into the database."""

    labs, tasks = _catalog_indexes(items)
    lab_rows: dict[str, ItemRecord] = {}
    new_records = 0

    for lab_id, lab in labs.items():
        item = await _get_item(session, type="lab", title=lab["title"], parent_id=None)
        if item is None:
            item = ItemRecord(type="lab", title=lab["title"], parent_id=None)
            session.add(item)
            await session.flush()
            new_records += 1
        lab_rows[lab_id] = item

    for (lab_id, _task_id), task in tasks.items():
        parent = lab_rows.get(lab_id)
        if parent is None:
            continue

        item = await _get_item(
            session,
            type="task",
            title=task["title"],
            parent_id=parent.id,
        )
        if item is None:
            session.add(
                ItemRecord(type="task", title=task["title"], parent_id=parent.id)
            )
            new_records += 1

    await session.commit()
    return {"new_records": new_records, "total_records": len(labs) + len(tasks)}


async def load_logs(
    logs: list[dict[str, Any]],
    items: list[dict[str, Any]],
    session: AsyncSession,
) -> dict[str, int]:
    """Insert learners and interaction logs into the database."""

    labs, tasks = _catalog_indexes(items)
    new_records = 0

    for log in logs:
        student_id = str(log["student_id"])
        student_group = str(log.get("group", ""))
        lab_id = str(log["lab"])
        task_id = log.get("task")
        external_id = int(log["id"])

        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = learner_result.first()
        if learner is None:
            learner = Learner(external_id=student_id, student_group=student_group)
            session.add(learner)
            await session.flush()
        elif student_group and learner.student_group != student_group:
            learner.student_group = student_group
            session.add(learner)

        existing = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == external_id)
        )
        if existing.first() is not None:
            continue

        lab_item = labs.get(lab_id)
        if lab_item is None:
            continue

        task_item = tasks.get((lab_id, str(task_id))) if task_id is not None else None
        if task_id is not None and task_item is None:
            continue

        if task_item is not None:
            parent_lab = await _get_item(
                session,
                type="lab",
                title=lab_item["title"],
                parent_id=None,
            )
            if parent_lab is None:
                continue

            db_item = await _get_item(
                session,
                type="task",
                title=task_item["title"],
                parent_id=parent_lab.id,
            )
        else:
            db_item = await _get_item(
                session,
                type="lab",
                title=lab_item["title"],
                parent_id=None,
            )

        if db_item is None:
            continue

        session.add(
            InteractionLog(
                external_id=external_id,
                learner_id=learner.id,
                item_id=db_item.id,
                kind="attempt",
                score=float(log["score"]) if log.get("score") is not None else None,
                checks_passed=int(log["passed"])
                if log.get("passed") is not None
                else None,
                checks_total=int(log["total"])
                if log.get("total") is not None
                else None,
                created_at=_parse_timestamp(log["submitted_at"]),
            )
        )
        new_records += 1

    await session.commit()
    return {"new_records": new_records, "total_records": len(logs)}


async def sync(session: AsyncSession) -> dict[str, int]:
    """Run the full ETL pipeline."""

    items = await fetch_items()
    await load_items(items, session)

    result = await session.exec(select(func.max(InteractionLog.created_at)))
    last_synced_at = result.first()
    since = _format_since(last_synced_at) if last_synced_at is not None else None

    logs = await fetch_logs(since=since)
    load_result = await load_logs(logs, items, session)

    total_result = await session.exec(select(func.count(InteractionLog.id)))
    total_records = total_result.one()

    return {
        "new_records": load_result["new_records"],
        "total_records": total_records,
    }

"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


async def _get_lab_id(session: AsyncSession, lab: str) -> int | None:
    lab_title = lab.replace("-", " ").title()
    statement = select(ItemRecord.id).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%{lab_title}%"),
    )
    result = await session.exec(statement)
    return result.scalars().first()


async def _get_task_ids(session: AsyncSession, lab_id: int) -> list[int]:
    statement = select(ItemRecord.id).where(ItemRecord.parent_id == lab_id)
    result = await session.exec(statement)
    return list(result.scalars().all())


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    lab_id = await _get_lab_id(session, lab)
    if lab_id is None:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    task_ids = await _get_task_ids(session, lab_id)
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    bucket = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")
    statement = (
        select(bucket, func.count(InteractionLog.id))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket)
    )
    result = await session.exec(statement)
    counts = {row[0]: row[1] for row in result.all()}
    buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [{"bucket": name, "count": counts.get(name, 0)} for name in buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    lab_id = await _get_lab_id(session, lab)
    if lab_id is None:
        return []

    statement = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .select_from(ItemRecord)
        .outerjoin(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_id)
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    result = await session.exec(statement)
    return [
        {
            "task": row.task,
            "avg_score": row.avg_score,
            "attempts": row.attempts,
        }
        for row in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    lab_id = await _get_lab_id(session, lab)
    if lab_id is None:
        return []

    task_ids = await _get_task_ids(session, lab_id)
    if not task_ids:
        return []

    date_expr = func.date(InteractionLog.created_at).label("date")
    statement = (
        select(date_expr, func.count(InteractionLog.id).label("submissions"))
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(date_expr)
        .order_by(date_expr)
    )
    result = await session.exec(statement)
    return [
        {"date": row.date, "submissions": row.submissions} for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    lab_id = await _get_lab_id(session, lab)
    if lab_id is None:
        return []

    task_ids = await _get_task_ids(session, lab_id)
    if not task_ids:
        return []

    statement = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .select_from(InteractionLog)
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(
            InteractionLog.item_id.in_(task_ids),
            Learner.student_group != "",
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    result = await session.exec(statement)
    return [
        {
            "group": row.group,
            "avg_score": row.avg_score,
            "students": row.students,
        }
        for row in result.all()
    ]

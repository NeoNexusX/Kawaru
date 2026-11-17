"""历史访问统计的路由契约测试；Redis 全部替换，不访问数据库或外部服务。"""

from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, Mock, call

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from api import stats_api
from database import visit_stats
from routers.stats import stats_router


@pytest.fixture
def redis_values():
    return {}


@pytest.fixture
def redis_reader(monkeypatch, redis_values):
    async def read(keys, *other_keys):
        if isinstance(keys, str):
            keys = [keys, *other_keys]
        return [redis_values.get(key) for key in keys]

    # 仅允许 MGET：任何续期、写计数、扫描或逐条 GET 都会令测试失败。
    reader = Mock(spec_set=["mget"])
    reader.mget = AsyncMock(side_effect=read)
    monkeypatch.setattr(visit_stats, "redis_client", reader)
    return reader


@pytest.fixture
def clock(monkeypatch):
    now = Mock(return_value=datetime(2026, 11, 17, 12, tzinfo=UTC))
    monkeypatch.setattr(stats_api, "utc_time", now)
    monkeypatch.setattr(visit_stats, "utc_time", now)
    return now


@pytest.fixture
def client(redis_reader, clock):
    app = FastAPI()
    app.include_router(stats_router)
    with TestClient(app) as test_client:
        yield test_client


def test_default_returns_30_days_with_missing_markers(client, redis_reader):
    response = client.get("/stats/visits/history")

    assert response.status_code == 200
    body = response.json()
    assert body["timezone"] == "UTC"
    assert body["days"] == 30
    assert body["start_date"] == "2026-10-19"
    assert body["end_date"] == "2026-11-17"
    assert body["range_total"] == 0
    assert body["missing_days"] == 30
    assert len(body["items"]) == 30
    assert all(item["count"] == 0 and item["missing"] for item in body["items"])
    assert sum(item["is_partial"] for item in body["items"]) == 1
    assert body["items"][-1]["is_partial"] is True
    redis_reader.mget.assert_awaited_once()


def test_history_preserves_zero_missing_order_and_range_total(client, redis_values, redis_reader):
    redis_values.update({
        "stats:all:daily:2026-11-14": "120",
        "stats:all:daily:2026-11-15": "0",
        "stats:all:daily:2026-11-17": "60",
        "stats:all:total": "9999",
    })

    response = client.get("/stats/visits/history", params={"days": 4})

    assert response.status_code == 200
    assert response.json() == {
        "timezone": "UTC",
        "start_date": "2026-11-14",
        "end_date": "2026-11-17",
        "days": 4,
        "range_total": 180,
        "missing_days": 1,
        "items": [
            {"date": "2026-11-14", "count": 120, "missing": False, "is_partial": False},
            {"date": "2026-11-15", "count": 0, "missing": False, "is_partial": False},
            {"date": "2026-11-16", "count": 0, "missing": True, "is_partial": False},
            {"date": "2026-11-17", "count": 60, "missing": False, "is_partial": True},
        ],
    }
    assert redis_reader.mock_calls == [call.mget([
        "stats:all:daily:2026-11-14",
        "stats:all:daily:2026-11-15",
        "stats:all:daily:2026-11-16",
        "stats:all:daily:2026-11-17",
    ])]


def test_history_does_not_schedule_visit_count(client, monkeypatch):
    increment = AsyncMock()
    monkeypatch.setattr(visit_stats, "incr_visit", increment)

    assert client.get("/stats/visits/history", params={"days": 1}).status_code == 200
    increment.assert_not_awaited()


@pytest.mark.parametrize("days,start", [(1, "2026-11-17"), (90, "2026-08-20")])
def test_day_count_boundaries(client, redis_reader, days, start):
    response = client.get("/stats/visits/history", params={"days": days})

    assert response.status_code == 200
    body = response.json()
    expected_dates = [
        (date.fromisoformat(start) + timedelta(days=offset)).isoformat()
        for offset in range(days)
    ]
    assert body["start_date"] == start
    assert [item["date"] for item in body["items"]] == expected_dates
    redis_reader.mget.assert_awaited_once_with([
        f"stats:all:daily:{day}" for day in expected_dates
    ])


@pytest.mark.parametrize(
    "today,end_date,days,expected_dates",
    [
        ("2026-11-17", "2026-08-20", 1, ["2026-08-20"]),
        ("2026-11-17", "2026-11-01", 3, ["2026-10-30", "2026-10-31", "2026-11-01"]),
        ("2027-01-02", "2027-01-01", 3, ["2026-12-30", "2026-12-31", "2027-01-01"]),
        ("2028-03-02", "2028-03-01", 3, ["2028-02-28", "2028-02-29", "2028-03-01"]),
    ],
)
def test_custom_range_across_calendar_boundaries(client, clock, today, end_date, days, expected_dates):
    clock.return_value = datetime.fromisoformat(today).replace(tzinfo=UTC)

    response = client.get("/stats/visits/history", params={"days": days, "end_date": end_date})

    assert response.status_code == 200
    body = response.json()
    assert body["start_date"] == expected_dates[0]
    assert body["end_date"] == end_date
    assert [item["date"] for item in body["items"]] == expected_dates
    assert not any(item["is_partial"] for item in body["items"])


@pytest.mark.parametrize("params", [
    {"days": "0"},
    {"days": "91"},
    {"days": "-1"},
    {"days": "1.5"},
    {"days": "abc"},
    {"end_date": "2026-02-30"},
    {"end_date": "not-a-date"},
])
def test_invalid_parameters_are_422_without_redis_access(client, redis_reader, params):
    assert client.get("/stats/visits/history", params=params).status_code == 422
    redis_reader.mget.assert_not_awaited()


@pytest.mark.parametrize("params", [
    {"days": 1, "end_date": "2026-11-18"},
    {"days": 1, "end_date": "2026-08-19"},
    {"days": 90, "end_date": "2026-11-16"},
    {"days": 2, "end_date": "2026-08-20"},
    {"end_date": "0001-01-01"},
])
def test_outside_retention_is_400_without_clamping(client, redis_reader, params):
    response = client.get("/stats/visits/history", params=params)

    assert response.status_code == 400
    assert "items" not in response.json()
    redis_reader.mget.assert_not_awaited()


def test_one_utc_snapshot_is_used_across_midnight(client, clock, redis_values):
    clock.side_effect = [
        datetime(2026, 11, 17, 23, 59, 59, tzinfo=UTC),
        datetime(2026, 11, 18, 0, 0, 0, tzinfo=UTC),
    ]
    redis_values["stats:all:daily:2026-11-17"] = "12"

    before = client.get("/stats/visits/history", params={"days": 1})
    assert before.status_code == 200
    assert clock.call_count == 1
    assert before.json()["items"] == [
        {"date": "2026-11-17", "count": 12, "missing": False, "is_partial": True}
    ]

    after = client.get("/stats/visits/history", params={"days": 1})
    assert after.status_code == 200
    assert clock.call_count == 2
    assert after.json()["items"] == [
        {"date": "2026-11-18", "count": 0, "missing": True, "is_partial": True}
    ]


@pytest.mark.parametrize("error_type", [RedisConnectionError, RedisTimeoutError])
def test_redis_failure_is_503_not_zero_counts(client, redis_reader, error_type):
    redis_reader.mget.side_effect = error_type("backend unavailable")

    response = client.get("/stats/visits/history")

    assert response.status_code == 503
    assert "temporarily unavailable" in response.text
    assert "items" not in response.json()
    assert "backend unavailable" not in response.text


@pytest.mark.parametrize("value", ["", "-1", "1.5", "NaN", "invalid", "1_000", " 1", "１２"])
def test_corrupt_counter_is_500_not_missing_or_zero(client, redis_values, value):
    redis_values["stats:all:daily:2026-11-17"] = value

    response = client.get("/stats/visits/history", params={"days": 1})

    assert response.status_code == 500
    assert "Internal server error" in response.text
    assert "items" not in response.json()


def test_existing_summary_contract_is_unchanged(client, redis_values, redis_reader):
    redis_values.update({
        "stats:all:daily:2026-11-17": "12",
        "stats:all:monthly:2026-11": "34",
        "stats:all:total": "567",
    })

    response = client.get("/stats/visits")

    assert response.status_code == 200
    assert response.json() == {"daily": 12, "monthly": 34, "total": 567}
    redis_reader.mget.assert_awaited_once_with(
        "stats:all:daily:2026-11-17", "stats:all:monthly:2026-11", "stats:all:total"
    )


def test_openapi_documents_public_query_and_response(client):
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/stats/visits/history"]["get"]
    parameters = {param["name"]: param for param in operation["parameters"]}

    assert not operation.get("security")
    assert set(parameters) == {"days", "end_date"}
    assert parameters["days"]["schema"]["default"] == 30
    assert parameters["days"]["schema"]["minimum"] == 1
    assert parameters["days"]["schema"]["maximum"] == 90
    assert {"200", "400", "422", "500", "503"} <= operation["responses"].keys()
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/VisitHistoryResponse"
    }

"""Cross-batch reconciliation of ticket numbers shared between current lists.

The comparison is defined on the downloaded CSV: row order is ignored,
duplicate rows count by multiplicity, and a ticket line with no replacement
part has an empty part code. Only ticket numbers present in both this
submission and another batch's current manifest are compared.
"""

import csv
import io

import pytest


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("SUPPLY_DB", str(tmp_path / "supply.db"))
    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as test_client:
        yield test_client


def publish(client, batch, tickets, parts, row_limit="100"):
    return client.post(
        f"/api/batches/{batch}/publish",
        files={
            "tickets": ("tickets.csv", tickets.encode(), "text/csv"),
            "parts": ("parts.csv", parts.encode(), "text/csv"),
        },
        data={"row_limit": str(row_limit)},
    )


def ticket_rows(*rows):
    return "ticket_no,station\n" + "".join(f"{t},{s}\n" for t, s in rows)


def part_rows(*rows):
    return "ticket_no,part_code\n" + "".join(f"{t},{p}\n" for t, p in rows)


def download(client, manifest):
    """Every detail row of a manifest, as a multiset; excludes headers."""
    rows = []
    for package in manifest["packages"]:
        body = list(
            csv.reader(io.StringIO(client.get(package["download_url"]).text))
        )
        rows.extend(body[1:])
    return sorted(rows)


def latest(client, batch):
    return client.get(f"/api/batches/{batch}/manifests/latest")


def test_conflicting_shared_ticket_rejects_and_keeps_both_current(client):
    v1 = publish(
        client,
        "b1",
        ticket_rows(("T1", "S1"), ("T2", "S2")),
        part_rows(("T1", "P1"), ("T2", "P2")),
    )
    assert v1.status_code == 201, v1.text
    before_b1 = v1.json()

    conflict = publish(
        client,
        "b2",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", "P9")),
    )
    assert conflict.status_code == 409, conflict.text
    detail = conflict.json()["detail"]
    assert detail["error"] == "ticket_detail_conflict"
    assert detail["conflicts"] == [{"ticket_no": "T1", "batch_id": "b1"}]

    # Rejected batch has no manifest at all; the other batch is untouched.
    assert latest(client, "b2").status_code == 404
    assert latest(client, "b1").json()["version"] == before_b1["version"]
    assert download(client, before_b1) == [
        ["T1", "S1", "P1"],
        ["T2", "S2", "P2"],
    ]


def test_rejected_resubmission_keeps_previous_manifest_current(client):
    first = publish(
        client,
        "b2",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", "P1")),
    )
    assert first.status_code == 201, first.text
    publish(
        client,
        "b1",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", "P1")),
    )

    rejected = publish(
        client,
        "b1",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", "P2")),
    )
    assert rejected.status_code == 409, rejected.text
    # b1's original version is still the only one and still current.
    assert latest(client, "b1").json()["version"] == "b1.v0001"
    versions = [
        m["version"] for m in client.get("/api/batches/b1/manifests").json()["manifests"]
    ]
    assert versions == ["b1.v0001"]
    assert download(client, latest(client, "b1").json()) == [["T1", "S1", "P1"]]


def test_same_detail_different_submission_order_is_accepted(client):
    publish(
        client,
        "b1",
        ticket_rows(("T1", "S1"), ("T2", "S2"), ("T1", "S1")),
        part_rows(("T1", "PA"), ("T2", "PB")),
    )
    # Same downloaded rows, but rows are supplied in a different order.
    accepted = publish(
        client,
        "b2",
        ticket_rows(("T2", "S2"), ("T1", "S1"), ("T1", "S1")),
        part_rows(("T2", "PB"), ("T1", "PA")),
    )
    assert accepted.status_code == 201, accepted.text


def test_equal_multiset_with_scaled_duplicates_is_accepted(client):
    # 2 ticket rows x 1 part line and 1 ticket row x 2 part lines both
    # download the same two identical detail rows.
    publish(
        client,
        "b1",
        ticket_rows(("T1", "S1"), ("T1", "S1")),
        part_rows(("T1", "P1")),
    )
    accepted = publish(
        client,
        "b2",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", "P1"), ("T1", "P1")),
    )
    assert accepted.status_code == 201, accepted.text
    assert download(client, latest(client, "b1").json()) == download(
        client, latest(client, "b2").json()
    )


def test_different_duplicate_multiplicity_conflicts(client):
    publish(
        client,
        "b1",
        ticket_rows(("T1", "S1"), ("T1", "S1")),
        part_rows(("T1", "P1")),
    )
    rejected = publish(
        client,
        "b2",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", "P1")),
    )
    assert rejected.status_code == 409, rejected.text
    assert rejected.json()["detail"]["conflicts"] == [
        {"ticket_no": "T1", "batch_id": "b1"}
    ]


def test_no_parts_versus_empty_part_code_is_consistent(client):
    # A ticket line with no replacement part downloads with an empty part
    # code; a submitted part line whose code is empty is the same row.
    first = publish(
        client,
        "b1",
        ticket_rows(("T1", "S1")),
        part_rows(("T2", "P2")),
    )
    assert first.status_code == 201, first.text
    accepted = publish(
        client,
        "b2",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", ""), ("T2", "P2")),
    )
    assert accepted.status_code == 201, accepted.text


def test_station_difference_conflicts_even_with_same_parts(client):
    publish(
        client,
        "b1",
        ticket_rows(("T1", "S1")),
        part_rows(("T1", "P1")),
    )
    rejected = publish(
        client,
        "b2",
        ticket_rows(("T1", "S2")),
        part_rows(("T1", "P1")),
    )
    assert rejected.status_code == 409, rejected.text


def test_only_shared_ticket_numbers_are_compared(client):
    publish(
        client,
        "b1",
        ticket_rows(("T1", "S1"), ("T2", "S2")),
        part_rows(("T1", "P1"), ("T2", "P2")),
    )
    # Only T1 is shared; T9 differs from everything in b1 and is not compared.
    accepted = publish(
        client,
        "b2",
        ticket_rows(("T1", "S1"), ("T9", "S9")),
        part_rows(("T1", "P1"), ("T9", "P9")),
    )
    assert accepted.status_code == 201, accepted.text

    # T2 from b1 is not in b2, so it was never compared; changing it now that
    # it is shared conflicts.
    rejected = publish(
        client,
        "b2",
        ticket_rows(("T1", "S1"), ("T2", "S2"), ("T9", "S9")),
        part_rows(("T1", "P1"), ("T2", "PX"), ("T9", "P9")),
    )
    assert rejected.status_code == 409, rejected.text
    assert rejected.json()["detail"]["conflicts"] == [
        {"ticket_no": "T2", "batch_id": "b1"}
    ]


def test_conflicts_are_reported_for_every_other_batch(client):
    publish(client, "b1", ticket_rows(("T1", "S1")), part_rows(("T1", "P1")))
    publish(client, "b2", ticket_rows(("T2", "S2")), part_rows(("T2", "P2")))

    rejected = publish(
        client,
        "b3",
        ticket_rows(("T1", "S1"), ("T2", "S2")),
        part_rows(("T1", "PX"), ("T2", "PY")),
    )
    assert rejected.status_code == 409, rejected.text
    assert rejected.json()["detail"]["conflicts"] == [
        {"ticket_no": "T1", "batch_id": "b1"},
        {"ticket_no": "T2", "batch_id": "b2"},
    ]


def test_non_overlapping_batches_publish_while_others_exist(client):
    publish(client, "b1", ticket_rows(("T1", "S1")), part_rows(("T1", "P1")))
    # No shared ticket numbers: accepted despite b1 having a different detail
    # for its own tickets.
    accepted = publish(
        client,
        "b2",
        ticket_rows(("T2", "S2")),
        part_rows(("T2", "P2")),
    )
    assert accepted.status_code == 201, accepted.text

    # Order does not matter: a third batch overlapping only b2 must agree
    # with b2 and is unaffected by b1.
    accepted = publish(
        client,
        "b3",
        ticket_rows(("T2", "S2")),
        part_rows(("T2", "P2")),
    )
    assert accepted.status_code == 201, accepted.text


def test_conflict_compares_against_current_not_superseded_version(client):
    # b1 publishes P1 then supersedes it with P2.
    publish(client, "b1", ticket_rows(("T1", "S1")), part_rows(("T1", "P1")))
    superseded = publish(
        client, "b1", ticket_rows(("T1", "S1")), part_rows(("T1", "P2"))
    )
    assert superseded.status_code == 201, superseded.text
    assert superseded.json()["version"] == "b1.v0002"

    # A new batch matching the superseded v0001 conflicts with current v0002.
    stale = publish(client, "b2", ticket_rows(("T1", "S1")), part_rows(("T1", "P1")))
    assert stale.status_code == 409, stale.text
    assert stale.json()["detail"]["conflicts"] == [
        {"ticket_no": "T1", "batch_id": "b1"}
    ]

    # Matching the current version is accepted.
    current = publish(client, "b2", ticket_rows(("T1", "S1")), part_rows(("T1", "P2")))
    assert current.status_code == 201, current.text


def _outcome(tmp_path, monkeypatch, first_parts, second_parts):
    """Publish the same two conflicting detail sets in the given order."""
    monkeypatch.setenv("SUPPLY_DB", str(tmp_path / "supply.db"))
    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as fresh:
        first = publish(
            fresh, "b1", ticket_rows(("T1", "S1")), part_rows(*first_parts)
        )
        second = publish(
            fresh, "b2", ticket_rows(("T1", "S1")), part_rows(*second_parts)
        )
    return first.status_code, second.status_code


def test_import_order_does_not_change_the_outcome(tmp_path, monkeypatch):
    detailed = (("T1", "P1"),)
    other = (("T1", "P2"),)

    # The first submitted detail set is published; the second, disagreeing
    # set is rejected. Which side is submitted first does not make the pair
    # consistent or inconsistent -- the comparison is symmetric.
    assert _outcome(tmp_path, monkeypatch, detailed, other) == (201, 409)
    monkeypatch.delenv("SUPPLY_DB")
    assert _outcome(tmp_path, monkeypatch, other, detailed) == (201, 409)

    # Two identical detail sets are always accepted, in either order.
    monkeypatch.delenv("SUPPLY_DB")
    assert _outcome(tmp_path, monkeypatch, detailed, detailed) == (201, 201)
    monkeypatch.delenv("SUPPLY_DB")
    assert _outcome(tmp_path, monkeypatch, detailed, detailed) == (201, 201)

#!/usr/bin/env python3
"""
SF Insurance Broker Lead Tracker
- View all leads with search/filter
- LinkedIn pre-fill links
- Status tracking (New → Contacted → Invited → Confirmed → Declined)
- Notes per lead
- Dashboard stats
"""

import csv
import sqlite3
import os
from pathlib import Path
from urllib.parse import quote_plus
from datetime import datetime

from flask import Flask, render_template, request, jsonify, g

app = Flask(__name__)
DB_PATH = Path(__file__).parent / "leads.db"
CSV_PATH = Path(__file__).parent.parent / "output" / "sf_insurance_broker_targets.csv"

STATUSES = ["new", "contacted", "invited", "confirmed", "declined", "not_interested"]
STATUS_LABELS = {
    "new": "New",
    "contacted": "Contacted",
    "invited": "Invited",
    "confirmed": "Confirmed",
    "declined": "Declined",
    "not_interested": "Not Interested",
}


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(str(DB_PATH))
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(str(DB_PATH))
    db.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT DEFAULT '',
            address TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            website TEXT DEFAULT '',
            email TEXT DEFAULT '',
            rating TEXT DEFAULT '',
            priority_score INTEGER DEFAULT 0,
            notes TEXT DEFAULT '',
            status TEXT DEFAULT 'new',
            linkedin_url TEXT DEFAULT '',
            dinner_rsvp TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            contacted_at TEXT DEFAULT '',
            invited_at TEXT DEFAULT '',
            confirmed_at TEXT DEFAULT ''
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER,
            action TEXT,
            details TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        )
    """)
    db.commit()
    db.close()


def import_csv():
    db = sqlite3.connect(str(DB_PATH))
    count = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    if count > 0:
        db.close()
        return

    if not CSV_PATH.exists():
        db.close()
        return

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            db.execute("""
                INSERT INTO leads (name, type, address, phone, website, email, rating, priority_score, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get("name", ""),
                row.get("type", ""),
                row.get("address", ""),
                row.get("phone", ""),
                row.get("website", ""),
                row.get("email", ""),
                row.get("rating", ""),
                int(row.get("priority_score", 0) or 0),
                row.get("notes", ""),
            ))

    db.commit()
    imported = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    print(f"Imported {imported} leads from CSV")
    db.close()


# --- Routes ---

@app.route("/")
def index():
    return render_template("index.html", statuses=STATUS_LABELS)


@app.route("/api/leads")
def get_leads():
    db = get_db()
    status = request.args.get("status", "")
    lead_type = request.args.get("type", "")
    search = request.args.get("search", "")
    has_email = request.args.get("has_email", "")

    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if status:
        query += " AND status = ?"
        params.append(status)
    if lead_type:
        query += " AND type = ?"
        params.append(lead_type)
    if search:
        query += " AND (name LIKE ? OR email LIKE ? OR address LIKE ? OR phone LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if has_email == "yes":
        query += " AND email != ''"
    elif has_email == "no":
        query += " AND email = ''"

    query += " ORDER BY priority_score DESC, name ASC"
    rows = db.execute(query, params).fetchall()

    leads = []
    for r in rows:
        d = dict(r)
        name = d["name"]
        company = name
        # Build LinkedIn search URL
        d["linkedin_search"] = f"https://www.linkedin.com/search/results/all/?keywords={quote_plus(company + ' San Francisco')}"
        d["linkedin_message"] = f"https://www.linkedin.com/search/results/people/?keywords={quote_plus(company + ' San Francisco')}"
        leads.append(d)

    return jsonify(leads)


@app.route("/api/leads/<int:lead_id>", methods=["PATCH"])
def update_lead(lead_id):
    db = get_db()
    data = request.json

    allowed = ["status", "notes", "linkedin_url", "dinner_rsvp", "email", "phone", "website"]
    sets = []
    params = []

    for field in allowed:
        if field in data:
            sets.append(f"{field} = ?")
            params.append(data[field])

    if "status" in data:
        now = datetime.utcnow().isoformat()
        status = data["status"]
        if status == "contacted":
            sets.append("contacted_at = ?")
            params.append(now)
        elif status == "invited":
            sets.append("invited_at = ?")
            params.append(now)
        elif status == "confirmed":
            sets.append("confirmed_at = ?")
            params.append(now)

    if not sets:
        return jsonify({"error": "No fields to update"}), 400

    sets.append("updated_at = ?")
    params.append(datetime.utcnow().isoformat())
    params.append(lead_id)

    db.execute(f"UPDATE leads SET {', '.join(sets)} WHERE id = ?", params)

    # Log activity
    action = data.get("status", "updated")
    details = data.get("notes", "")
    if "status" in data:
        details = f"Status changed to {STATUS_LABELS.get(data['status'], data['status'])}"
    db.execute("INSERT INTO activity_log (lead_id, action, details) VALUES (?, ?, ?)",
               (lead_id, action, details))

    db.commit()
    return jsonify({"ok": True})


@app.route("/api/leads/<int:lead_id>/log")
def get_lead_log(lead_id):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM activity_log WHERE lead_id = ? ORDER BY created_at DESC",
        (lead_id,)
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/stats")
def get_stats():
    db = get_db()

    total = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    by_status = {}
    for s in STATUSES:
        by_status[s] = db.execute("SELECT COUNT(*) FROM leads WHERE status = ?", (s,)).fetchone()[0]

    by_type = {}
    for row in db.execute("SELECT type, COUNT(*) as c FROM leads GROUP BY type").fetchall():
        by_type[row["type"]] = row["c"]

    with_email = db.execute("SELECT COUNT(*) FROM leads WHERE email != ''").fetchone()[0]
    with_website = db.execute("SELECT COUNT(*) FROM leads WHERE website != ''").fetchone()[0]
    with_phone = db.execute("SELECT COUNT(*) FROM leads WHERE phone != ''").fetchone()[0]

    recent = db.execute(
        "SELECT l.name, a.action, a.details, a.created_at FROM activity_log a JOIN leads l ON l.id = a.lead_id ORDER BY a.created_at DESC LIMIT 15"
    ).fetchall()

    return jsonify({
        "total": total,
        "by_status": by_status,
        "by_type": by_type,
        "with_email": with_email,
        "with_website": with_website,
        "with_phone": with_phone,
        "recent_activity": [dict(r) for r in recent],
        "status_labels": STATUS_LABELS,
    })


@app.route("/api/bulk", methods=["POST"])
def bulk_update():
    db = get_db()
    data = request.json
    ids = data.get("ids", [])
    status = data.get("status", "")

    if not ids or not status:
        return jsonify({"error": "Need ids and status"}), 400

    now = datetime.utcnow().isoformat()
    for lid in ids:
        sets = ["status = ?", "updated_at = ?"]
        params = [status, now]
        if status == "contacted":
            sets.append("contacted_at = ?")
            params.append(now)
        elif status == "invited":
            sets.append("invited_at = ?")
            params.append(now)
        elif status == "confirmed":
            sets.append("confirmed_at = ?")
            params.append(now)

        params.append(lid)
        db.execute(f"UPDATE leads SET {', '.join(sets)} WHERE id = ?", params)
        db.execute("INSERT INTO activity_log (lead_id, action, details) VALUES (?, ?, ?)",
                   (lid, status, f"Bulk status change to {STATUS_LABELS.get(status, status)}"))

    db.commit()
    return jsonify({"ok": True, "updated": len(ids)})


if __name__ == "__main__":
    init_db()
    import_csv()
    print("\n  Lead Tracker running at http://localhost:5001\n")
    app.run(debug=True, port=5001)

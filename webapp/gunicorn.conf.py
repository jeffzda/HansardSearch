"""
gunicorn.conf.py — Hansard Search Web App

post_fork hook re-opens the SQLite connection in each worker after fork.
SQLite connections opened in the master process (--preload) are not safe
to share across fork boundaries; each worker needs its own connection.
"""


def post_fork(server, worker):
    """Re-open the SQLite FTS connection in the newly forked worker."""
    try:
        import sqlite3
        import app as flask_app
        flask_app._FTS_CONN = sqlite3.connect(
            str(flask_app._FTS_DB_PATH), check_same_thread=False
        )
        server.log.info("post_fork: SQLite connection re-opened in worker %s", worker.pid)
    except Exception as exc:
        server.log.warning("post_fork: failed to re-open SQLite connection: %s", exc)

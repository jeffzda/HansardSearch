"""
gunicorn.conf.py — Hansard Search Web App

post_fork hook reinitialises threading primitives that are inherited as
locked from the master process when --preload is used. Without this, the
body-cache lock acquired by the prewarm background thread in the master is
inherited in a permanently-locked state by each worker, causing every search
request to hang indefinitely.
"""
import threading


def post_fork(server, worker):
    """Reinitialise body cache after fork so the worker starts clean."""
    try:
        import app as flask_app
        flask_app._body_lock = threading.Lock()
        flask_app._body_cache = {}
        # Restart prewarm in this worker so the cache is warm before requests arrive
        t = threading.Thread(
            target=flask_app._prewarm_body_cache,
            daemon=True,
            name="body-prewarm",
        )
        t.start()
        server.log.info("post_fork: body cache reset and prewarm started in worker %s", worker.pid)
    except Exception as exc:
        server.log.warning("post_fork: failed to reset body cache: %s", exc)

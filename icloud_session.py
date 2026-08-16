import logging


CONTROL_PLANE_TIMEOUT = (10, 60)
DOWNLOAD_TIMEOUT = (10, 300)
_TIMEOUT_HOOK_MARKER = "_icloud_linux_timeout_hook"


def _safe_getattr(obj, name):
    try:
        return getattr(obj, name, None)
    except Exception:
        return None


def install_pyi_cloud_session_timeouts(api, logger=None):
    """Set safe defaults for pyicloud requests without replacing explicit ones."""
    logger = logger or logging.getLogger(__name__)
    sessions = []
    for owner in (api, _safe_getattr(api, "drive")):
        session = _safe_getattr(owner, "session")
        if session is not None and all(session is not known for known in sessions):
            sessions.append(session)

    if not sessions:
        logger.warning("Could not install pyicloud request timeouts: no session found")
        return False

    installed = False
    for session in sessions:
        request = _safe_getattr(session, "request")
        if not callable(request):
            logger.warning(
                "Could not install pyicloud request timeouts: session has no request method"
            )
            continue
        if getattr(request, _TIMEOUT_HOOK_MARKER, False) is True:
            installed = True
            continue

        def request_with_default_timeout(*args, _request=request, **kwargs):
            if kwargs.get("timeout") is None:
                kwargs["timeout"] = (
                    DOWNLOAD_TIMEOUT if kwargs.get("stream") else CONTROL_PLANE_TIMEOUT
                )
            return _request(*args, **kwargs)

        setattr(request_with_default_timeout, _TIMEOUT_HOOK_MARKER, True)
        try:
            setattr(session, "request", request_with_default_timeout)
            installed = True
        except Exception as exc:
            logger.warning("Could not install pyicloud request timeouts: %s", exc)

    return installed

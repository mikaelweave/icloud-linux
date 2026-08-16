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
    found_session = False

    def install_timeout_hook(session):
        request = _safe_getattr(session, "request")
        if not callable(request):
            logger.warning(
                "Could not install pyicloud request timeouts: session has no request method"
            )
            return False
        if getattr(request, _TIMEOUT_HOOK_MARKER, False) is True:
            return True

        def request_with_default_timeout(*args, _request=request, **kwargs):
            if kwargs.get("timeout") is None:
                kwargs["timeout"] = (
                    DOWNLOAD_TIMEOUT if kwargs.get("stream") else CONTROL_PLANE_TIMEOUT
                )
            return _request(*args, **kwargs)

        setattr(request_with_default_timeout, _TIMEOUT_HOOK_MARKER, True)
        try:
            setattr(session, "request", request_with_default_timeout)
            return True
        except Exception as exc:
            logger.warning("Could not install pyicloud request timeouts: %s", exc)
            return False

    api_session = _safe_getattr(api, "session")
    installed = False
    if api_session is not None:
        found_session = True
        installed = install_timeout_hook(api_session)

    drive = _safe_getattr(api, "drive")
    drive_session = _safe_getattr(drive, "session")
    if drive_session is not None and drive_session is not api_session:
        found_session = True
        installed = install_timeout_hook(drive_session) or installed

    if not found_session:
        logger.warning("Could not install pyicloud request timeouts: no session found")

    return installed

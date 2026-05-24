#!/usr/bin/env python3
"""Patch pyicloud 2.5.0 trusted-device bridge payload handling.

Apple changed the push payload field from ``sessionUUID`` to ``flowid`` after
pyicloud 2.5.0 was released. This helper applies the minimal compatibility
patch to the installed package in the active virtualenv until pyicloud ships the
fix upstream.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path


BRIDGE_MODULE = "pyicloud.hsa2_bridge"
MISMATCH_ERROR = "Trusted-device bridge returned a mismatched session UUID."


class PatchError(RuntimeError):
    pass


def _replace_once(text: str, old: str, new: str, label: str) -> tuple[str, bool]:
    count = text.count(old)
    if count == 0:
        return text, False
    if count > 1:
        raise PatchError(f"Patch anchor is ambiguous: {label}")
    return text.replace(old, new), True


def _replace_or_confirm(
    text: str, old: str, new: str, label: str, patched_marker: str
) -> tuple[str, bool]:
    text, changed = _replace_once(text, old, new, label)
    if changed or patched_marker in text:
        return text, changed
    raise PatchError(f"Patch anchor not found: {label}")


def is_flowid_compatible(text: str) -> bool:
    return (
        'alias="flowid"' in text
        and "validated.session_uuid or validated.flow_id" in text
        and MISMATCH_ERROR not in text
    )


def patch_bridge_source(text: str) -> tuple[str, bool]:
    if is_flowid_compatible(text):
        return text, False

    original = text
    replacements = [
        (
            '    session_uuid: StrictStr = Field(alias="sessionUUID")\n',
            '    session_uuid: Optional[StrictStr] = Field(default=None, alias="sessionUUID")\n'
            '    flow_id: Optional[StrictStr] = Field(default=None, alias="flowid")\n',
            "bridge payload model flowid field",
        ),
        (
            '    @field_validator("session_uuid")\n'
            "    @classmethod\n"
            "    def _validate_session_uuid(cls, value: str) -> str:\n"
            '        """Reject blank bridge session identifiers."""\n'
            "        if not value.strip():\n"
            '            raise ValueError("sessionUUID must not be blank")\n'
            "        return value\n",
            '    @field_validator("session_uuid", "flow_id")\n'
            "    @classmethod\n"
            "    def _validate_session_identifier(cls, value: Optional[str]) -> Optional[str]:\n"
            '        """Reject blank bridge session identifiers when present."""\n'
            "        if value is not None and not value.strip():\n"
            '            raise ValueError("Bridge session identifiers must not be blank")\n'
            "        return value\n",
            "bridge payload identifier validator",
        ),
        (
            "        if not validated.session_uuid:\n"
            "            raise PyiCloudTrustedDevicePromptException(\n"
            '                "Trusted-device bridge push payload is missing sessionUUID."\n'
            "            )\n"
            "\n"
            "        return cls(\n"
            "            payload=payload,\n"
            "            session_uuid=validated.session_uuid,\n",
            "        session_uuid = validated.session_uuid or validated.flow_id\n"
            "        if not session_uuid:\n"
            "            raise PyiCloudTrustedDevicePromptException(\n"
            '                "Trusted-device bridge push payload is missing sessionUUID/flowid."\n'
            "            )\n"
            "\n"
            "        return cls(\n"
            "            payload=payload,\n"
            "            session_uuid=session_uuid,\n",
            "bridge payload session_uuid resolution",
        ),
    ]

    patched_markers = [
        'alias="flowid"',
        '@field_validator("session_uuid", "flow_id")',
        "validated.session_uuid or validated.flow_id",
    ]
    for (old, new, label), patched_marker in zip(replacements, patched_markers):
        text, _ = _replace_or_confirm(text, old, new, label, patched_marker)

    mismatch_block_bootstrap = (
        "                if push_payload.session_uuid != session_uuid:\n"
        "                    raise PyiCloudTrustedDevicePromptException(\n"
        f'                        "{MISMATCH_ERROR}"\n'
        "                    )\n"
        "\n"
    )
    mismatch_block_verify = (
        "        if push_payload.session_uuid != bridge_state.session_uuid:\n"
        "            raise PyiCloudTrustedDeviceVerificationException(\n"
        f'                "{MISMATCH_ERROR}"\n'
        "            )\n"
    )
    text, changed_bootstrap = _replace_once(
        text,
        mismatch_block_bootstrap,
        "",
        "bridge bootstrap mismatch check",
    )
    text, changed_verify = _replace_once(
        text,
        mismatch_block_verify,
        "",
        "bridge verification mismatch check",
    )
    if text == original or not is_flowid_compatible(text):
        raise PatchError("Patch did not produce a flowid-compatible bridge module")
    return text, True


def find_bridge_path() -> Path:
    module = importlib.import_module(BRIDGE_MODULE)
    module_file = getattr(module, "__file__", None)
    if not module_file:
        raise PatchError(f"Unable to locate {BRIDGE_MODULE}")
    return Path(module_file)


def patch_bridge_file(path: Path) -> bool:
    original = path.read_text(encoding="utf-8")
    patched, changed = patch_bridge_source(original)
    if not changed:
        return False

    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        backup.write_text(original, encoding="utf-8")

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(patched, encoding="utf-8")
    os.replace(tmp_path, path)
    return True


def main() -> int:
    path = find_bridge_path()
    changed = patch_bridge_file(path)
    action = "Patched" if changed else "pyicloud bridge already compatible"
    print(f"{action}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import importlib.util
import pathlib
import unittest


PATCHER_PATH = pathlib.Path(__file__).parent / "scripts" / "patch_pyicloud_hsa2_bridge.py"
SPEC = importlib.util.spec_from_file_location("patch_pyicloud_hsa2_bridge", PATCHER_PATH)
patcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(patcher)


SAMPLE_BRIDGE_SOURCE = '''\
class _BridgePushPayloadModel(BaseModel):
    session_uuid: StrictStr = Field(alias="sessionUUID")
    next_step: Optional[StrictStr | StrictInt] = Field(default=None, alias="nextStep")

    @field_validator("session_uuid")
    @classmethod
    def _validate_session_uuid(cls, value: str) -> str:
        """Reject blank bridge session identifiers."""
        if not value.strip():
            raise ValueError("sessionUUID must not be blank")
        return value

    @field_validator("next_step")
    @classmethod
    def _validate_next_step(cls, value):
        return value

class BridgePushPayload:
    @classmethod
    def from_payload(cls, payload):
        validated = _BridgePushPayloadModel.model_validate(payload)
        if not validated.session_uuid:
            raise PyiCloudTrustedDevicePromptException(
                "Trusted-device bridge push payload is missing sessionUUID."
            )

        return cls(
            payload=payload,
            session_uuid=validated.session_uuid,
        )

class TrustedDeviceBridgeBootstrapper:
    def start(self):
                if push_payload.session_uuid != session_uuid:
                    raise PyiCloudTrustedDevicePromptException(
                        "Trusted-device bridge returned a mismatched session UUID."
                    )

                bridge_state = TrustedDeviceBridgeState()

    def _apply_bridge_push(self):
        if push_payload.session_uuid != bridge_state.session_uuid:
            raise PyiCloudTrustedDeviceVerificationException(
                "Trusted-device bridge returned a mismatched session UUID."
            )
        LOGGER.debug("Decoded")
'''


class PyiCloudBridgePatchTests(unittest.TestCase):
    def test_patch_bridge_source_adds_flowid_support_and_is_idempotent(self):
        patched, changed = patcher.patch_bridge_source(SAMPLE_BRIDGE_SOURCE)

        self.assertTrue(changed)
        self.assertIn('flow_id: Optional[StrictStr] = Field(default=None, alias="flowid")', patched)
        self.assertIn("validated.session_uuid or validated.flow_id", patched)
        self.assertNotIn("mismatched session UUID", patched)
        self.assertTrue(patcher.is_flowid_compatible(patched))

        patched_again, changed_again = patcher.patch_bridge_source(patched)
        self.assertFalse(changed_again)
        self.assertEqual(patched_again, patched)

    def test_patch_bridge_source_repairs_partially_patched_bridge(self):
        patched, _ = patcher.patch_bridge_source(SAMPLE_BRIDGE_SOURCE)
        partial = patched.replace(
            '        LOGGER.debug("Decoded")\n',
            "        if push_payload.session_uuid != bridge_state.session_uuid:\n"
            "            raise PyiCloudTrustedDeviceVerificationException(\n"
            '                "Trusted-device bridge returned a mismatched session UUID."\n'
            "            )\n"
            '        LOGGER.debug("Decoded")\n',
        )

        repaired, changed = patcher.patch_bridge_source(partial)

        self.assertTrue(changed)
        self.assertTrue(patcher.is_flowid_compatible(repaired))
        self.assertNotIn("mismatched session UUID", repaired)

    def test_patch_bridge_source_fails_when_expected_anchors_are_missing(self):
        with self.assertRaises(patcher.PatchError):
            patcher.patch_bridge_source("class _BridgePushPayloadModel(BaseModel):\n    pass\n")


if __name__ == "__main__":
    unittest.main()

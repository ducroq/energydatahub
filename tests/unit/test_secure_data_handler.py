"""Unit tests for utils/secure_data_handler.py — the AES-CBC + HMAC-SHA256 envelope.

This module had no unit test until 2026-08-08. It was not merely uncovered: because the
verification hook falls back to the whole tests/unit suite when a file has no mapped test,
editing it produced a *green* hook from 681 tests that never imported it. That is the
project's documented silent-no-op signature, applied to the one module named in Hard
Constraints ("all published data encrypted with AES-CBC + HMAC-SHA256").

These tests pin the properties the published contract actually depends on, not the
implementation: authenticity before confidentiality (verify the MAC, then decrypt), the
MAC covering the IV as well as the ciphertext, and a fresh IV per call.
"""

import base64
import json
import os

import pytest
from cryptography.exceptions import InvalidSignature

from utils.secure_data_handler import SecureDataHandler

KEY_LEN = 32  # AES-256 + HMAC-SHA256
IV_LEN = 16
SIG_LEN = 32


@pytest.fixture
def keys():
    """Deterministic-length but random keys — never a fixed literal, so a copied
    test key can't drift into being treated as a real one."""
    return os.urandom(KEY_LEN), os.urandom(KEY_LEN)


@pytest.fixture
def handler(keys):
    return SecureDataHandler(*keys)


@pytest.fixture
def payload():
    """Shaped like a real published file: the {metadata, data} envelope."""
    return {
        "metadata": {"schema_version": "2.4", "timezone": "Europe/Amsterdam"},
        "data": {"2026-08-08T14:00:00+02:00": {"price": -3.14, "volume": 0}},
    }


class TestRoundTrip:
    def test_roundtrip_preserves_payload(self, handler, payload):
        assert handler.decrypt_and_verify(handler.encrypt_and_sign(payload)) == payload

    @pytest.mark.parametrize(
        "value",
        [
            {},
            {"empty_nested": {}},
            {"unicode": "Ω 北 émoji 🌬 — Amsterdam"},
            {"floats": [1e-9, -0.0, 3.141592653589793]},
            {"nulls": None, "bools": [True, False]},
            {"deep": {"a": {"b": {"c": {"d": [1, 2, {"e": "f"}]}}}}},
        ],
    )
    def test_roundtrip_edge_payloads(self, handler, value):
        assert handler.decrypt_and_verify(handler.encrypt_and_sign(value)) == value

    def test_roundtrip_at_exact_block_multiple(self, handler):
        """Classic padding edge case: plaintext already a whole number of blocks must
        still round-trip, which requires appending a full extra block of padding rather
        than nothing. Getting this wrong truncates the last 16 bytes on decrypt."""
        empty = len(json.dumps({"f": ""}).encode())
        data = {"f": "x" * (-empty % 16 or 16)}
        assert len(json.dumps(data).encode()) % 16 == 0, "test setup must hit the edge case"
        assert handler.decrypt_and_verify(handler.encrypt_and_sign(data)) == data


class TestOutputShape:
    def test_output_is_ascii_base64(self, handler, payload):
        out = handler.encrypt_and_sign(payload)
        assert isinstance(out, str)
        out.encode("ascii")  # must survive embedding in a JSON file
        base64.b64decode(out)  # must be valid base64

    def test_output_layout_is_iv_ciphertext_mac(self, handler, payload):
        raw = base64.b64decode(handler.encrypt_and_sign(payload))
        assert len(raw) > IV_LEN + SIG_LEN
        body = len(raw) - IV_LEN - SIG_LEN
        assert body % 16 == 0, "ciphertext must be whole AES blocks"

    def test_plaintext_does_not_leak_into_ciphertext(self, handler):
        marker = "SUPER_DISTINCTIVE_MARKER_VALUE"
        raw = base64.b64decode(handler.encrypt_and_sign({"secret": marker}))
        assert marker.encode() not in raw


class TestIVFreshness:
    def test_same_payload_encrypts_differently_each_call(self, handler, payload):
        outs = {handler.encrypt_and_sign(payload) for _ in range(10)}
        assert len(outs) == 10, "a reused IV would make identical payloads identical"

    def test_ivs_are_distinct(self, handler, payload):
        ivs = {base64.b64decode(handler.encrypt_and_sign(payload))[:IV_LEN] for _ in range(10)}
        assert len(ivs) == 10


class TestTamperDetection:
    """Every mutation below must raise before any plaintext is produced."""

    @staticmethod
    def _flip(blob: str, index: int) -> str:
        raw = bytearray(base64.b64decode(blob))
        raw[index] ^= 0x01
        return base64.b64encode(bytes(raw)).decode()

    def test_tampered_ciphertext_is_rejected(self, handler, payload):
        blob = handler.encrypt_and_sign(payload)
        with pytest.raises(InvalidSignature):
            handler.decrypt_and_verify(self._flip(blob, IV_LEN + 1))

    def test_tampered_iv_is_rejected(self, handler, payload):
        """The MAC must cover the IV, not just the ciphertext — otherwise an attacker
        flips the IV and controls the first plaintext block under CBC."""
        blob = handler.encrypt_and_sign(payload)
        with pytest.raises(InvalidSignature):
            handler.decrypt_and_verify(self._flip(blob, 0))

    def test_tampered_signature_is_rejected(self, handler, payload):
        blob = handler.encrypt_and_sign(payload)
        raw = base64.b64decode(blob)
        with pytest.raises(InvalidSignature):
            handler.decrypt_and_verify(self._flip(blob, len(raw) - 1))

    def test_truncated_blob_is_rejected(self, handler, payload):
        raw = base64.b64decode(handler.encrypt_and_sign(payload))
        truncated = base64.b64encode(raw[:-16]).decode()
        with pytest.raises(InvalidSignature):
            handler.decrypt_and_verify(truncated)

    def test_wrong_hmac_key_is_rejected(self, keys, payload):
        enc_key, hmac_key = keys
        blob = SecureDataHandler(enc_key, hmac_key).encrypt_and_sign(payload)
        attacker = SecureDataHandler(enc_key, os.urandom(KEY_LEN))
        with pytest.raises(InvalidSignature):
            attacker.decrypt_and_verify(blob)

    def test_wrong_encryption_key_never_returns_a_payload(self, keys, payload):
        """Holding the right MAC key but the wrong encryption key, the MAC legitimately
        passes and decryption yields garbage. The contract that matters is that garbage
        never surfaces as a dict: it must raise, not return something a caller would
        treat as data. Today that is a JSONDecodeError from the parse step."""
        enc_key, hmac_key = keys
        blob = SecureDataHandler(enc_key, hmac_key).encrypt_and_sign(payload)
        wrong = SecureDataHandler(os.urandom(KEY_LEN), hmac_key)
        with pytest.raises((json.JSONDecodeError, UnicodeDecodeError, ValueError)):
            wrong.decrypt_and_verify(blob)

    def test_mac_is_checked_before_decryption(self, handler, payload):
        """Authenticity before confidentiality. A corrupted ciphertext must fail the MAC
        rather than reach the unpadding step — an unpad error on attacker-controlled input
        is the padding-oracle shape. InvalidSignature (not ValueError/IndexError) is the
        evidence that verify() ran first."""
        blob = handler.encrypt_and_sign(payload)
        corrupted = bytearray(base64.b64decode(blob))
        for i in range(IV_LEN, len(corrupted) - SIG_LEN):
            corrupted[i] ^= 0xFF
        with pytest.raises(InvalidSignature):
            handler.decrypt_and_verify(base64.b64encode(bytes(corrupted)).decode())

    def test_cross_handler_blobs_do_not_interchange(self, payload):
        a = SecureDataHandler(os.urandom(KEY_LEN), os.urandom(KEY_LEN))
        b = SecureDataHandler(os.urandom(KEY_LEN), os.urandom(KEY_LEN))
        with pytest.raises(InvalidSignature):
            b.decrypt_and_verify(a.encrypt_and_sign(payload))


class TestPadding:
    def test_pad_unpad_are_inverse(self, handler):
        for n in range(0, 40):
            data = b"y" * n
            assert handler._unpad(handler._pad(data)) == data

    def test_pad_always_adds_at_least_one_byte(self, handler):
        for n in range(0, 40):
            padded = handler._pad(b"y" * n)
            assert len(padded) > n
            assert len(padded) % 16 == 0

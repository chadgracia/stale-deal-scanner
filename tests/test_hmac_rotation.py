"""Reset links and HMAC_SECRET rotation: current key accepted, legacy
(HMAC_SECRET_OLD) refused with the expired text, empty key fails closed.
Fake keys; fake S3/SES/Pipeline from test_review."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from test_review import ReviewBase  # noqa: E402

NEW, OLD = "test-new-hmac", "test-old-hmac"


class ResetRotation(ReviewBase):
    def setUp(self):
        self.build({})
        self.lf.HMAC_SECRET, self.lf.HMAC_SECRET_OLD = NEW, OLD

    def body(self, token):
        return self.web("GET", {"action": "reset", "deal_id": "9", "token": token}).get("body") or ""

    def test_signs_with_current_key_only(self):
        self.assertEqual(self.lf.make_reset_token(9), self.lf._sign(NEW, "reset:9"))
        self.assertEqual(self.lf.make_token(9), self.lf._sign(NEW, "9"))

    def test_current_token_accepted(self):
        b = self.body(self.lf._sign(NEW, "reset:9"))
        self.assertNotIn("This reset link is invalid", b)
        self.assertNotIn(self.lf.LINK_EXPIRED_MSG, b)

    def test_legacy_token_refused_with_expired_text(self):
        b = self.body(self.lf._sign(OLD, "reset:9"))
        self.assertIn(self.lf.LINK_EXPIRED_MSG, b)
        b = self.web("POST", {"action": "reset", "deal_id": "9", "days": "30",
                              "token": self.lf._sign(OLD, "reset:9")}).get("body") or ""
        self.assertIn(self.lf.LINK_EXPIRED_MSG, b)
        self.assertEqual(self.pl.puts, [])
        self.assertEqual(self.ses.sent, [])

    def test_garbage_token_invalid(self):
        self.assertIn("This reset link is invalid", self.body("nope"))

    def test_legacy_off_without_old_key(self):
        self.lf.HMAC_SECRET_OLD = ""
        self.assertIn("This reset link is invalid", self.body(self.lf._sign(OLD, "reset:9")))

    def test_empty_key_fails_closed(self):
        self.lf.HMAC_SECRET = ""
        self.assertEqual(self.lf.make_reset_token(9), "")
        self.assertEqual(self.lf.make_token(9), "")
        self.assertEqual(self.lf.reset_url(9), "")
        for tok in ("", self.lf._sign("", "reset:9"), self.lf._sign(OLD, "reset:9"), self.lf._sign(NEW, "reset:9")):
            self.assertIn("This reset link is invalid", self.body(tok))


if __name__ == "__main__":
    unittest.main()

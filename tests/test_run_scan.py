"""Run: python3 -m unittest discover tests
run_scan with fake S3 / SES / Pipeline — proves it is report-only."""
import io, json, os, sys, types, unittest
from datetime import datetime, timezone, timedelta

CHAD = "cgracia@rainmakersecurities.com"


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.puts = []

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise Exception(f"NoSuchKey {Bucket}/{Key}")
        return {"Body": io.BytesIO(json.dumps(self.objects[(Bucket, Key)]).encode())}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = json.loads(Body)
        self.puts.append(Key)


class FakeSES:
    def __init__(self):
        self.sent = []

    def send_email(self, **kw):
        self.sent.append(kw)


def pdt(dt):
    return dt.strftime("%Y/%m/%d %H:%M:%S -0500")


def deal(did, name, stage_id, updated, refresh=None):
    cf = {"custom_label_1958": [5011675]}
    if refresh is not None:
        cf["custom_label_3994687"] = refresh
    return {"id": did, "name": name, "deal_stage": {"id": stage_id}, "updated_at": pdt(updated),
            "primary_contact": {"id": 900 + did, "full_name": "Client"}, "primary_contact_id": 900 + did,
            "custom_fields": cf}


class RunScanReportOnly(unittest.TestCase):
    def setUp(self):
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=400)
        self.deals = [
            deal(1, "Firm Way Overdue", 111800, old),
            deal(2, "Inquiry Overdue", 2109142, now - timedelta(days=100)),
            deal(3, "HOLD DEAL OVERDUE", 2094373, old),
            deal(4, "Confirm Never Stale", 2388323, old, refresh=0),
            deal(5, "Firm Heads Up", 111800, now - timedelta(days=55)),
        ]
        state = {"deals": {
            # Old code would have obsoleted deal 1 today (notice 10 days ago, not edited since).
            "1": {"noticed_at": (now - timedelta(days=10)).isoformat(), "updated_at_at_notice": self.deals[0]["updated_at"]},
            "2": {"noticed_at": (now - timedelta(days=1)).isoformat(), "updated_at_at_notice": "x",
                  "reset_at": (now - timedelta(days=70)).isoformat()},
            "3": {"noticed_at": (now - timedelta(days=10)).isoformat(), "updated_at_at_notice": self.deals[2]["updated_at"]},
            "77": {"obsoleted_at": (now - timedelta(days=2)).isoformat(), "noticed_at": now.isoformat()},
        }}
        self.s3 = FakeS3({("full-pipeline-cache", "deals.json"): {"deals": self.deals},
                          ("full-pipeline-cache", "stale-scanner-state.json"): state,
                          ("pipeline-token", "pipeline-jwt.json"): {"jwt": "x"}})
        self.ses = FakeSES()
        s3, ses = self.s3, self.ses

        def client(name, **kw):
            if name == "s3":
                return s3
            if name == "ses":
                return ses
            raise AssertionError(f"unexpected boto3 client {name} (Bedrock must not be called)")

        sys.modules["boto3"] = types.SimpleNamespace(client=client)
        os.environ.update({"DRY_RUN": "false", "TEST_EMAIL_OVERRIDE": "", "SCANNER_URL": "https://scan.example/",
                           "HMAC_SECRET": "t"})
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        sys.modules.pop("lambda_function", None)
        import lambda_function as lf
        self.lf = lf
        self.api_calls = []
        lf.call_pipeline_api = lambda method, endpoint, payload=None, jwt=None: (
            self.api_calls.append((method, endpoint, payload)) or {"status": 200, "data": {}})

        def no_network(*a, **k):
            raise AssertionError("network call attempted")
        lf.urllib.request.urlopen = no_network
        self.result = lf.lambda_handler({}, None)

    def test_no_pipeline_writes(self):
        puts = [c for c in self.api_calls if c[0] != "GET"]
        self.assertEqual(puts, [])

    def test_only_chad_gets_email(self):
        self.assertEqual(len(self.ses.sent), 1)
        for m in self.ses.sent:
            self.assertEqual(m["Destination"]["ToAddresses"], [CHAD])

    def test_report_contents(self):
        m = self.ses.sent[0]
        html_body = m["Message"]["Body"]["Html"]["Data"]
        text = m["Message"]["Body"]["Text"]["Data"]
        self.assertTrue(m["Message"]["Subject"]["Data"].startswith("Stale Deal Scanner Report — "))
        for body in (html_body, text):
            self.assertNotIn("HOLD DEAL", body)
            self.assertIn("Report only — the scanner no longer obsoletes deals or emails clients.", body)
            self.assertIn("Due for review (2)", body)
            self.assertIn("Heads-up — due in the next 7 days (1)", body)
            self.assertNotIn("Obsolete (", body)
            self.assertNotIn("Notices sent", body)
            self.assertNotIn("Pending removal", body)
            self.assertLess(body.index("Firm Way Overdue"), body.index("Inquiry Overdue"))  # oldest first
        self.assertIn("action=reset&amp;deal_id=1", html_body)
        self.assertEqual(self.result["due"], 2)
        self.assertEqual(self.result["headsup"], 1)

    def test_leftover_notice_keys_stripped(self):
        st = self.s3.objects[("full-pipeline-cache", "stale-scanner-state.json")]["deals"]
        for did, rec in st.items():
            self.assertNotIn("noticed_at", rec, did)
            self.assertNotIn("updated_at_at_notice", rec, did)
        self.assertIn("reset_at", st["2"])          # reset_at kept
        self.assertIn("obsoleted_at", st["77"])     # obsoleted_at kept
        self.assertNotIn("3", st)                   # Hold record popped
        self.assertNotIn("1", st)                   # empty record dropped


if __name__ == "__main__":
    unittest.main()

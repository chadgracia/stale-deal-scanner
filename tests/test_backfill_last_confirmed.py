"""Run: python3 -m unittest discover tests
Last Confirmed backfill with fake S3 / SES / Pipeline."""
import io, json, os, sys, types, unittest

CHAD = "cgracia@rainmakersecurities.com"
FIELD = "custom_label_4012491"


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


def deal(did, name, stage_id, updated, last_confirmed=None):
    cf = {"custom_label_1958": [5011675]}
    if last_confirmed is not None:
        cf[FIELD] = last_confirmed
    return {"id": did, "name": name, "deal_stage": {"id": stage_id}, "updated_at": updated,
            "custom_fields": cf}


DEALS = [
    deal(1, "Inquiry Blank", 2109142, "2026/03/05 10:00:00 -0500"),
    deal(2, "Hold Blank", 2094373, "2025/12/31 23:59:59 -0500"),
    deal(3, "Firm Already Set", 111800, "2026/01/01 10:00:00 -0500", "2026/02/02"),
    deal(4, "Obsolete Blank", 2348038, "2026/01/01 10:00:00 -0500"),   # not an active stage
    deal(5, "Matched Blank Snapshot Set Live", 2381534, "2026/04/01 10:00:00 -0500", ""),
]


class BackfillBase(unittest.TestCase):
    deals = DEALS
    live_cf = {5: {FIELD: "2026/10/01"}}   # deal 5 got a value after the snapshot
    put_status = 200

    def setUp(self):
        objects = {("pipeline-token", "pipeline-jwt.json"): {"jwt": "x"}}
        if self.deals is not None:
            objects[("full-pipeline-cache", "deals.json")] = {"deals": self.deals}
        self.s3 = FakeS3(objects)
        self.ses = FakeSES()
        s3, ses = self.s3, self.ses

        def client(name, **kw):
            if name == "s3":
                return s3
            if name == "ses":
                return ses
            raise AssertionError(f"unexpected boto3 client {name}")

        sys.modules["boto3"] = types.SimpleNamespace(client=client)
        os.environ.update({"DRY_RUN": "false", "TEST_EMAIL_OVERRIDE": "", "SCANNER_URL": "", "HMAC_SECRET": "t"})
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        sys.modules.pop("lambda_function", None)
        import lambda_function as lf
        self.lf = lf
        self.api_calls = []
        self.sleeps = []
        lf.time.sleep = lambda s: self.sleeps.append(s)

        def api(method, endpoint, payload=None, jwt=None):
            self.api_calls.append((method, endpoint, payload))
            did = int(endpoint.split("/")[2].split(".")[0])
            if method == "GET":
                return {"status": 200, "data": {"id": did, "custom_fields": dict(self.live_cf.get(did, {}))}}
            return {"status": self.put_status, "data": {}}
        lf.call_pipeline_api = api

        def no_network(*a, **k):
            raise AssertionError("network call attempted")
        lf.urllib.request.urlopen = no_network

    def puts(self):
        return [c for c in self.api_calls if c[0] != "GET"]

    def body(self):
        self.assertEqual(len(self.ses.sent), 1)
        m = self.ses.sent[0]
        self.assertEqual(m["Destination"]["ToAddresses"], [CHAD])
        return m["Message"]["Subject"]["Data"], m["Message"]["Body"]["Text"]["Data"]


class DryRun(BackfillBase):
    def test_dry_makes_no_writes(self):
        res = self.lf.lambda_handler({"backfill_last_confirmed": "dry"}, None)
        self.assertEqual(self.api_calls, [])
        self.assertEqual(self.s3.puts, [])
        subject, text = self.body()
        self.assertIn("DRY RUN", subject)
        self.assertEqual(res["selected"], 3)
        self.assertIn("Inquiry Blank (id=1, Inquiry) → 2026-03-05", text)
        self.assertIn("Hold Blank (id=2, Hold) → 2025-12-31", text)
        self.assertNotIn("Firm Already Set", text)
        self.assertNotIn("Obsolete Blank", text)


class Execute(BackfillBase):
    def test_execute_writes_only_the_field(self):
        res = self.lf.lambda_handler({"backfill_last_confirmed": "execute"}, None)
        puts = self.puts()
        self.assertEqual(sorted(p[1] for p in puts), ["/deals/1.json", "/deals/2.json"])
        self.assertEqual([p for p in puts if p[1] == "/deals/1.json"][0][2],
                         {"deal": {"custom_fields": {FIELD: "2026-03-05"}}})
        for _, _, payload in puts:
            self.assertEqual(list(payload), ["deal"])
            self.assertEqual(list(payload["deal"]), ["custom_fields"])
            self.assertEqual(list(payload["deal"]["custom_fields"]), [FIELD])
        self.assertEqual((res["written"], res["skipped"], res["failed"]), (2, 1, 0))
        self.assertIn(0.3, self.sleeps)
        _, text = self.body()
        self.assertIn("Written 2, skipped 1, failed 0", text)

    def test_existing_values_never_touched(self):
        self.lf.lambda_handler({"backfill_last_confirmed": "execute"}, None)
        touched = {c[1] for c in self.api_calls}
        self.assertNotIn("/deals/3.json", touched)          # set in snapshot: not even fetched
        self.assertNotIn("/deals/5.json", {p[1] for p in self.puts()})   # set live: GET only, no PUT
        self.assertIn(("GET", "/deals/5.json", None), self.api_calls)

    def test_eventbridge_never_reaches_backfill(self):
        self.lf.run_scan = lambda: {"scan": True}
        res = self.lf.lambda_handler({"source": "aws.events", "backfill_last_confirmed": "execute"}, None)
        self.assertEqual(res, {"scan": True})
        self.assertEqual(self.puts(), [])


class Sep23RestoreDateLeftBlank(BackfillBase):
    deals = DEALS + [deal(6, "Inquiry Restored Sep 23", 2109142, "2026/09/23 14:02:11 -0400"),
                     deal(7, "Firm Restored Sep 23", 111800, "2026/09/23 09:00:00 -0400")]

    def test_dry_lists_but_does_not_count(self):
        res = self.lf.lambda_handler({"backfill_last_confirmed": "dry"}, None)
        self.assertEqual(res["selected"], 3)
        self.assertEqual(res["left_blank_restore"], 2)
        _, text = self.body()
        self.assertIn("Deals that would be written: 3", text)
        head = text.index("Left blank — Sep 23 restore date (2):")
        self.assertGreater(text.index("Inquiry Restored Sep 23 (id=6, Inquiry)"), head)
        self.assertGreater(text.index("Firm Restored Sep 23 (id=7, Firm)"), head)
        self.assertNotIn("2026-09-23", text)

    def test_execute_never_touches_them(self):
        res = self.lf.lambda_handler({"backfill_last_confirmed": "execute"}, None)
        touched = {c[1] for c in self.api_calls}
        self.assertNotIn("/deals/6.json", touched)
        self.assertNotIn("/deals/7.json", touched)
        self.assertEqual(res["written"], 2)
        _, text = self.body()
        self.assertIn("Left blank — Sep 23 restore date (2):", text)


class StopsAfterFiveFailures(BackfillBase):
    deals = [deal(i, f"Deal {i}", 2109142, "2026/03/05 10:00:00 -0500") for i in range(1, 9)]
    live_cf = {}
    put_status = 503

    def test_stops(self):
        res = self.lf.lambda_handler({"backfill_last_confirmed": "execute"}, None)
        self.assertTrue(res["stopped"])
        self.assertEqual(res["failed"], 5)
        self.assertEqual(len(self.puts()), 5 * 4)   # each PUT: 1 try + 3 retries
        _, text = self.body()
        self.assertIn("not attempted 3", text)


class EmptySnapshotAborts(BackfillBase):
    deals = []

    def test_abort(self):
        res = self.lf.lambda_handler({"backfill_last_confirmed": "execute"}, None)
        self.assertEqual(res["statusCode"], 500)
        self.assertEqual(self.api_calls, [])
        subject, _ = self.body()
        self.assertIn("ABORTED", subject)


class MissingSnapshotAborts(EmptySnapshotAborts):
    deals = None


if __name__ == "__main__":
    unittest.main()

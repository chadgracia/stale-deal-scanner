"""Run: python3 -m unittest discover tests
Obsolete review page with fake Pipeline / S3 / SES."""
import io, json, os, sys, types, unittest, urllib.parse

CHAD = "cgracia@rainmakersecurities.com"
KEY = "test-admin-key"
OBSOLETE, INQUIRY, CONFIRM, HOLD, FIRM = 2348038, 2109142, 2388323, 2094373, 111800
LAST_CONFIRMED = "custom_label_4012491"


class FakeS3:
    def __init__(self, objects, fail_keys=()):
        self.objects, self.fail_keys = objects, set(fail_keys)

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise Exception(f"An error occurred (NoSuchKey): {Key}")
        return {"Body": io.BytesIO(json.dumps(self.objects[(Bucket, Key)]).encode())}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        if Key in self.fail_keys:
            raise Exception("AccessDenied")
        self.objects[(Bucket, Key)] = json.loads(Body)


class FakeSES:
    def __init__(self):
        self.sent = []

    def send_email(self, **kw):
        self.sent.append(kw)


def mkdeal(did, stage=OBSOLETE, sell=False, name=None, company="Acme", company_id=10, contact_id=500,
           updated="2026/09/23 10:00:00 -0500"):
    return {"id": did, "name": name or f"{company}: Deal {did}", "deal_stage": {"id": stage},
            "updated_at": updated, "created_at": "2025/01/02 09:00:00 -0500",
            "company": {"id": company_id, "name": company},
            "primary_contact": {"id": contact_id, "full_name": f"Client {did}", "email": f"c{did}@x.com"},
            "custom_fields": {"custom_label_1958": [5011675 if sell else 5077819],
                              "custom_label_3064360": [6250090], "custom_label_3064339": 10.5,
                              "custom_label_3064369": 9.5, "custom_label_3065488": 100000,
                              "custom_label_3064645": 500000}}


class FakePipeline:
    def __init__(self, deals, put_status=200):
        self.deals = {d["id"]: d for d in deals}
        self.calls = []
        self.put_status = put_status

    def __call__(self, method, endpoint, payload=None, jwt=None):
        self.calls.append((method, endpoint, payload))
        if method == "GET" and endpoint.startswith("/deals.json?"):
            q = urllib.parse.parse_qs(endpoint.split("?", 1)[1])
            assert "conditions[deal_stage]" in q, endpoint
            stage = int(q["conditions[deal_stage]"][0])
            page, per = int(q["page"][0]), int(q["per_page"][0])
            rows = [d for d in self.deals.values() if d["deal_stage"]["id"] == stage]
            return {"status": 200, "data": {"entries": rows[(page - 1) * per: page * per],
                                            "pagination": {"total": len(rows)}}}
        if method == "GET" and endpoint.startswith("/deals/"):
            did = int(endpoint.split("/")[2].split(".")[0])
            return {"status": 200, "data": json.loads(json.dumps(self.deals[did]))} if did in self.deals \
                else {"status": 404, "data": "nf"}
        if method == "PUT":
            return {"status": self.put_status, "data": {}}
        raise AssertionError(f"unexpected call {method} {endpoint}")

    @property
    def puts(self):
        return [c for c in self.calls if c[0] == "PUT"]


class ReviewBase(unittest.TestCase):
    admin_key = KEY
    fail_keys = ()

    def build(self, deals, snapshot=None, put_status=200):
        self.s3 = FakeS3({("full-pipeline-cache", "deals.json"): {"deals": snapshot or []},
                          ("pipeline-token", "pipeline-jwt.json"): {"jwt": "x"}}, self.fail_keys)
        self.ses = FakeSES()
        s3, ses = self.s3, self.ses

        def client(name, **kw):
            return {"s3": s3, "ses": ses}[name]

        sys.modules["boto3"] = types.SimpleNamespace(client=client)
        os.environ.update({"ADMIN_KEY": self.admin_key, "SCANNER_URL": "https://scan.example/",
                           "HMAC_SECRET": "t", "DRY_RUN": "false", "TEST_EMAIL_OVERRIDE": ""})
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        sys.modules.pop("lambda_function", None)
        import lambda_function as lf
        self.lf = lf
        self.pl = FakePipeline(deals, put_status)
        lf.call_pipeline_api = self.pl
        lf.time = types.SimpleNamespace(sleep=lambda s: None)

    def web(self, method, params):
        ev = {"requestContext": {"http": {"method": method}}}
        if method == "GET":
            ev["queryStringParameters"] = params
        else:
            ev["queryStringParameters"] = {}
            ev["body"] = urllib.parse.urlencode(params)
        return self.lf.lambda_handler(ev, None)

    def apply(self, ids, stage="inquiry", kept=""):
        return self.web("POST", {"action": "review_apply", "key": KEY, "stage": stage,
                                 "ids": ",".join(str(i) for i in ids), "kept": kept})

    def log(self):
        return self.s3.objects.get(("full-pipeline-cache", "obsolete-review/log.json"), {}).get("entries", [])

    def assert_only_chad_emailed(self):
        for m in self.ses.sent:
            self.assertEqual(m["Destination"]["ToAddresses"], [CHAD])


class ReviewTests(ReviewBase):
    def test_get_makes_zero_puts_and_flags(self):
        deals = [mkdeal(1), mkdeal(2, sell=True, company="Anthropic", company_id=20),
                 mkdeal(3, stage=FIRM), mkdeal(4, contact_id=777)]
        snapshot = [mkdeal(90, stage=INQUIRY, contact_id=777)]   # live twin of deal 4
        self.build(deals, snapshot)
        r = self.web("GET", {"action": "review", "key": KEY})
        self.assertEqual(self.pl.puts, [])
        body = r["body"]
        data = json.loads(body.split("const ROWS = ", 1)[1].split(";\n", 1)[0])
        self.assertEqual(sorted(x["id"] for x in data), [1, 2, 4])          # Firm deal not listed
        by = {x["id"]: x for x in data}
        self.assertTrue(by[4]["dup"])
        self.assertFalse(by[1]["dup"])
        self.assertTrue(by[2]["anthropic_sell"])
        self.assertEqual(by[2]["price"], "$9.50")                           # net for Sell
        self.assertEqual(by[1]["price"], "$10.50")                          # gross for Buy
        list_calls = [c[1] for c in self.pl.calls if c[1].startswith("/deals.json")]
        self.assertTrue(all("conditions[deal_stage]=2348038" in u and "deal_stage][]" not in u for u in list_calls))
        self.assertEqual(self.ses.sent, [])

    def test_wrong_or_missing_key_refused(self):
        self.build([mkdeal(1)])
        for params in ({"action": "review", "key": "nope"}, {"action": "review"}):
            self.assertIn("Nothing here.", self.web("GET", params)["body"])
        r = self.web("POST", {"action": "review_apply", "key": "nope", "ids": "1"})
        self.assertIn("Nothing here.", r["body"])
        self.assertEqual(self.pl.calls, [])

    def test_apply_three(self):
        self.build([mkdeal(1), mkdeal(2), mkdeal(3)])
        self.apply([1, 2, 3])
        self.assertEqual(len(self.pl.puts), 3)
        for _, ep, payload in self.pl.puts:
            self.assertEqual(set(payload), {"deal"})
            self.assertEqual(set(payload["deal"]), {"deal_stage_id", "custom_fields"})
            self.assertEqual(payload["deal"]["deal_stage_id"], INQUIRY)
            self.assertEqual(payload["deal"]["custom_fields"], {LAST_CONFIRMED: "2026-09-23"})
        self.assertEqual([e["action"] for e in self.log()], ["restored"] * 3)
        self.assertEqual(len(self.ses.sent), 1)
        self.assert_only_chad_emailed()

    def test_confirm_stage_and_anthropic_sell_goes_to_hold(self):
        self.build([mkdeal(1), mkdeal(2, sell=True, company="Anthropic", name="Anthropic: $5M Sell")])
        self.apply([1, 2], stage="confirm")
        stages = {int(ep.split("/")[2].split(".")[0]): p["deal"]["deal_stage_id"] for _, ep, p in self.pl.puts}
        self.assertEqual(stages, {1: CONFIRM, 2: HOLD})

    def test_no_longer_obsolete_is_skipped(self):
        self.build([mkdeal(1), mkdeal(2, stage=FIRM)])
        r = self.apply([1, 2])
        self.assertEqual([ep for _, ep, _ in self.pl.puts], ["/deals/1.json"])
        self.assertIn("no longer Obsolete", r["body"])

    def test_sixty_ticked_gives_fifty_and_continue_ten(self):
        self.build([mkdeal(i) for i in range(1, 61)])
        r = self.apply(list(range(1, 61)))
        self.assertEqual(len(self.pl.puts), 50)
        body = r["body"]
        self.assertIn("Continue — next 10", body)
        rest = body.split('name="ids" value="', 1)[1].split('"', 1)[0]
        self.assertEqual(rest.split(","), [str(i) for i in range(51, 61)])
        self.assert_only_chad_emailed()

    def test_kept_rows_logged(self):
        self.build([mkdeal(1), mkdeal(2), mkdeal(3)])
        self.apply([1], kept="2,3")
        acts = sorted((e["deal_id"], e["action"]) for e in self.log())
        self.assertEqual(acts, [(1, "restored"), (2, "kept"), (3, "kept")])
        self.assertEqual(len(self.pl.puts), 1)

    def test_undo(self):
        self.build([mkdeal(1)])
        self.apply([1])
        self.pl.deals[1]["deal_stage"]["id"] = INQUIRY          # Pipeline now shows it restored
        r = self.web("POST", {"action": "review_undo", "key": KEY, "deal_id": "1"})
        self.assertEqual(self.pl.puts[-1][2], {"deal": {"deal_stage_id": OBSOLETE}})
        self.assertIn("moved back to Obsolete", r["body"])
        self.assertEqual(self.log()[-1]["action"], "undone")
        # A deal the log never restored cannot be undone.
        n = len(self.pl.puts)
        self.web("POST", {"action": "review_undo", "key": KEY, "deal_id": "999"})
        self.assertEqual(len(self.pl.puts), n)

    def test_report_links_review_page(self):
        self.build([mkdeal(1, stage=FIRM, updated="2024/01/01 10:00:00 -0500")])
        self.s3.objects[("full-pipeline-cache", "deals.json")] = {"deals": [mkdeal(1, stage=FIRM, updated="2024/01/01 10:00:00 -0500")]}
        self.lf.lambda_handler({}, None)
        self.assertEqual(len(self.ses.sent), 1)
        self.assert_only_chad_emailed()
        self.assertIn(f"https://scan.example/?action=review&key={KEY}", self.ses.sent[0]["Message"]["Body"]["Text"]["Data"])
        self.assertEqual(self.pl.puts, [])


class LogSaveFails(ReviewBase):
    fail_keys = ("obsolete-review/log.json",)

    def test_failed_log_save_means_zero_puts(self):
        self.build([mkdeal(1), mkdeal(2), mkdeal(3)])
        r = self.apply([1, 2, 3], kept="")
        self.assertEqual(self.pl.puts, [])
        self.assertIn("Could not save the review log", r["body"])
        self.assertEqual(self.ses.sent, [])


class BlankAdminKey(ReviewBase):
    admin_key = ""

    def test_blank_admin_key_refuses_everything(self):
        self.build([mkdeal(1)])
        for params in ({"action": "review", "key": ""}, {"action": "review", "key": KEY}):
            self.assertIn("Nothing here.", self.web("GET", params)["body"])
        self.assertIn("Nothing here.", self.web("POST", {"action": "review_apply", "key": "", "ids": "1"})["body"])
        self.assertEqual(self.pl.calls, [])


if __name__ == "__main__":
    unittest.main()

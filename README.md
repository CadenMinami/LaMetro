# LA Metro Reliability Platform

Real-time LA Metro transit reliability platform built on AWS. Ingests live GTFS-Realtime feeds, computes on-time performance, predicts delays with an ML model, and analyzes neighborhood-level reliability disparities. The live pipeline (ingest → Kinesis → enrichment → DynamoDB → SageMaker) and dashboard are deployed; Phases 1–8 are built (Phase 9 polish in progress).

## Architecture (target state)

GTFS-RT → Kinesis → Lambda (enrichment) → DynamoDB (hot state) + Firehose → S3 → Athena / SageMaker. Live frontend over API Gateway WebSockets, hosted via CloudFront. See `claude.md` for the full spec.

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Data flowing — Lambda fetches GTFS-RT every 60s | ✅ deployed |
| 2 | Storage tier — Kinesis + DynamoDB + S3 archive | ✅ deployed |
| 3 | Frontend MVP — Next.js + ArcGIS map | ✅ deployed |
| 4 | Schedule deviation algorithm | ✅ deployed |
| 5 | Real-time WebSocket push | ✅ deployed |
| 6 | Cognito auth + SNS geofence alerts | ✅ deployed |
| 7 | SageMaker delay predictor + Step Functions retraining | ✅ deployed |
| 8 | Equity analysis (ArcGIS Living Atlas + census) | ✅ built (deploy pending) |
| 9 | Polish, demo, README, cost report | in progress |

## Equity finding (Phase 8)

**Question:** do LA Metro buses serving lower-income neighborhoods run less reliably than those serving wealthier ones?

**Answer: no — and that's the finding.** Joining ~4 weeks of per-route on-time performance to ACS median household income (ArcGIS Living Atlas) across all ~2,495 LA County census tracts, there is **no statistically significant relationship** between neighborhood income and reliability (Pearson r = −0.17, p = 0.07; n = 108 routes). LA Metro is *uniformly* unreliable — ~26% on-time within ±60s — regardless of income. The faint, non-significant trend actually runs **opposite** the usual equity narrative (denser, higher-income job corridors trend slightly *less* reliable), consistent with congestion rather than neighborhood income driving delay. The unreliability is **system-wide, not an income gap**.

**The more interesting story is how that answer held up.** An intermediate run looked significant (r = −0.26, p = 0.009) — but only because the ArcGIS feature service silently capped responses, so the join used just 1,769 of 2,495 tracts. Adding stable-sorted pagination to pull *all* tracts weakened the effect back to non-significant. A result that gets *less* impressive as the data gets *more* complete is exactly the false positive you want to catch before publishing. See [`docs/PHASE_8_EQUITY_FINDING.md`](docs/PHASE_8_EQUITY_FINDING.md); reproduce with `ml/equity_analysis.py`.

## Local development

Prereqs: Node 20+, Python 3.12+, AWS CLI v2 with credentials configured (region `us-west-2`), AWS CDK v2.

```bash
# Run unit tests
pytest lambdas/

# Synth, diff, or deploy — wrapper sources .env automatically
scripts/cdk synth
scripts/cdk diff
scripts/cdk deploy LaMetro-IngestionStack

# Tail Lambda logs
aws logs tail /aws/lambda/la-metro-ingestion --region us-west-2 --follow

# Tear everything down (do this on long breaks to avoid idle costs)
scripts/cdk destroy --all
```

## Data source

Phase 1 is currently pointed at the **MTA NYC subway** GTFS-RT feed (public, no key) while we wait for Swiftly to approve API access for LA Metro. The handler is generic GTFS-RT — once Swiftly access lands, comment out `LA_METRO_FEED_URL` in `.env` and redeploy. The handler will fall back to its default (LA Metro Swiftly endpoint) and use the `LA_METRO_API_KEY` automatically.

## Cost discipline

Hard cap: $30/month during active development, $15/month idle. CloudWatch billing alarm at $20, AWS Budget at $30. All resources tagged `Project=la-metro` for Cost Explorer filtering. `cdk destroy` everything during long breaks.

## Future work

### Phase 6 follow-ups
- **Email alerts:** the per-user "email me" toggle is built and persisted; wiring
  it to SES (or SNS→Lambda→SES) is the remaining step. Deferred to avoid the SES
  sandbox during the build.
- **Per-stop directional geofences:** geofences currently fire on a route's
  average delay. The `stop_id` field is reserved so alerts can later target the
  next vehicle approaching a specific stop in the user's direction.
- **Real-time alert push:** notifications are polled every 60s; they could ride
  the existing WebSocket for instant delivery once the socket is authenticated.

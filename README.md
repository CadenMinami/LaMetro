# LA Metro Reliability Platform

Real-time LA Metro transit reliability platform built on AWS. Ingests live GTFS-Realtime feeds, computes on-time performance, predicts delays with an ML model, and exposes neighborhood-level reliability disparities. Currently in Phase 1 (data ingestion).

## Architecture (target state)

GTFS-RT → Kinesis → Lambda (enrichment) → DynamoDB (hot state) + Firehose → S3 → Athena / SageMaker. Live frontend over API Gateway WebSockets, hosted via CloudFront. See `claude.md` for the full spec.

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Data flowing — Lambda fetches GTFS-RT every 60s | ✅ deployed |
| 2 | Storage tier — Kinesis + DynamoDB + S3 archive | pending |
| 3 | Frontend MVP — Next.js + ArcGIS map | pending |
| 4 | Schedule deviation algorithm | pending |
| 5 | Real-time WebSocket push | pending |
| 6 | Cognito auth + SNS geofence alerts | pending |
| 7 | SageMaker delay predictor + Step Functions retraining | pending |
| 8 | Equity analysis (ArcGIS Living Atlas + census) | pending |
| 9 | Polish, demo, README, cost report | pending |

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

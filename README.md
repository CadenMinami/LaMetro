# LA Metro Reliability Platform

A real-time, AWS-native platform that ingests live LA Metro GTFS-Realtime feeds, computes and **predicts** bus/rail reliability, and surfaces neighborhood-level reliability through a live ArcGIS map. Built end-to-end across nine phases — streaming, serverless, multi-tier storage, IaC, observability, ML deployment, and a geospatial equity analysis.

**Live demo:** https://d1trwh3zs290xm.cloudfront.net · **Equity map:** https://d1trwh3zs290xm.cloudfront.net/equity/ · _Demo video: coming soon_

> Every architectural decision was made deliberately, weighing the rationale and tradeoffs against the alternatives.

## The problem

Transit riders don't care about average performance — they care whether *their* bus is late *right now*, and over time, which routes they can rely on. This platform answers both: a live map of every LA Metro vehicle colored by delay, per-route on-time trends, an ML-predicted "next-window" delay, and geofenced alerts when a user's route slips.

## Architecture

GTFS-RT → Kinesis → enrichment Lambda (schedule deviation) → DynamoDB hot state + Firehose → S3 → Athena / SageMaker. A nightly Step Functions pipeline retrains an XGBoost delay model; a Serverless Inference endpoint backs precomputed per-route predictions. The Next.js + ArcGIS frontend is served from CloudFront and pushed live updates over API Gateway WebSockets.

### Tech stack
- **AWS:** Kinesis, Lambda, DynamoDB, Firehose, S3, Athena/Glue, SageMaker (Serverless Inference), Step Functions, EventBridge, API Gateway (REST + WebSocket), Cognito, SNS, CloudFront, CloudWatch — all via **CDK v2 (TypeScript)**.
- **Backend:** Python 3.12 Lambdas (`gtfs-realtime-bindings`, `boto3`, `shapely`).
- **Frontend:** Next.js 14 (App Router, static export), TypeScript, ArcGIS Maps SDK for JavaScript, Tailwind.
- **ML / analysis:** XGBoost; geopandas + ArcGIS Python API for the equity join.

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
| 8 | Equity analysis (ArcGIS Living Atlas + census) | ✅ deployed |
| 9 | Polish — diagram, demo, cost report | in progress |

## Equity finding (Phase 8)

**Question:** do LA Metro buses serving lower-income neighborhoods run less reliably than those serving wealthier ones?

**Answer: no — and that's the finding.** Joining ~4 weeks of per-route on-time performance to ACS median household income (ArcGIS Living Atlas) across all ~2,495 LA County census tracts, there is **no statistically significant relationship** between neighborhood income and reliability (Pearson r = −0.17, p = 0.07; n = 108 routes). LA Metro is *uniformly* unreliable — ~26% on-time within ±60s — regardless of income. The faint, non-significant trend runs **opposite** the usual equity narrative (denser, higher-income job corridors trend slightly *less* reliable), consistent with congestion rather than income driving delay. The unreliability is **system-wide, not an income gap**.

**The more interesting story is how that answer held up.** An intermediate run looked significant (r = −0.26, p = 0.009) — but only because the ArcGIS feature service silently capped responses, so the join used just 1,769 of 2,495 tracts. Adding stable-sorted pagination to pull *all* tracts weakened the effect back to non-significant. A result that gets *less* impressive as the data gets *more* complete is exactly the false positive you want to catch before publishing. Reproduce with `ml/equity_analysis.py`.

## Cost discipline

Hard cap: **$30/month** during active development, **$15/month** idle. Net billed to date is **~$0** (Free Tier + credits); on list price, making ingestion **scale to zero** cut the dominant cost (DynamoDB) by **94% — $56→$3/mo**, leaving the single Kinesis shard (~$11/mo) as the idle floor. Cost-control choices are baked in: one Kinesis shard, DynamoDB on-demand, **SageMaker Serverless** inference ($0.06/mo), and scale-to-zero ingestion. `cdk destroy` everything during long breaks.

## Local development

Prereqs: Node 20+, Python 3.12+, AWS CLI v2 (region `us-west-2`), AWS CDK v2; Docker only for the training container Lambda.

```bash
# Unit tests (Lambdas + ML)
pytest

# Synth / diff / deploy (wrapper sources .env)
scripts/cdk synth
scripts/cdk deploy LaMetro-StorageStack LaMetro-MLStack LaMetro-ApiStack

# Tail a Lambda
aws logs tail /aws/lambda/la-metro-ingestion --region us-west-2 --follow

# Equity analysis (needs ml/.venv-equity + ArcGIS creds in ml/.env-equity)
ml/.venv-equity/bin/python ml/equity_analysis.py --bucket <archive-bucket>

# Tear down on long breaks to avoid idle cost
scripts/cdk destroy --all
```

The ingestion handler is generic GTFS-RT (LA Metro via Swiftly by default), so the pipeline can be pointed at any agency's feed by changing one env var.

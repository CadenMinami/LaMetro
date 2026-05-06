# LA Metro Reliability Platform — Project Spec

## What we're building

A real-time LA Metro transit reliability platform. It ingests live GTFS-Realtime feeds from LA Metro every 30 seconds, computes on-time performance, predicts arrival delays with an ML model, surfaces transit equity disparities across LA neighborhoods, and serves a live ArcGIS-based map dashboard. Users can sign up and get geofenced alerts when their regular bus or train is running late.

The architecture is intentionally AWS-native using flagship services (Kinesis, Lambda, DynamoDB, SageMaker, API Gateway WebSockets, CDK, CloudWatch). Every choice should be defensible — if there's a simpler way to do something that loses architectural signal, prefer the AWS-native option.

## Goals

1. **Working product:** a live dashboard at a custom domain showing real LA Metro vehicles updating in real time with delay information.
2. **Engineering depth:** demonstrate streaming, serverless, IaC, observability, ML deployment, and multi-tier storage on AWS.
3. **A finding:** correlate transit reliability with neighborhood demographics from ArcGIS Living Atlas to produce a memorable insight, not just a feature list.
4. **Cost discipline:** total monthly AWS cost during development under $15. Idle cost when not actively iterating under $5.

## Non-goals

- Not a generic transit app. No trip planning, fare payment, or route search — Google Maps and the LA Metro app already do that.
- Not multi-city. LA Metro only. Adding more agencies is a v2 problem.
- Not mobile-native. Responsive web app is enough.
- Not a custom-trained model on satellite imagery or anything exotic. The ML is XGBoost on tabular features.

## Tech stack

### AWS services
- **Kinesis Data Streams** — primary event bus for vehicle position events
- **Lambda** — all compute (ingestion, enrichment, aggregation, alerts, API)
- **DynamoDB** — hot state (current positions, route aggregates, geofences, users)
- **Kinesis Data Firehose** — Kinesis → S3 archival
- **S3** — cold storage of all historical events, partitioned for Athena
- **Athena** — historical analytics queries over S3
- **SageMaker** — XGBoost training jobs + Serverless Inference for delay prediction
- **Step Functions** — orchestrates retraining pipeline
- **EventBridge Scheduler** — triggers ingestion Lambda every 30s and weekly training
- **API Gateway** — REST API for dashboard data + WebSockets for live updates
- **Cognito** — user auth for personalized features
- **SNS** — push/email alerts on geofence triggers
- **CloudWatch** — logs, metrics, dashboards, alarms
- **X-Ray** — distributed tracing across the Lambda chain
- **CloudFront + S3** — frontend hosting

### Application stack
- **Backend:** Python 3.12 for all Lambdas. Uses `gtfs-realtime-bindings`, `boto3`, `pydantic` for validation.
- **Frontend:** Next.js 14 (App Router), TypeScript, ArcGIS API for JavaScript v4, Tailwind, shadcn/ui.
- **IaC:** AWS CDK v2 in TypeScript. Single CDK app, separate stacks per logical layer (`IngestionStack`, `BillingStack`, `StorageStack`, `MLStack`, `ApiStack`, `FrontendStack`).
- **CI/CD:** GitHub Actions. Tests on every PR, deploy to staging on merge to main, manual promote to prod via workflow_dispatch.
- **Testing:** pytest for Lambdas with `moto` for AWS mocking. Playwright for frontend smoke tests.

## Architecture

### Data flow

```
LA Metro GTFS-RT feed
        │ (every 30s, EventBridge Scheduler)
        ▼
[Ingestion Lambda]
        │ parses protobuf, emits one event per vehicle
        ▼
[Kinesis Data Stream: vehicle-positions]
        │
        ├──► [Enrichment Lambda] ──► [DynamoDB: hot-vehicles]
        │         │ snap to route, compute delay
        │         ▼
        │    [DynamoDB: aggregates]  (per-route rolling stats, written by Aggregation Lambda)
        │
        └──► [Kinesis Firehose] ──► [S3: cold archive]
                                          │
                                          └──► [Athena] (historical queries)
                                          └──► [SageMaker training] (weekly via Step Functions)
                                                    │
                                                    └──► [SageMaker Serverless Inference endpoint]

[Aggregation Lambda] (every 60s)
        │ reads hot-vehicles, computes route-level metrics, checks geofences
        ├──► [DynamoDB: aggregates]
        ├──► [API Gateway WebSockets] (push to connected clients)
        └──► [SNS] (geofence breach alerts)

[User] ──► [CloudFront] ──► [S3: Next.js static export]
   │
   ├──► [API Gateway REST] ──► [Query Lambda] ──► [DynamoDB]
   ├──► [API Gateway WebSockets] ◄── live position updates
   └──► [Cognito] for auth
```

### DynamoDB tables

**`hot-vehicles`**
- PK: `geohash` (precision 6, ~1.2km cells) — partition key for spatial queries
- SK: `vehicle_id`
- Attrs: `lat`, `lon`, `route_id`, `trip_id`, `delay_seconds`, `bearing`, `speed_mps`, `last_updated`
- TTL: 1 hour on `last_updated`
- GSI: `route_id` → `last_updated` (for "all vehicles on route X")

**`route-aggregates`**
- PK: `route_id`
- SK: `window_start_iso` (5-minute buckets)
- Attrs: `avg_delay_seconds`, `on_time_pct`, `vehicle_count`, `p95_delay_seconds`
- TTL: 7 days

**`geofences`**
- PK: `user_id`
- SK: `geofence_id`
- Attrs: `route_id`, `stop_id`, `threshold_seconds`, `notification_channel`, `enabled`
- GSI: `route_id` → `user_id` (for "who cares about this route?")

**`users`**
- PK: `user_id` (Cognito sub)
- Attrs: `email`, `created_at`, `home_routes` (list)

**`websocket-connections`**
- PK: `connection_id`
- Attrs: `user_id`, `subscribed_routes` (list), `connected_at`
- TTL: 2 hours

### S3 bucket structure

```
s3://la-metro-archive-{env}/
  raw-events/
    year=2026/month=05/day=03/hour=14/
      la-metro-stream-1-2026-05-03-14-30-00-{uuid}.gz
  processed-features/
    year=2026/month=05/day=03/
      route-day-features.parquet
  models/
    delay-predictor/
      v=2026-05-03-weekly/
        model.tar.gz
        metrics.json
```

### Schedule deviation logic

The trickiest piece of pure algorithm work. For each vehicle position event:

1. Look up the trip's scheduled stop times from GTFS static data (cached in Lambda memory + S3)
2. Find the two scheduled stops the vehicle is between, by interpolating along the route's shape geometry
3. Compute scheduled progress: linear interpolation of `(distance_traveled / total_route_distance)` based on current time vs. the schedule
4. Compute actual progress: how far along the route shape the current GPS point is (Shapely or `pyproj`)
5. Delay = (actual_position_in_time_terms) − (scheduled_position_in_time_terms)

Edge cases:
- Vehicle is off-route (deviation from shape > 200m) → mark as "off-route" and skip delay calc
- Trip hasn't started yet or already ended → skip
- GTFS static data version mismatch → fall back to last known good version

## Build sequence

Each phase ends with a working, demoable artifact — no phase exceeds a week without something runnable.

### Phase 1 — Data flowing (Week 1)
- AWS account, IAM admin user (separate from root), AWS CLI configured
- Initialize CDK app, create `IngestionStack` with single Lambda + EventBridge schedule
- Register for LA Metro / Swiftly developer portal API key, store in AWS Secrets Manager
- Lambda fetches GTFS-RT vehicle positions feed, parses protobuf, logs vehicle count
- **Demo:** CloudWatch logs showing vehicle positions parsed every minute

### Phase 2 — Storage tier (Week 2)
- Add Kinesis Data Stream to CDK
- Ingestion Lambda writes to Kinesis instead of just logging
- Add Enrichment Lambda triggered by Kinesis, writes to `hot-vehicles` DynamoDB table
- Add Kinesis Firehose → S3 with Parquet conversion + hourly partitioning
- Add a one-off query Lambda exposed via API Gateway: `GET /vehicles?bbox=lon1,lat1,lon2,lat2`
- **Demo:** curl the API, get JSON list of currently-active vehicles in a bounding box

### Phase 3 — Frontend MVP (Week 3)
- Initialize Next.js app, set up ArcGIS API for JavaScript
- Static map of LA centered on downtown, base layer from Esri
- Fetch vehicles from API every 30s, render as pins; color by route
- Deploy to S3 + CloudFront via CDK `FrontendStack`
- **Demo:** live URL showing real LA Metro vehicles moving on the map

### Phase 4 — Schedule deviation (Week 4)
- Download GTFS static feed, store in S3, write loader that pulls into Lambda layer
- Implement schedule deviation algorithm in enrichment Lambda
- Add `delay_seconds` to `hot-vehicles` table
- Add Aggregation Lambda on EventBridge 1-min schedule, writes to `route-aggregates`
- Frontend: color vehicle pins by delay (green <1min, yellow 1-3min, orange 3-5min, red >5min)
- Add route detail page: `/routes/[routeId]` showing on-time % over time as a chart
- **Demo:** map shows colored pins; click a route → see its on-time % over the day

### Phase 5 — Real-time push (Week 5)
- Add API Gateway WebSockets stack
- Connection Lambda writes to `websocket-connections`, subscribes user to routes
- Aggregation Lambda fans out updates over WebSocket to subscribed connections
- Frontend replaces 30s polling with WebSocket subscription
- **Demo:** map updates feel instant; vehicles glide instead of teleporting

### Phase 6 — Auth + alerts (Week 6)
- Cognito user pool, hosted UI for sign-up/sign-in
- Frontend integrates Cognito (Amplify auth library or `aws-jwt-verify`)
- "My Routes" page where users add geofences ("alert me if 720 to UCLA is >5min late")
- Aggregation Lambda checks geofences on each update; on breach, publishes to SNS
- SNS topic with email subscription
- **Demo:** sign up, add a geofence, force a delay, receive email

### Phase 7 — ML pipeline (Weeks 7-8)
- Step Functions state machine: weekly trigger via EventBridge
- State 1: Athena query extracts last 30 days of position events into a Parquet feature table in S3
- State 2: SageMaker training job (XGBoost built-in) trains delay predictor with features `[route_id, hour_of_day, day_of_week, current_temp, recent_route_avg_delay, upstream_stop_delay]`
- State 3: Register new model version, evaluate against held-out test set, only deploy if MAE improves
- State 4: Deploy to SageMaker Serverless Inference endpoint
- Inference Lambda calls endpoint when frontend requests prediction for a stop
- Frontend: route detail page shows "predicted arrival in 7 min (typical 5 min)" for upcoming stops
- **Demo:** prediction shown on frontend; README documents MAE on holdout

### Phase 8 — Equity analysis (Week 9)
- Use ArcGIS Python API to load Living Atlas median income by census tract for LA County
- One-time Athena job: join 90 days of route on-time stats with census tracts the routes pass through
- Frontend: "Equity" page with map overlay of median income + route reliability heatmap
- **Demo:** equity page with a documented headline finding

### Phase 9 — Polish (Week 10)
- Architecture diagram (Excalidraw or draw.io)
- Demo video (Loom, 3-5 min)
- README with: problem, architecture, tradeoffs, finding, cost breakdown, future work
- CloudWatch dashboard publicly viewable (or screenshots in README)

## Code organization

```
la-metro-reliability/
├── cdk/                          # CDK app
│   ├── bin/cdk.ts
│   ├── lib/
│   │   ├── ingestion-stack.ts
│   │   ├── billing-stack.ts
│   │   ├── storage-stack.ts
│   │   ├── ml-stack.ts
│   │   ├── api-stack.ts
│   │   └── frontend-stack.ts
│   ├── package.json
│   └── tsconfig.json
├── lambdas/                      # one folder per Lambda
│   ├── ingestion/
│   │   ├── handler.py
│   │   ├── requirements.txt
│   │   └── tests/test_handler.py
│   ├── enrichment/
│   ├── aggregation/
│   ├── query-api/
│   ├── websocket-connect/
│   ├── websocket-disconnect/
│   ├── inference/
│   └── shared/                   # shared layer: GTFS parsing, schedule deviation, geohash utils
├── ml/
│   ├── feature_extraction.sql    # Athena query for training features
│   ├── train.py                  # SageMaker training script
│   └── evaluate.py
├── frontend/                     # Next.js app
│   ├── app/
│   ├── components/
│   ├── lib/
│   └── package.json
├── docs/
│   ├── PROJECT_SPEC.md           # this file
│   ├── ARCHITECTURE.md
│   ├── TRADEOFFS.md
│   └── COST.md
├── .github/workflows/
│   ├── pr-checks.yml
│   ├── deploy-staging.yml
│   └── deploy-prod.yml
└── README.md
```

## Cost controls

1. **CloudWatch billing alarms at $5, $10, $15** (codified in `BillingStack`, deployed to us-east-1 where billing metrics live)
2. **AWS Budget** at $15/month with 80% actual + 100% forecasted email notifications
3. **Cost allocation tags on every resource** via CDK: `Project=la-metro`, `Environment=dev|staging|prod`, `ManagedBy=cdk`
4. **Never deploy SageMaker real-time endpoints.** Only Serverless Inference. A real-time `ml.m5.large` endpoint left running is ~$70/month.
5. **DynamoDB on-demand only.**
6. **Single Kinesis shard** — 1MB/s and 1000 records/s is plenty for one transit agency.
7. **Lambda memory: start at 512MB**, increase only when measurably slow.
8. **`cdk destroy` between active development gaps of 1+ week.** The Kinesis shard alone is $11/month idling.

## Tradeoffs

These are interview-defensible choices. The *why* is documented because the alternatives are not obviously wrong.

1. **Kinesis vs SQS vs direct DynamoDB writes.** Kinesis chosen because: multiple consumers (DDB write + Firehose archival + future ML feature pipeline), ordered processing per shard, replay capability. SQS would lose ordering and replay. Direct DDB would couple consumers tightly.

2. **DynamoDB vs RDS for hot state.** DDB chosen because: single-digit ms reads at any scale, no connection pooling pain in Lambda, geohash partition keys map cleanly to spatial query patterns, on-demand billing matches bursty load. RDS would mean RDS Proxy + connection mgmt + idle costs.

3. **XGBoost vs LSTM for delay prediction.** XGBoost chosen because: tabular features, faster training (~2 min vs hours), better on small data (30 days), interpretable feature importance. LSTM would overfit and be expensive to retrain.

4. **SageMaker Serverless Inference vs always-on endpoint.** Serverless chosen because: portfolio-scale traffic is bursty and low volume, cold start <2s is acceptable for non-blocking UX, ~$3/month vs $70/month.

5. **Geohash partition key vs DynamoDB GSI on lat/lon.** Geohash chosen because: native bounding-box queries via prefix scans, hot partitions avoided by precision tuning. GSI on lat/lon doesn't natively support 2D range queries.

6. **API Gateway WebSockets vs AppSync subscriptions.** WebSockets chosen because: simpler Lambda integration, no GraphQL learning tax, lower per-message cost. AppSync is the right call at production scale with many client types.

7. **CDK vs Terraform vs SAM.** CDK chosen because: typed (TypeScript) infrastructure, native AWS-first abstractions, single language for backend Lambdas in Python while infra is TS. Terraform would work fine but is more verbose for AWS-only stacks.

8. **Weekly retraining vs nightly.** Weekly chosen because: transit-pattern drift is slow (schedules change quarterly, traffic patterns shift week-over-week), nightly retraining 7× the SageMaker cost without measurable accuracy improvement.

9. **AWS Secrets Manager vs SSM Parameter Store for API keys.** Secrets Manager chosen because: native rotation support, fine-grained IAM per-secret, automatic encryption with separate KMS key. SSM is fine for plain config but lacks first-class rotation.

## What "done" looks like

- Live URL (custom domain) showing real-time LA Metro vehicles
- Public CloudWatch dashboard with pipeline metrics
- README with: problem statement, architecture diagram, tradeoffs, equity finding, cost report, future work, demo video link
- GitHub repo with CI/CD passing, tests passing, IaC clean
- Total monthly cost report showing <$15/month during active development

## Anti-goals (intentionally out of scope)

- **Uber/Lyft data integration** — scope creep; v2.
- **Rust for Lambdas** — Python is fine; Rust would slow development without proportional gains here.
- **Transformer for delay prediction** — XGBoost wins on tabular data at this size.
- **Multi-agency support** — LA Metro only. Architecture should *allow* extension but doesn't implement it.
- **Native mobile app** — responsive web is enough.
- **Aurora instead of DynamoDB** — see tradeoffs.

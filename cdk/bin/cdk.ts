#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { IngestionStack } from '../lib/ingestion-stack';
import { BillingStack } from '../lib/billing-stack';
import { StorageStack } from '../lib/storage-stack';
import { ProcessingStack } from '../lib/processing-stack';
import { ApiStack } from '../lib/api-stack';
import { FrontendStack } from '../lib/frontend-stack';

const app = new cdk.App();

const account = process.env.CDK_DEFAULT_ACCOUNT;
const env = {
  account,
  region: process.env.CDK_DEFAULT_REGION ?? 'us-west-2',
};

const tags = {
  Project: 'la-metro',
  Environment: app.node.tryGetContext('environment') ?? 'dev',
  ManagedBy: 'cdk',
};

const storage = new StorageStack(app, 'LaMetro-StorageStack', {
  env,
  description: 'Phase 2 storage: Kinesis stream, DynamoDB hot table, S3 archive.',
});

const ingestion = new IngestionStack(app, 'LaMetro-IngestionStack', {
  env,
  swiftlySecretName: process.env.SWIFTLY_SECRET_NAME ?? 'la-metro/swiftly-api-key',
  laMetroFeedUrl: process.env.LA_METRO_FEED_URL,
  vehicleStream: storage.vehicleStream,
  description: 'LA Metro GTFS-RT ingestion Lambda + EventBridge schedule.',
});

const processing = new ProcessingStack(app, 'LaMetro-ProcessingStack', {
  env,
  vehicleStream: storage.vehicleStream,
  hotVehiclesTable: storage.hotVehiclesTable,
  routeAggregatesTable: storage.routeAggregatesTable,
  description:
    'Processing: Enrichment Lambda (Kinesis → DynamoDB) + Aggregation Lambda (every 1m).',
});

const api = new ApiStack(app, 'LaMetro-ApiStack', {
  env,
  hotVehiclesTable: storage.hotVehiclesTable,
  description: 'Phase 2 read API: GET /vehicles?bbox=… backed by a Lambda + REST API Gateway.',
});

const frontend = new FrontendStack(app, 'LaMetro-FrontendStack', {
  env,
  description: 'Phase 3 frontend: S3 + CloudFront serving the Next.js static export.',
});

// Billing alarms must live in us-east-1 — that's the only region where AWS
// publishes the AWS/Billing EstimatedCharges metric.
const billing = new BillingStack(app, 'LaMetro-BillingStack', {
  env: { account, region: 'us-east-1' },
  alarmEmail: process.env.BILLING_ALERT_EMAIL ?? 'cadenminami@gmail.com',
  monthlyBudgetUsd: 15,
  alarmThresholdsUsd: [5, 10, 15],
  description: 'Cost guardrails: SNS-backed CloudWatch billing alarms + monthly Budget.',
});

for (const stack of [storage, ingestion, processing, api, frontend, billing]) {
  for (const [k, v] of Object.entries(tags)) {
    cdk.Tags.of(stack).add(k, v);
  }
}

#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { IngestionStack } from '../lib/ingestion-stack';
import { BillingStack } from '../lib/billing-stack';
import { StorageStack, WEBSOCKET_CONNECTIONS_TABLE_NAME } from '../lib/storage-stack';
import { ProcessingStack } from '../lib/processing-stack';
import { ApiStack } from '../lib/api-stack';
import { FrontendStack } from '../lib/frontend-stack';
import { WebSocketStack } from '../lib/websocket-stack';
import { AuthStack } from '../lib/auth-stack';
import { MLStack } from '../lib/ml-stack';

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
  connectionsTableName: WEBSOCKET_CONNECTIONS_TABLE_NAME,
  description: 'LA Metro GTFS-RT ingestion Lambda + EventBridge schedule.',
});

const auth = new AuthStack(app, 'LaMetro-AuthStack', {
  env,
  usersTable: storage.usersTable,
  description: 'Phase 6: Cognito user pool + PostConfirmation trigger.',
});

// WebSocketStack must be constructed *before* ProcessingStack so we can
// pass the callback URL + grantManageConnections() into the enrichment
// Lambda for Phase 5b fan-out.
const websocket = new WebSocketStack(app, 'LaMetro-WebSocketStack', {
  env,
  connectionsTable: storage.websocketConnectionsTable,
  description: 'Phase 5a: WebSocket API + connection manager Lambda.',
});

const processing = new ProcessingStack(app, 'LaMetro-ProcessingStack', {
  env,
  vehicleStream: storage.vehicleStream,
  hotVehiclesTable: storage.hotVehiclesTable,
  routeAggregatesTable: storage.routeAggregatesTable,
  archiveBucket: storage.archiveBucket,
  websocketConnectionsTable: storage.websocketConnectionsTable,
  websocketStack: websocket,
  geofencesTable: storage.geofencesTable,
  notificationsTable: storage.notificationsTable,
  description:
    'Processing: Enrichment (Kinesis → DDB + delay + WebSocket fan-out) + Aggregation (1m).',
});

const api = new ApiStack(app, 'LaMetro-ApiStack', {
  env,
  hotVehiclesTable: storage.hotVehiclesTable,
  routeAggregatesTable: storage.routeAggregatesTable,
  archiveBucket: storage.archiveBucket,
  userPool: auth.userPool,
  usersTable: storage.usersTable,
  geofencesTable: storage.geofencesTable,
  notificationsTable: storage.notificationsTable,
  description:
    'Read API (/vehicles, /routes, /stops) + Cognito-authorized user API (/geofences, /notifications, /me).',
});

const frontend = new FrontendStack(app, 'LaMetro-FrontendStack', {
  env,
  description: 'Phase 3 frontend: S3 + CloudFront serving the Next.js static export.',
});

const ml = new MLStack(app, 'LaMetro-MLStack', {
  env,
  routeAggregatesTable: storage.routeAggregatesTable,
  weatherCacheTable: storage.weatherCacheTable,
  archiveBucket: storage.archiveBucket,
  description: 'Phase 7a: feature-snapshot Lambda + Glue catalog (extended in 7b/7c).',
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

for (const stack of [storage, auth, ingestion, processing, api, frontend, websocket, billing, ml]) {
  for (const [k, v] of Object.entries(tags)) {
    cdk.Tags.of(stack).add(k, v);
  }
}

#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { IngestionStack } from '../lib/ingestion-stack';
import { BillingStack } from '../lib/billing-stack';

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

const ingestion = new IngestionStack(app, 'LaMetro-IngestionStack', {
  env,
  swiftlySecretName: process.env.SWIFTLY_SECRET_NAME ?? 'la-metro/swiftly-api-key',
  laMetroFeedUrl: process.env.LA_METRO_FEED_URL,
  description: 'LA Metro GTFS-RT ingestion Lambda + EventBridge schedule.',
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

for (const stack of [ingestion, billing]) {
  for (const [k, v] of Object.entries(tags)) {
    cdk.Tags.of(stack).add(k, v);
  }
}

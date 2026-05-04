#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { IngestionStack } from '../lib/ingestion-stack';

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION ?? 'us-west-2',
};

const tags = {
  Project: 'la-metro',
  Environment: app.node.tryGetContext('environment') ?? 'dev',
  ManagedBy: 'cdk',
};

const ingestion = new IngestionStack(app, 'LaMetro-IngestionStack', {
  env,
  laMetroApiKey: process.env.LA_METRO_API_KEY,
  laMetroFeedUrl: process.env.LA_METRO_FEED_URL,
  description: 'LA Metro GTFS-RT ingestion Lambda + EventBridge schedule.',
});

for (const [k, v] of Object.entries(tags)) {
  cdk.Tags.of(ingestion).add(k, v);
}

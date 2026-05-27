import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';

export interface StorageStackProps extends cdk.StackProps {
  archiveBucketName?: string;
}

/**
 * Phase 2 storage tier:
 *   - Kinesis Data Stream (1 shard) — primary event bus for vehicle positions
 *   - DynamoDB hot-vehicles — geohash-partitioned current state
 *   - S3 archive bucket — cold storage written by Firehose (RETAIN on destroy)
 *
 * Why one shard: 1MB/s + 1000 records/s is more than enough for one transit
 * agency (~1700 vehicles × 1 record/min = 28 records/s). Reshard later if we
 * add a second agency.
 *
 * Phase 6 additions: `users`, `geofences` (+ route_id GSI), and
 * `notifications` tables for auth + in-app alerts.
 */
export class StorageStack extends cdk.Stack {
  public readonly vehicleStream: kinesis.Stream;
  public readonly hotVehiclesTable: dynamodb.Table;
  public readonly routeAggregatesTable: dynamodb.Table;
  public readonly websocketConnectionsTable: dynamodb.Table;
  public readonly usersTable: dynamodb.Table;
  public readonly geofencesTable: dynamodb.Table;
  public readonly notificationsTable: dynamodb.Table;
  public readonly archiveBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: StorageStackProps = {}) {
    super(scope, id, props);

    this.vehicleStream = new kinesis.Stream(this, 'VehiclePositionsStream', {
      streamName: 'la-metro-vehicle-positions',
      shardCount: 1,
      retentionPeriod: cdk.Duration.hours(24),
      // Stream is the system's source of truth in flight; safe to recreate on
      // destroy since the durable copy lives in S3.
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.hotVehiclesTable = new dynamodb.Table(this, 'HotVehiclesTable', {
      tableName: 'la-metro-hot-vehicles',
      partitionKey: { name: 'geohash', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'vehicle_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      // Drop stale rows after 1h so we never serve dead vehicle state.
      timeToLiveAttribute: 'ttl_epoch',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // GSI for "give me all vehicles on route X" — used by the route detail
    // page in Phase 4.
    this.hotVehiclesTable.addGlobalSecondaryIndex({
      indexName: 'route_id-last_updated-index',
      partitionKey: { name: 'route_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'last_updated', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    // Phase 4b: rolling per-route, per-5-min-window stats. Updated every minute
    // by the Aggregation Lambda; consumed by /routes/{routeId} on the frontend.
    // 7-day TTL keeps the table small without losing the day's history.
    this.routeAggregatesTable = new dynamodb.Table(this, 'RouteAggregatesTable', {
      tableName: 'la-metro-route-aggregates',
      partitionKey: { name: 'route_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'window_start_iso', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl_epoch',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Phase 5a: each open WebSocket has one row here. Connection Lambdas
    // write/delete on $connect / $disconnect; the subscribe handler updates
    // the bbox/route filter. Enrichment scans this table to fan out position
    // updates. 2h TTL guarantees stale rows can't accumulate even if the
    // disconnect handler fails to fire (e.g., abrupt client kill).
    this.websocketConnectionsTable = new dynamodb.Table(this, 'WebSocketConnectionsTable', {
      tableName: 'la-metro-websocket-connections',
      partitionKey: { name: 'connection_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl_epoch',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Phase 6: per-user identity record. PK = Cognito `sub`. Seeded by the
    // PostConfirmation Lambda trigger on signup; updated by the user-api
    // Lambda when the user toggles email alerts. No TTL — this is durable
    // account data, not hot state.
    this.usersTable = new dynamodb.Table(this, 'UsersTable', {
      tableName: 'la-metro-users',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Phase 6: one row per geofence a user has defined. The Aggregation Lambda
    // queries the route_id GSI ("who cares about route X?") each minute and
    // updates last_alerted_epoch when it fires an alert.
    this.geofencesTable = new dynamodb.Table(this, 'GeofencesTable', {
      tableName: 'la-metro-geofences',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'geofence_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.geofencesTable.addGlobalSecondaryIndex({
      indexName: 'route_id-index',
      partitionKey: { name: 'route_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      // Project everything: the Aggregation Lambda needs threshold_seconds,
      // enabled, and last_alerted_epoch from the index read, plus the table
      // keys (user_id, geofence_id) to write the cooldown update back.
      projectionType: dynamodb.ProjectionType.ALL,
    });

    // Phase 6: in-app notifications. PK = user_id, SK = created_at (microsecond
    // ISO, also the client-facing id). 7-day TTL keeps the table small.
    this.notificationsTable = new dynamodb.Table(this, 'NotificationsTable', {
      tableName: 'la-metro-notifications',
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'created_at', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl_epoch',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.archiveBucket = new s3.Bucket(this, 'ArchiveBucket', {
      bucketName: props.archiveBucketName,
      // RETAIN: the archive is the historical record. Don't let `cdk destroy`
      // wipe it. To delete, do it deliberately via the console.
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      versioned: false,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          // Hot for 30 days, then move to Infrequent Access (cheaper).
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
    });

    // Kinesis Firehose: tee the same Kinesis stream into S3 with hourly
    // partitioning. GZIP'd JSON for now — Parquet conversion needs a Glue
    // table, deferred to Phase 7 when Athena queries this archive.
    //
    // Critical: Firehose validates its IAM role at create time. If the role's
    // policies are attached as separate AWS::IAM::Policy resources (what CDK's
    // `grantRead`/`addToPolicy` produces), CFN can race ahead and create the
    // Firehose before IAM has globally propagated the policy attachments,
    // leading to "not authorized to perform kinesis:DescribeStream" failures.
    // Inlining all policies into the Role construct itself makes them part of
    // a single AWS::IAM::Role resource, which is created atomically.
    const firehoseLogGroup = new cdk.aws_logs.LogGroup(this, 'FirehoseLogs', {
      logGroupName: '/aws/kinesisfirehose/la-metro-archive',
      retention: cdk.aws_logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const firehoseRole = new iam.Role(this, 'FirehoseRole', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
      inlinePolicies: {
        FirehoseDelivery: new iam.PolicyDocument({
          statements: [
            // Kinesis source — Firehose still requires the legacy
            // DescribeStream action alongside modern read perms.
            new iam.PolicyStatement({
              actions: [
                'kinesis:DescribeStream',
                'kinesis:GetShardIterator',
                'kinesis:GetRecords',
                'kinesis:ListShards',
              ],
              resources: [this.vehicleStream.streamArn],
            }),
            // S3 destination
            new iam.PolicyStatement({
              actions: [
                's3:AbortMultipartUpload',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:ListBucket',
                's3:ListBucketMultipartUploads',
                's3:PutObject',
              ],
              resources: [
                this.archiveBucket.bucketArn,
                `${this.archiveBucket.bucketArn}/*`,
              ],
            }),
            // CloudWatch Logs for Firehose's delivery diagnostics
            new iam.PolicyStatement({
              actions: ['logs:PutLogEvents', 'logs:CreateLogStream'],
              resources: [`${firehoseLogGroup.logGroupArn}:*`],
            }),
          ],
        }),
      },
    });

    const archiveDeliveryStream = new firehose.CfnDeliveryStream(this, 'ArchiveDeliveryStream', {
      deliveryStreamName: 'la-metro-archive',
      deliveryStreamType: 'KinesisStreamAsSource',
      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: this.vehicleStream.streamArn,
        roleArn: firehoseRole.roleArn,
      },
      extendedS3DestinationConfiguration: {
        bucketArn: this.archiveBucket.bucketArn,
        roleArn: firehoseRole.roleArn,
        // Hive-style partitioning so Athena can prune by date.
        prefix: 'raw-events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
        errorOutputPrefix: 'errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/',
        compressionFormat: 'GZIP',
        bufferingHints: {
          intervalInSeconds: 300,  // flush every 5 min
          sizeInMBs: 5,            // or every 5 MB, whichever first
        },
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: firehoseLogGroup.logGroupName,
          logStreamName: 'S3Delivery',
        },
      },
    });

    // Explicit ordering: don't let CFN create the Firehose until both the
    // role and log group are fully established.
    archiveDeliveryStream.node.addDependency(firehoseRole);
    archiveDeliveryStream.node.addDependency(firehoseLogGroup);

    new cdk.CfnOutput(this, 'VehicleStreamName', { value: this.vehicleStream.streamName });
    new cdk.CfnOutput(this, 'VehicleStreamArn', { value: this.vehicleStream.streamArn });
    new cdk.CfnOutput(this, 'HotVehiclesTableName', { value: this.hotVehiclesTable.tableName });
    new cdk.CfnOutput(this, 'RouteAggregatesTableName', { value: this.routeAggregatesTable.tableName });
    new cdk.CfnOutput(this, 'WebSocketConnectionsTableName', {
      value: this.websocketConnectionsTable.tableName,
    });
    new cdk.CfnOutput(this, 'UsersTableName', { value: this.usersTable.tableName });
    new cdk.CfnOutput(this, 'GeofencesTableName', { value: this.geofencesTable.tableName });
    new cdk.CfnOutput(this, 'NotificationsTableName', { value: this.notificationsTable.tableName });
    new cdk.CfnOutput(this, 'ArchiveBucketName', { value: this.archiveBucket.bucketName });
  }
}

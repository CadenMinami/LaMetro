import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';

export interface ApiStackProps extends cdk.StackProps {
  hotVehiclesTable: dynamodb.ITable;
  routeAggregatesTable: dynamodb.ITable;
  // Phase 4d: archive bucket holds the parsed GTFS-static pickle the
  // arrivals API loads. The Lambda only needs read access under the
  // `gtfs-static/` prefix.
  archiveBucket: s3.IBucket;
}

/**
 * Read API:
 *   GET /vehicles?bbox=lon_min,lat_min,lon_max,lat_max[&route_id=X][&limit=N]
 *   GET /routes/{routeId}/aggregates
 *   GET /stops                           (Phase 4d)
 *   GET /stops/{stopId}/arrivals         (Phase 4d)
 *
 * Backed by a Query Lambda + REST API Gateway. CORS open to '*' for now —
 * tighten to specific origins in Phase 9.
 */
export class ApiStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    const lambdaAssetPath = path.join(__dirname, '..', '..', 'lambdas', 'query_api', '.build');
    const functionName = 'la-metro-query-api';

    const logGroup = new logs.LogGroup(this, 'QueryFnLogs', {
      logGroupName: `/aws/lambda/${functionName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const queryFn = new lambda.Function(this, 'QueryFn', {
      functionName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(lambdaAssetPath),
      // Phase 4d: bumped from 512 → 1024 MB. The arrivals endpoint loads the
      // GTFS-static pickle (~30 MB unpickled into ~150 MB of Python objects
      // when fully realized). 1024 MB also doubles vCPU allocation, which
      // halves the pickle.loads cost on cold start.
      memorySize: 1024,
      // 30s — worst-case /vehicles bbox covers ~500 precision-6 cells, each
      // a sequential DDB query at ~30-50ms. Arrivals cold start adds 3-6s
      // for the pickle load. The cushion absorbs both.
      timeout: cdk.Duration.seconds(30),
      environment: {
        HOT_VEHICLES_TABLE_NAME: props.hotVehiclesTable.tableName,
        ROUTE_AGGREGATES_TABLE_NAME: props.routeAggregatesTable.tableName,
        GEOHASH_PRECISION: '6',
        GTFS_STATIC_BUCKET: props.archiveBucket.bucketName,
        GTFS_STATIC_POINTER_KEY: 'gtfs-static/current.txt',
        // ZoneInfo on the AL2/AL2023 Lambda image needs an explicit tzdata
        // package (in requirements.txt). Belt-and-suspenders: also point at
        // the bundled tzdata via the runtime env if it's there.
        TZ: 'America/Los_Angeles',
      },
      logGroup,
      description:
        'Read API: /vehicles, /routes/{id}/aggregates, /stops, /stops/{id}/arrivals.',
    });

    props.hotVehiclesTable.grantReadData(queryFn);
    props.routeAggregatesTable.grantReadData(queryFn);
    // Scope the S3 read grant to the gtfs-static prefix only — the Lambda has
    // no business reading raw vehicle archives.
    props.archiveBucket.grantRead(queryFn, 'gtfs-static/*');

    const api = new apigw.LambdaRestApi(this, 'QueryApi', {
      restApiName: 'la-metro-query-api',
      handler: queryFn,
      // Auto-wires every path/method to the lambda. We add /vehicles below.
      proxy: false,
      deployOptions: {
        stageName: 'prod',
        throttlingBurstLimit: 100,
        throttlingRateLimit: 60,  // 60 req/s account default; per-IP throttling added in Phase 9
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigw.Cors.ALL_ORIGINS,
        allowMethods: ['GET', 'OPTIONS'],
        allowHeaders: ['Content-Type'],
      },
    });

    const vehicles = api.root.addResource('vehicles');
    vehicles.addMethod('GET');

    // /routes/{routeId}/aggregates — used by the route detail page.
    const routes = api.root.addResource('routes');
    const routeById = routes.addResource('{routeId}');
    const aggregates = routeById.addResource('aggregates');
    aggregates.addMethod('GET');

    // Phase 4d: /stops and /stops/{stopId}/arrivals
    const stops = api.root.addResource('stops');
    stops.addMethod('GET');
    const stopById = stops.addResource('{stopId}');
    const arrivals = stopById.addResource('arrivals');
    arrivals.addMethod('GET');

    new cdk.CfnOutput(this, 'ApiUrl', {
      value: api.url,
      description: 'Base URL for the LA Metro query API.',
    });
  }
}

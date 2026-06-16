import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';

export interface FrontendStackProps extends cdk.StackProps {
  // Path to the Next.js static export output (frontend/out). Resolved at synth.
  exportPath?: string;
}

export class FrontendStack extends cdk.Stack {
  public readonly distributionDomain: string;
  public readonly bucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: FrontendStackProps = {}) {
    super(scope, id, props);

    // Private bucket — no public ACLs, no website hosting. CloudFront reaches
    // it via Origin Access Control (OAC), the modern replacement for OAI.
    this.bucket = new s3.Bucket(this, 'SiteBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Sub-path index resolver. Without this, /route/ returns 404 because
    // S3+OAC doesn't serve index.html for arbitrary prefixes the way an S3
    // website endpoint would. Runs at the viewer edge in <1ms; no Lambda
    // cold-start cost. See https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/example-function-add-index.html
    const indexRewriter = new cloudfront.Function(this, 'IndexRewriter', {
      code: cloudfront.FunctionCode.fromInline(`
        function handler(event) {
          var request = event.request;
          var uri = request.uri;
          if (uri.endsWith('/')) {
            request.uri += 'index.html';
          } else if (!uri.includes('.')) {
            request.uri += '/index.html';
          }
          return request;
        }
      `),
      comment: 'Rewrites extensionless paths to /<path>/index.html for static export.',
    });

    const distribution = new cloudfront.Distribution(this, 'Distribution', {
      defaultBehavior: {
        origin: origins.S3BucketOrigin.withOriginAccessControl(this.bucket),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        cachePolicy: cloudfront.CachePolicy.CACHING_OPTIMIZED,
        compress: true,
        functionAssociations: [
          {
            function: indexRewriter,
            eventType: cloudfront.FunctionEventType.VIEWER_REQUEST,
          },
        ],
      },
      defaultRootObject: 'index.html',
      // Static export emits per-route HTML; map missing keys back to index.html
      // so deep links and client-side route changes both resolve cleanly.
      errorResponses: [
        { httpStatus: 403, responseHttpStatus: 200, responsePagePath: '/index.html' },
        { httpStatus: 404, responseHttpStatus: 200, responsePagePath: '/index.html' },
      ],
      // PriceClass 100 = US/Canada/Europe edges only. Cuts CDN cost for a
      // US-focused audience. Switch to ALL when serving global users.
      priceClass: cloudfront.PriceClass.PRICE_CLASS_100,
    });

    const exportPath = props.exportPath ?? path.join(__dirname, '..', '..', 'frontend', 'out');
    new s3deploy.BucketDeployment(this, 'DeploySite', {
      sources: [s3deploy.Source.asset(exportPath)],
      destinationBucket: this.bucket,
      distribution,
      distributionPaths: ['/*'],
      prune: true,
    });

    this.distributionDomain = distribution.distributionDomainName;

    new cdk.CfnOutput(this, 'SiteUrl', {
      value: `https://${distribution.distributionDomainName}`,
    });
    new cdk.CfnOutput(this, 'BucketName', { value: this.bucket.bucketName });
  }
}

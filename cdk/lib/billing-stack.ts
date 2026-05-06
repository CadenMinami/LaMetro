import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as cwActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as budgets from 'aws-cdk-lib/aws-budgets';

export interface BillingStackProps extends cdk.StackProps {
  alarmEmail: string;
  monthlyBudgetUsd: number;
  alarmThresholdsUsd: number[];
}

/**
 * Cost guardrails. Deploy to us-east-1 — AWS Billing metrics are only
 * published there regardless of where the rest of your stacks live.
 *
 * One-time prerequisite (account-wide, not codifiable in CDK):
 *   AWS Billing console → Billing preferences → enable "Receive Billing Alerts".
 *   Without this, the EstimatedCharges metric never gets populated.
 */
export class BillingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: BillingStackProps) {
    super(scope, id, props);

    const topic = new sns.Topic(this, 'BillingAlertsTopic', {
      displayName: 'LA Metro Billing Alerts',
    });
    topic.addSubscription(new subs.EmailSubscription(props.alarmEmail));

    for (const threshold of props.alarmThresholdsUsd) {
      const alarm = new cloudwatch.Alarm(this, `BillingAlarm${threshold}USD`, {
        alarmName: `la-metro-billing-over-${threshold}-usd`,
        alarmDescription: `Total AWS estimated charges have exceeded $${threshold}.`,
        metric: new cloudwatch.Metric({
          namespace: 'AWS/Billing',
          metricName: 'EstimatedCharges',
          dimensionsMap: { Currency: 'USD' },
          statistic: 'Maximum',
          period: cdk.Duration.hours(6),
        }),
        threshold,
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      alarm.addAlarmAction(new cwActions.SnsAction(topic));
    }

    new budgets.CfnBudget(this, 'MonthlyBudget', {
      budget: {
        budgetName: 'la-metro-monthly',
        budgetType: 'COST',
        timeUnit: 'MONTHLY',
        budgetLimit: { amount: props.monthlyBudgetUsd, unit: 'USD' },
        // No cost filter: track total account spend. Tag-based filters require
        // activating cost allocation tags in the Billing console first (~24h
        // backfill delay), so we keep this simple. This account is dedicated
        // to la-metro per the project spec, so total ≈ project cost anyway.
      },
      notificationsWithSubscribers: [
        {
          notification: {
            comparisonOperator: 'GREATER_THAN',
            notificationType: 'ACTUAL',
            threshold: 80,
            thresholdType: 'PERCENTAGE',
          },
          subscribers: [{ subscriptionType: 'EMAIL', address: props.alarmEmail }],
        },
        {
          notification: {
            comparisonOperator: 'GREATER_THAN',
            notificationType: 'FORECASTED',
            threshold: 100,
            thresholdType: 'PERCENTAGE',
          },
          subscribers: [{ subscriptionType: 'EMAIL', address: props.alarmEmail }],
        },
      ],
    });

    new cdk.CfnOutput(this, 'BillingAlertsTopicArn', {
      value: topic.topicArn,
      description: 'SNS topic that fan-outs to email when any billing alarm fires.',
    });
  }
}

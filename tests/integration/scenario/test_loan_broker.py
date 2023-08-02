"""
This scenario test is taken from https://github.com/localstack-samples/sample-loan-broker-stepfunctions-lambda
which in turn is based on https://www.enterpriseintegrationpatterns.com/ramblings/loanbroker_stepfunctions.html
"""
import json
import os
from dataclasses import dataclass

import aws_cdk
import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_lambda as awslambda
import aws_cdk.aws_lambda_destinations as destinations
import aws_cdk.aws_logs as logs
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import pytest
from aws_cdk.aws_events import EventBus, EventPattern, Rule, RuleTargetInput
from aws_cdk.aws_lambda_event_sources import SnsEventSource, SqsEventSource
from aws_cdk.aws_sqs import IQueue
from constructs import Construct

from localstack.testing.aws.util import is_aws_cloud
from localstack.testing.pytest import markers
from localstack.testing.scenario.provisioning import InfraProvisioner
from localstack.utils.strings import short_uid
from localstack.utils.sync import retry

RECIPIENT_LIST_STACK_NAME = "LoanBroker-RecipientList"
PUB_SUB_STACK_NAME = "LoanBroker-PubSub"
PROJECT_NAME = "CDK Loan Broker"
OUTPUT_LOAN_BROKER_STATE_MACHINE_ARN = "LoanBrokerArn"
LOAN_BROKER_TABLE = "LoanBrokerBanksTable"

CREDIT_BUREAU_JS = "./resources_loan_broker/bank_app_credit_bureau.js"
BANK_APP_JS = "./resources_loan_broker/bank_app.js"
BANK_APP_SNS_JS = "./resources_loan_broker/bank_app_sns.js"
BANK_APP_QUOTE_AGGREGATOR_JS = "./resources_loan_broker/bank_app_quote_aggregator.js"
BANK_APP_QUOTE_REQUESTER_JS = "./resources_loan_broker/bank_app_quote_requester.js"


def _read_file_as_string(filename: str):
    file_path = os.path.join(os.path.dirname(__file__), filename)

    content = None
    with open(file_path, "r") as file:
        content = file.read()
    return content


@dataclass
class Bank:
    bank_id: str
    base_rate: str
    max_loan: str
    min_credit_score: str

    def get_env(self) -> str:
        return {
            "BANK_ID": self.bank_id,
            "BASE_RATE": self.base_rate,
            "MAX_LOAN_AMOUNT": self.max_loan,
            "MIN_CREDIT_SCORE": self.min_credit_score,
        }


class ContentFilter(Construct):
    rule_target_input: RuleTargetInput

    def __init__(self, scope: Construct, id: str, event_path: str):
        super().__init__(scope, id)
        self.rule_target_input = RuleTargetInput.from_event_path(event_path)

    @classmethod
    def create_payload_filter(cls, scope: Construct, id: str):
        return cls(scope, id, "$.detail.responsePayload")


class MessageFilter(Construct):
    event_pattern: EventPattern

    def __init__(self, scope: Construct, id: str, props: EventPattern):
        super().__init__(scope, id)
        self.event_pattern = props

    @classmethod
    def field_exists(cls, scope: Construct, id: str, field_to_check: str):
        return cls(
            scope,
            id,
            EventPattern(detail={"response_payload": {field_to_check: [{"exists": True}]}}),
        )


@dataclass
class MessageContentFilterProps:
    source_event_bus: EventBus
    target_queue: IQueue
    message_filter: MessageFilter
    content_filter: ContentFilter


class MessageContentFilter(Construct):
    def __init__(self, scope: Construct, id: str, props: MessageContentFilterProps):
        super().__init__(scope, id)
        message_filter_rule = Rule(
            scope,
            f"{id}Rule",
            event_bus=props.source_event_bus,
            rule_name=f"{id}Rule",
            event_pattern=props.message_filter.event_pattern,
        )
        message_filter_rule.add_target(
            targets.SqsQueue(
                queue=props.target_queue, message=props.content_filter.rule_target_input or {}
            )
        )


class TestLoanBrokerScenario:
    BANKS = {
        "BankRecipientPawnShop": Bank(
            bank_id="PawnShop", base_rate="5", max_loan="500000", min_credit_score="400"
        ),
        "BankRecipientUniversal": Bank(
            bank_id="Universal", base_rate="4", max_loan="700000", min_credit_score="500"
        ),
        "BankRecipientPremium": Bank(
            bank_id="Premium", base_rate="3", max_loan="900000", min_credit_score="600"
        ),
    }

    @pytest.fixture(scope="class", autouse=True)
    def infrastructure(self, aws_client):
        infra = InfraProvisioner(aws_client)
        app = cdk.App()
        recipient_stack = cdk.Stack(app, RECIPIENT_LIST_STACK_NAME)
        cdk.Tags.of(recipient_stack).add("Project", PROJECT_NAME)
        cdk.Tags.of(recipient_stack).add("Stackname", RECIPIENT_LIST_STACK_NAME)
        self.setup_recipient_list_stack(recipient_stack)
        #
        # pub_sub_stack = cdk.Stack(app, PUB_SUB_STACK_NAME)
        # cdk.Tags.of(pub_sub_stack).add("Project", PROJECT_NAME)
        # cdk.Tags.of(pub_sub_stack).add("Stackname", PUB_SUB_STACK_NAME)
        # self.setup_pub_sub_stack(pub_sub_stack)

        infra.add_cdk_stack(recipient_stack)
        # infra.add_cdk_stack(pub_sub_stack)

        # set skip_teardown=True to prevent the stack to be deleted
        with infra.provisioner(skip_teardown=False) as prov:
            if not infra.skipped_provisioning:
                # here we could add some initial setup, e.g. pre-filling the app with data
                bank_addresses = [{"S": bank_name} for bank_name in self.BANKS.keys()]
                aws_client.dynamodb.put_item(
                    TableName=LOAN_BROKER_TABLE,
                    Item={"Type": {"S": "Home"}, "BankAddress": {"L": bank_addresses}},
                )
            yield prov

    @pytest.mark.parametrize(
        "step_function_input,expected_result",
        [
            # score linked to this SSN will receive quotes
            ({"SSN": "123-45-6789", "Amount": 5000, "Term": 30}, "SUCCEEDED"),
            # score linked to this SSN will not receive quotes, but step function call succeeds
            ({"SSN": "458-45-6789", "Amount": 5000, "Term": 30}, "SUCCEEDED"),
            ({"SSN": "inv-45-6789", "Amount": 5000, "Term": 30}, "FAILED"),
            ({"unexpected": "234-45-6789"}, "FAILED"),
            ({"SSN": "234-45-6789"}, "FAILED"),  # TODO LS: it keeps in RUNNING but should fail
        ],
    )
    @markers.snapshot.skip_snapshot_verify(
        paths=["$..traceHeader", "$..cause", "$..error"]
    )  # TODO add missing properties
    def test_stepfunctions_input_recipient_list(
        self, aws_client, infrastructure, step_function_input, expected_result, snapshot
    ):
        snapshot.add_transformer(snapshot.transform.stepfunctions_api())
        snapshot.add_transformer(snapshot.transform.key_value("executionArn"))
        snapshot.add_transformer(snapshot.transform.key_value("stateMachineArn"))
        snapshot.add_transformer(snapshot.transform.key_value("traceHeader"))
        snapshot.add_transformer(snapshot.transform.key_value("name"))

        outputs = infrastructure.get_stack_outputs(RECIPIENT_LIST_STACK_NAME)
        state_machine_arn = outputs.get(OUTPUT_LOAN_BROKER_STATE_MACHINE_ARN)
        execution_name = f"my-test-{short_uid()}"

        result = aws_client.stepfunctions.start_execution(
            name=execution_name,
            stateMachineArn=state_machine_arn,
            input=json.dumps(step_function_input),
        )
        execution_arn = result["executionArn"]

        def _execution_finished():
            res = aws_client.stepfunctions.describe_execution(executionArn=execution_arn)
            assert res["status"] == expected_result
            return res

        result = retry(_execution_finished, sleep=2, retries=100 if is_aws_cloud() else 10)

        snapshot.match("describe-execution-finished", result)

    def setup_recipient_list_stack(self, stack: cdk.Stack):
        credit_bureau_lambda = awslambda.Function(
            stack,
            "CreditBureauLambda",
            handler="index.handler",
            code=awslambda.InlineCode(code=_read_file_as_string(CREDIT_BUREAU_JS)),
            runtime=awslambda.Runtime.NODEJS_18_X,
        )

        get_credit_score_form_credit_bureau = tasks.LambdaInvoke(
            stack,
            "Get Credit Score from credit bureau",
            lambda_function=credit_bureau_lambda,
            payload=sfn.TaskInput.from_object({"SSN.$": "$.SSN", "RequestId.$": "$$.Execution.Id"}),
            result_path="$.Credit",
            result_selector={
                "Score.$": "$.Payload.body.score",
                "History.$": "$.Payload.body.history",
            },
            retry_on_service_exceptions=False,
        )

        bank_table = dynamodb.Table(
            stack,
            "LoanBrokerBanksTable",
            partition_key={"name": "Type", "type": dynamodb.AttributeType.STRING},
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name=LOAN_BROKER_TABLE,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        fetch_bank_address_from_database = tasks.DynamoGetItem(
            stack,
            "Fetch Bank Addresses from database",
            table=bank_table,
            key={"Type": tasks.DynamoAttributeValue.from_string("Home")},
            result_path="$.Banks",
            result_selector={"BankAddress.$": "$.Item.BankAddress.L[*].S"},
        )

        get_individual_bank_quotes = sfn.CustomState(
            stack,
            "Get individual bank quotes",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName.$": "$.function",
                    "Payload": {
                        "SSN.$": "$.SSN",
                        "Amount.$": "$.Amount",
                        "Term.$": "$.Term",
                        "Credit.$": "$.Credit",
                    },
                },
                "ResultSelector": {"Quote.$": "$.Payload"},
            },
        )

        get_all_bank_quotes = sfn.Map(
            stack,
            "Get all bank quotes",
            items_path="$.Banks.BankAddress",
            parameters={
                "function.$": "$$.Map.Item.Value",
                "SSN.$": "$.SSN",
                "Amount.$": "$.Amount",
                "Term.$": "$.Term",
                "Credit.$": "$.Credit",
            },
            result_path="$.Quotes",
        )

        loan_broker_definition = get_credit_score_form_credit_bureau.next(
            fetch_bank_address_from_database
        ).next(get_all_bank_quotes.iterator(get_individual_bank_quotes))

        loan_broker_log_group = logs.LogGroup(stack, "LoanBrokerLogGroup")
        loan_broker = sfn.StateMachine(
            stack,
            "LoanBroker",
            definition=loan_broker_definition,
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=cdk.Duration.minutes(5),
            logs={
                "destination": loan_broker_log_group,
                "level": sfn.LogLevel.ALL,
                "include_execution_data": True,
            },
            tracing_enabled=True,
        )

        for bank_name, bank_env in self.BANKS.items():
            bank_function = self._create_bank_function(stack, bank_name, bank_env.get_env())
            bank_function.grant_invoke(loan_broker)

        cdk.CfnOutput(
            stack, OUTPUT_LOAN_BROKER_STATE_MACHINE_ARN, value=loan_broker.state_machine_arn
        )

    def _create_bank_function(self, stack: cdk.Stack, name: str, env: dict) -> awslambda.Function:
        return awslambda.Function(
            stack,
            name,
            runtime=awslambda.Runtime.NODEJS_18_X,
            handler="index.handler",
            code=awslambda.InlineCode(code=_read_file_as_string(BANK_APP_JS)),
            function_name=name,
            environment=env,
        )

    def setup_pub_sub_stack(self, stack: cdk.Stack):
        # TODO check why we need this again (it's already in the other stack)
        credit_bureau_lambda = awslambda.Function(
            stack,
            "CreditBureauLambda",
            runtime=awslambda.Runtime.NODEJS_18_X,
            handler="index.handler",
            code=awslambda.InlineCode(code=_read_file_as_string(CREDIT_BUREAU_JS)),
            function_name="CreditBureauLambda-PubSub",
        )
        # TODO also the 2nd time exactly the same ...
        get_credit_score_from_credit_bureau = tasks.LambdaInvoke(
            stack,
            "Get Credit Score from credit bureau",
            lambda_function=credit_bureau_lambda,
            payload=sfn.TaskInput.from_object({"SSN.$": "$.SSN", "RequestId.$": "$$.Execution.Id"}),
            result_path="$.Credit",
            result_selector={
                "Score.$": "$.Payload.body.score",
                "History.$": "$.Payload.body.history",
            },
            retry_on_service_exceptions=False,
        )

        mortgage_quotes_event_bus = events.EventBus(
            stack, "MortgageQuotesEventBus", event_bus_name="MortgageQuotesEventBus"
        )

        mortgage_quotes_queue = sqs.Queue(
            stack,
            "MortgageQuotesQueue",
            retention_period=cdk.Duration.minutes(5),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        non_empty_quote_message_filter = MessageFilter.field_exists(
            stack, "nonEmptyQuoteMessageFilter", "bankId"
        )
        payload_content_filter = ContentFilter.create_payload_filter(stack, "PayloadContentFilter")

        MessageContentFilter(
            stack,
            "FilterMortgageQuotes",
            MessageContentFilterProps(
                source_event_bus=mortgage_quotes_event_bus,
                target_queue=mortgage_quotes_queue,
                message_filter=non_empty_quote_message_filter,
                content_filter=payload_content_filter,
            ),
        )

        mortgage_quote_request_topic = sns.Topic(
            stack, "MortgageQuoteRequestTopic", display_name="MortgageQuoteRequest Topic"
        )
        for bank_name, bank_env in self.BANKS.items():
            bank_function = self._create_bank_function_sns(
                stack, bank_name, bank_env.get_env(), mortgage_quotes_event_bus
            )
            bank_function.add_event_source(SnsEventSource(topic=mortgage_quote_request_topic))

        mortgage_quotes_table = dynamodb.Table(
            stack,
            "MortgageQuotesTable",
            partition_key={"name": "Type", "type": dynamodb.AttributeType.STRING},
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name="MortgageQuotesTable",
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        quote_aggregator_lambda = awslambda.Function(
            stack,
            "QuoteAggregatorLambda",
            runtime=awslambda.Runtime.NODEJS_18_X,
            code=awslambda.InlineCode(code=_read_file_as_string(BANK_APP_QUOTE_AGGREGATOR_JS)),
            handler="index.handler",
            function_name="QuoteAggregator",
            environment={"MORTGAGE_QUOTES_TABLE": mortgage_quotes_table.table_name},
        )
        quote_aggregator_lambda.add_event_source(
            SqsEventSource(mortgage_quotes_queue, batch_size=10)
        )

        mortgage_quotes_queue.grant_consume_messages(quote_aggregator_lambda)
        mortgage_quotes_table.grant_write_data(quote_aggregator_lambda)

        request_mortgage_quotes_from_all_banks = tasks.SnsPublish(
            stack,
            "RequestMortgageFromAllBanks",
            topic=mortgage_quote_request_topic,
            message=sfn.TaskInput.from_object(
                {
                    "taskToken": sfn.JsonPath.task_token,
                    "input": sfn.JsonPath.entire_payload,
                    "context": sfn.JsonPath.entire_context,
                }
            ),
            result_path="$.Quotes",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            timeout=aws_cdk.Duration.seconds(5),
        )
        get_mortgage_quotes_lambda = awslambda.Function(
            stack,
            "GetMortgageQuotes",
            runtime=awslambda.Runtime.NODEJS_18_X,
            code=awslambda.InlineCode(code=_read_file_as_string(BANK_APP_QUOTE_REQUESTER_JS)),
            handler="index.handler",
            function_name="QuoteRequester",
            environment={"MORTGAGE_QUOTES_TABLE": mortgage_quotes_table.table_name},
        )
        mortgage_quotes_table.grant_read_data(get_mortgage_quotes_lambda)

        get_mortgage_quotes = tasks.LambdaInvoke(
            stack,
            "Get Mortgage Quotes",
            lambda_function=get_mortgage_quotes_lambda,
            payload=sfn.TaskInput.from_object({"Id.$": "$$.Execution.Id"}),
            result_path="$.result",
            result_selector={"Quotes.$": "$.Payload.quotes"},
            retry_on_service_exceptions=False,
        )

        transform_mortgage_quotes_response = sfn.Pass(
            stack,
            "Transform Mortgage Quotes Response",
            parameters={
                "SSN.$": "$.SSN",
                "Amount.$": "$.Amount",
                "Term.$": "$.Term",
                "Credit.$": "$.Credit",
                "Quotes.$": "$.result.Quotes",
            },
        )

        loan_broker_definition = get_credit_score_from_credit_bureau.next(
            request_mortgage_quotes_from_all_banks.add_catch(
                get_mortgage_quotes.next(transform_mortgage_quotes_response),
                errors=["States.Timeout"],
                result_path="$.Error",
            )
        )

        loan_broker_log_group = logs.LogGroup(stack, "LoanBrokerLogGroup")

        loan_broker = sfn.StateMachine(
            stack,
            "LoanBroker",
            definition=loan_broker_definition,
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=aws_cdk.Duration.minutes(5),
            logs={
                "destination": loan_broker_log_group,
                "level": sfn.LogLevel.ALL,
                "include_execution_data": True,
            },
            tracing_enabled=True,
        )

        mortgage_quote_request_topic.grant_publish(loan_broker)
        loan_broker.grant_task_response(quote_aggregator_lambda)

        cdk.CfnOutput(
            stack, OUTPUT_LOAN_BROKER_STATE_MACHINE_ARN, value=loan_broker.state_machine_arn
        )

    def _create_bank_function_sns(
        self, stack: cdk.Stack, name: str, env: dict, destination_event_bus: EventBus
    ) -> awslambda.Function:
        return awslambda.Function(
            stack,
            name,
            runtime=awslambda.Runtime.NODEJS_18_X,
            handler="index.handler",
            code=awslambda.InlineCode(code=_read_file_as_string(BANK_APP_SNS_JS)),
            function_name=name + "-PubSub",
            environment=env,
            on_success=destinations.EventBridgeDestination(event_bus=destination_event_bus),
        )

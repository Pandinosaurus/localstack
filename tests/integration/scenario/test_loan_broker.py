"""
This scenario test is taken from https://github.com/localstack-samples/sample-loan-broker-stepfunctions-lambda
which in turn is based on https://www.enterpriseintegrationpatterns.com/ramblings/loanbroker_stepfunctions.html
"""
import json
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
from localstack.testing.scenario.provisioning import InfraProvisioner
from localstack.utils.strings import short_uid
from localstack.utils.sync import retry

RECIPIENT_LIST_STACK_NAME = "LoanBroker-RecipientList"
PUB_SUB_STACK_NAME = "LoanBroker-PubSub"
PROJECT_NAME = "CDK Loan Broker"
OUTPUT_LOAN_BROKER_STATE_MACHINE_ARN = "LoanBrokerArn"
LOAN_BROKER_TABLE = "LoanBrokerBanksTable"
CREDIT_BUREAU_JS = r"""
const getRandomInt = (min, max) => {
    return min + Math.floor(Math.random() * (max - min));
};

exports.handler = async (event) => {
    const min_score = 300;
    const max_score = 900;

    var ssn_regex = new RegExp("^\\d{3}-\\d{2}-\\d{4}$");


    console.log("received event " + JSON.stringify(event))
    if (ssn_regex.test(event.SSN)) {
        console.log("ssn matches pattern")
        return {
            statusCode: 200,
            request_id: event.RequestId,
            body: {
                SSN: event.SSN,
                score: getRandomInt(min_score, max_score),
                history: getRandomInt(1, 30),
            },
        };
    } else {
        console.log("ssn not matching pattern")
        return {
            statusCode: 400,
            request_id: event.RequestId,
            body: {
                SSN: event.SSN,
            },
        };
    }
};
"""

BANK_APP_JS = """
/**
    Each bank will vary its behavior by the following parameters:

    MIN_CREDIT_SCORE - the customer's minimum credit score required to receive a quote from this bank.
    MAX_LOAN_AMOUNT - the maximum amount the bank is willing to lend to a customer.
    BASE_RATE - the minimum rate the bank might give. The actual rate increases for a lower credit score and some randomness.
    BANK_ID - as the loan broker processes multiple responses, knowing which bank supplied the quote will be handy.
 */

function calcRate(amount, term, score, history) {
    if (amount <= process.env.MAX_LOAN_AMOUNT && score >= process.env.MIN_CREDIT_SCORE) {
        return parseFloat(process.env.BASE_RATE) + Math.random() * ((1000 - score) / 100.0);
    }
}

exports.handler = async (event) => {
    console.log("Received request for %s", process.env.BANK_ID);
    console.log("Received event:", JSON.stringify(event, null, 4));

    const amount = event.Amount;
    const term = event.Term;
    const score = event.Credit.Score;
    const history = event.Credit.History;

    const bankId = process.env.BANK_ID;

    console.log("Loan Request over %d at credit score %d", amount, score);
    const rate = calcRate(amount, term, score, history);
    if (rate) {
        const response = { rate: rate, bankId: bankId };
        console.log(response);
        return response;
    }
};
"""
BANK_APP_SNS_JS = """
/**
    Each bank will vary its behavior by the following parameters:

    MIN_CREDIT_SCORE - the customer's minimum credit score required to receive a quote from this bank.
    MAX_LOAN_AMOUNT - the maximum amount the bank is willing to lend to a customer.
    BASE_RATE - the minimum rate the bank might give. The actual rate increases for a lower credit score and some randomness.
    BANK_ID - as the loan broker processes multiple responses, knowing which bank supplied the quote will be handy.
*/

function calcRate(amount, term, score, history) {
    if (amount <= process.env.MAX_LOAN_AMOUNT && score >= process.env.MIN_CREDIT_SCORE) {
        return parseFloat(process.env.BASE_RATE) + Math.random() * ((1000 - score) / 100.0);
    }
}

exports.handler = async (event, context) => {
    console.log("Received request for %s", process.env.BANK_ID);
    console.log("Received event:", JSON.stringify(event, null, 4));

    console.log(event.Records[0].Sns);
    const snsMessage = event.Records[0].Sns.Message;
    const msg = JSON.parse(snsMessage);
    console.debug(msg.input);

    const requestId = msg.context.Execution.Id;
    const taskToken = msg.taskToken;
    const bankId = process.env.BANK_ID;
    const data = msg.input;

    console.log("Loan Request over %d at credit score %d", data.Amount, data.Credit.Score);
    const rate = calcRate(data.Amount, data.Term, data.Credit.Score, data.Credit.History);

    if (rate) {
        const quote = {
            rate: rate,
            bankId: bankId,
            id: requestId,
            taskToken: taskToken,
        };
        console.log("Offering Loan", quote);

        return quote;
    } else {
        console.log("Rejecting Loan");
    }
};
"""
BANK_APP_QUOTE_AGGREGATOR_JS = """
const { DynamoDBClient, UpdateItemCommand } = require("@aws-sdk/client-dynamodb");
const { unmarshall } = require("@aws-sdk/util-dynamodb");
const { SFNClient, SendTaskSuccessCommand } = require("@aws-sdk/client-sfn");

const dynamodb = new DynamoDBClient({ apiVersion: "2012-08-10" });
const sfn = new SFNClient();

const mortgageQuotesTable = process.env.MORTGAGE_QUOTES_TABLE;


const quoteRequestComplete = (amountQuotes) =>
    amountQuotes >= 2;


const createAppendQuoteUpdateItemCommand = (tableName, id, quote) =>
    new UpdateItemCommand({
        TableName: tableName,
        Key: { Id: { S: id } },
        UpdateExpression: "SET #quotes = list_append(if_not_exists(#quotes, :empty_list), :quote)",
        ExpressionAttributeNames: {
            "#quotes": "quotes",
        },
        ExpressionAttributeValues: {
            ":quote": {
                L: [
                    {
                        M: {
                            bankId: { S: quote["bankId"] },
                            rate: { N: quote["rate"].toString() },
                        },
                    },
                ],
            },
            ":empty_list": { L: [] },
        },
        ReturnValues: "ALL_NEW",
    });


exports.handler = async (event) => {
    console.info("Received event:", JSON.stringify(event, null, 4));
    console.info("Processing %d records", event["Records"].length);

    var persistedMortgageQuotes;
    for (record of event["Records"]) {
        console.debug(record);

        var quote = JSON.parse(record["body"]);
        console.info("Persisting quote: %s", JSON.stringify(quote, null, 4));

        var id = quote["id"];
        var taskToken = quote["taskToken"];

        var appendQuoteUpdateItemCommand = createAppendQuoteUpdateItemCommand(mortgageQuotesTable, id, quote);

        var dynamodbResponse = await dynamodb.send(appendQuoteUpdateItemCommand);
        console.debug(JSON.stringify(dynamodbResponse));
        console.debug(unmarshall(dynamodbResponse.Attributes));
        persistedMortgageQuotes = unmarshall(dynamodbResponse.Attributes);
    }

    console.info("Persisted %d quotes", persistedMortgageQuotes.quotes.length);

    if (quoteRequestComplete(persistedMortgageQuotes.quotes.length)) {
        console.info("Enough quotes are available");
        var sendTaskSuccessCommand = new SendTaskSuccessCommand({
            taskToken,
            output: JSON.stringify(persistedMortgageQuotes.quotes),
        });

        try {
            var response = await sfn.send(sendTaskSuccessCommand);
            console.debug(response);
        } catch (error) {
            console.error(error);
        }
    } else {
        console.info("Not enough quotes available yet");
    }
};
"""
BANK_APP_QUOTE_REQUESTER_JS = """
const { DynamoDBClient, UpdateItemCommand } = require("@aws-sdk/client-dynamodb");
const { unmarshall } = require("@aws-sdk/util-dynamodb");
const { SFNClient, SendTaskSuccessCommand } = require("@aws-sdk/client-sfn");

const dynamodb = new DynamoDBClient({ apiVersion: "2012-08-10" });
const sfn = new SFNClient();

const mortgageQuotesTable = process.env.MORTGAGE_QUOTES_TABLE;


const quoteRequestComplete = (amountQuotes) =>
    amountQuotes >= 2;


const createAppendQuoteUpdateItemCommand = (tableName, id, quote) =>
    new UpdateItemCommand({
        TableName: tableName,
        Key: { Id: { S: id } },
        UpdateExpression: "SET #quotes = list_append(if_not_exists(#quotes, :empty_list), :quote)",
        ExpressionAttributeNames: {
            "#quotes": "quotes",
        },
        ExpressionAttributeValues: {
            ":quote": {
                L: [
                    {
                        M: {
                            bankId: { S: quote["bankId"] },
                            rate: { N: quote["rate"].toString() },
                        },
                    },
                ],
            },
            ":empty_list": { L: [] },
        },
        ReturnValues: "ALL_NEW",
    });


exports.handler = async (event) => {
    console.info("Received event:", JSON.stringify(event, null, 4));
    console.info("Processing %d records", event["Records"].length);

    var persistedMortgageQuotes;
    for (record of event["Records"]) {
        console.debug(record);

        var quote = JSON.parse(record["body"]);
        console.info("Persisting quote: %s", JSON.stringify(quote, null, 4));

        var id = quote["id"];
        var taskToken = quote["taskToken"];

        var appendQuoteUpdateItemCommand = createAppendQuoteUpdateItemCommand(mortgageQuotesTable, id, quote);

        var dynamodbResponse = await dynamodb.send(appendQuoteUpdateItemCommand);
        console.debug(JSON.stringify(dynamodbResponse));
        console.debug(unmarshall(dynamodbResponse.Attributes));
        persistedMortgageQuotes = unmarshall(dynamodbResponse.Attributes);
    }

    console.info("Persisted %d quotes", persistedMortgageQuotes.quotes.length);

    if (quoteRequestComplete(persistedMortgageQuotes.quotes.length)) {
        console.info("Enough quotes are available");
        var sendTaskSuccessCommand = new SendTaskSuccessCommand({
            taskToken,
            output: JSON.stringify(persistedMortgageQuotes.quotes),
        });

        try {
            var response = await sfn.send(sendTaskSuccessCommand);
            console.debug(response);
        } catch (error) {
            console.error(error);
        }
    } else {
        console.info("Not enough quotes available yet");
    }
};
"""


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

        pub_sub_stack = cdk.Stack(app, PUB_SUB_STACK_NAME)
        cdk.Tags.of(pub_sub_stack).add("Project", PROJECT_NAME)
        cdk.Tags.of(pub_sub_stack).add("Stackname", PUB_SUB_STACK_NAME)
        self.setup_pub_sub_stack(pub_sub_stack)

        infra.add_cdk_stack(recipient_stack)
        infra.add_cdk_stack(pub_sub_stack)

        # set skip_teardown=True to prevent the stack to be deleted
        with infra.provisioner(skip_teardown=True) as prov:
            if not infra.skipped_provisioning:
                # here we could add some initial setup, e.g. pre-filling the app with data
                bank_addresses = [{"S": bank_name} for bank_name in self.BANKS.keys()]
                aws_client.dynamodb.put_item(
                    TableName=LOAN_BROKER_TABLE,
                    Item={"Type": {"S": "Home"}, "BankAddress": {"L": bank_addresses}},
                )
            yield prov

    def test_something(self, aws_client, infrastructure):
        outputs = infrastructure.get_stack_outputs(RECIPIENT_LIST_STACK_NAME)
        state_machine_arn = outputs.get(OUTPUT_LOAN_BROKER_STATE_MACHINE_ARN)
        input = {"SSN": "123-45-6789", "Amount": 5000, "Term": 30}
        result = aws_client.stepfunctions.start_execution(
            name=f"my-first-test-{short_uid()}",
            stateMachineArn=state_machine_arn,
            input=json.dumps(input),
        )
        execution_arn = result["executionArn"]
        # TODO make snapshot right after the start, status should be running

        def _execution_finished():
            res = aws_client.stepfunctions.describe_execution(executionArn=execution_arn)
            assert res["Status"] == "SUCCEEDED"  # TODO
            return res

        result = retry(_execution_finished, sleep=2, retries=100 if is_aws_cloud() else 10)
        # snapshot.match("describe-execution-finished", result)

    def setup_recipient_list_stack(self, stack: cdk.Stack):
        credit_bureau_lambda = awslambda.Function(
            stack,
            "CreditBureauLambda",
            handler="index.handler",
            code=awslambda.InlineCode(code=CREDIT_BUREAU_JS),
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
            code=awslambda.InlineCode(code=BANK_APP_JS),
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
            code=awslambda.InlineCode(code=CREDIT_BUREAU_JS),
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
            code=awslambda.InlineCode(code=BANK_APP_QUOTE_AGGREGATOR_JS),
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
            code=awslambda.InlineCode(code=BANK_APP_QUOTE_REQUESTER_JS),
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
            code=awslambda.InlineCode(code=BANK_APP_SNS_JS),
            function_name=name + "-PubSub",
            environment=env,
            on_success=destinations.EventBridgeDestination(event_bus=destination_event_bus),
        )

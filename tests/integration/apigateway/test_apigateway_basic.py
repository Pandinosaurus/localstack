import base64
import itertools
import json
import os
import re
from collections import namedtuple
from typing import Callable, Optional

import pytest
import xmltodict
from botocore.exceptions import ClientError
from jsonpatch import apply_patch
from requests.structures import CaseInsensitiveDict

from localstack import config
from localstack.aws.accounts import get_aws_account_id
from localstack.aws.api.lambda_ import Runtime
from localstack.aws.handlers import cors
from localstack.config import get_edge_url
from localstack.constants import APPLICATION_JSON, LOCALHOST_HOSTNAME
from localstack.services.apigateway.helpers import (
    TAG_KEY_CUSTOM_ID,
    connect_api_gateway_to_sqs,
    get_resource_for_path,
    get_rest_api_paths,
    host_based_url,
    path_based_url,
)
from localstack.services.awslambda.lambda_api import add_event_source, use_docker
from localstack.services.awslambda.lambda_utils import LAMBDA_RUNTIME_PYTHON39
from localstack.testing.pytest import markers
from localstack.utils import testutil
from localstack.utils.aws import arns, aws_stack, queries
from localstack.utils.aws import resources as resource_util
from localstack.utils.collections import select_attributes
from localstack.utils.files import load_file
from localstack.utils.http import safe_requests as requests
from localstack.utils.json import clone
from localstack.utils.platform import get_arch
from localstack.utils.strings import short_uid, to_str
from localstack.utils.sync import retry
from tests.integration.apigateway.apigateway_fixtures import (
    api_invoke_url,
    create_rest_api_deployment,
    create_rest_api_integration,
    create_rest_api_integration_response,
    create_rest_api_method_response,
    create_rest_api_stage,
    create_rest_resource,
    create_rest_resource_method,
    delete_rest_api,
    get_rest_api,
    update_rest_api_deployment,
    update_rest_api_stage,
)
from tests.integration.apigateway.conftest import (
    APIGATEWAY_ASSUME_ROLE_POLICY,
    APIGATEWAY_LAMBDA_POLICY,
    APIGATEWAY_STEPFUNCTIONS_POLICY,
    STEPFUNCTIONS_ASSUME_ROLE_POLICY,
)
from tests.integration.awslambda.test_lambda import (
    TEST_LAMBDA_AWS_PROXY,
    TEST_LAMBDA_HTTP_RUST,
    TEST_LAMBDA_NODEJS,
    TEST_LAMBDA_NODEJS_APIGW_502,
    TEST_LAMBDA_NODEJS_APIGW_INTEGRATION,
    TEST_LAMBDA_PYTHON,
    TEST_LAMBDA_PYTHON_ECHO,
)

# TODO: split up the tests in this file into more specific test sub-modules


THIS_FOLDER = os.path.dirname(os.path.realpath(__file__))
TEST_SWAGGER_FILE_JSON = os.path.join(THIS_FOLDER, "../files", "swagger.json")
TEST_SWAGGER_FILE_YAML = os.path.join(THIS_FOLDER, "../files", "swagger.yaml")
TEST_IMPORT_REST_API_FILE = os.path.join(THIS_FOLDER, "../files", "pets.json")
TEST_IMPORT_MOCK_INTEGRATION = os.path.join(THIS_FOLDER, "../files", "openapi-mock.json")
TEST_IMPORT_REST_API_ASYNC_LAMBDA = os.path.join(THIS_FOLDER, "../files", "api_definition.yaml")

ApiGatewayLambdaProxyIntegrationTestResult = namedtuple(
    "ApiGatewayLambdaProxyIntegrationTestResult",
    [
        "data",
        "resource",
        "result",
        "url",
        "path_with_replace",
    ],
)


class TestAPIGateway:
    # template used to transform incoming requests at the API Gateway (stream name to be filled
    # in later)
    APIGATEWAY_DATA_INBOUND_TEMPLATE = """{
        "StreamName": "%s",
        "Records": [
            #set( $numRecords = $input.path('$.records').size() )
            #if($numRecords > 0)
            #set( $maxIndex = $numRecords - 1 )
            #foreach( $idx in [0..$maxIndex] )
            #set( $elem = $input.path("$.records[${idx}]") )
            #set( $elemJsonB64 = $util.base64Encode($elem.data) )
            {
                "Data": "$elemJsonB64",
                "PartitionKey": #if( $elem.partitionKey != '')"$elem.partitionKey"
                                #else"$elemJsonB64.length()"#end
            }#if($foreach.hasNext),#end
            #end
            #end
        ]
    }"""

    # endpoint paths
    API_PATH_DATA_INBOUND = "/data"
    API_PATH_HTTP_BACKEND = "/hello_world"
    API_PATH_LAMBDA_PROXY_BACKEND = "/lambda/foo1"
    API_PATH_LAMBDA_PROXY_BACKEND_WITH_PATH_PARAM = "/lambda/{test_param1}"

    API_PATH_LAMBDA_PROXY_BACKEND_ANY_METHOD = "/lambda-any-method/foo1"
    API_PATH_LAMBDA_PROXY_BACKEND_ANY_METHOD_WITH_PATH_PARAM = "/lambda-any-method/{test_param1}"

    API_PATH_LAMBDA_PROXY_BACKEND_WITH_IS_BASE64 = "/lambda-is-base64/foo1"

    # name of Kinesis stream connected to API Gateway
    TEST_STREAM_KINESIS_API_GW = "test-stream-api-gw"
    TEST_STAGE_NAME = "testing"
    TEST_LAMBDA_PROXY_BACKEND = "test_lambda_apigw_backend"
    TEST_LAMBDA_PROXY_BACKEND_WITH_PATH_PARAM = "test_lambda_apigw_backend_path_param"
    TEST_LAMBDA_PROXY_BACKEND_ANY_METHOD = "test_lambda_apigw_backend_any_method"
    TEST_LAMBDA_PROXY_BACKEND_ANY_METHOD_WITH_PATH_PARAM = (
        "test_lambda_apigw_backend_any_method_path_param"
    )
    TEST_LAMBDA_PROXY_BACKEND_WITH_IS_BASE64 = "test_lambda_apigw_backend_with_is_base64"
    TEST_LAMBDA_SQS_HANDLER_NAME = "lambda_sqs_handler"
    TEST_LAMBDA_AUTHORIZER_HANDLER_NAME = "lambda_authorizer_handler"
    TEST_API_GATEWAY_ID = "fugvjdxtri"

    TEST_API_GATEWAY_AUTHORIZER = {
        "name": "test",
        "type": "TOKEN",
        "providerARNs": ["arn:aws:cognito-idp:us-east-1:123412341234:userpool/us-east-1_123412341"],
        "authType": "custom",
        "authorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
        + "arn:aws:lambda:us-east-1:123456789012:function:myApiAuthorizer"
        "/invocations",
        "authorizerCredentials": "arn:aws:iam::123456789012:role/apigAwsProxyRole",
        "identitySource": "method.request.header.Authorization",
        "identityValidationExpression": ".*",
        "authorizerResultTtlInSeconds": 300,
    }
    TEST_API_GATEWAY_AUTHORIZER_OPS = [{"op": "replace", "path": "/name", "value": "test1"}]

    @markers.parity.aws_validated
    def test_delete_rest_api_with_invalid_id(self, aws_client):
        with pytest.raises(ClientError) as e:
            aws_client.apigateway.delete_rest_api(restApiId="foobar")

        assert e.value.response["Error"]["Code"] == "NotFoundException"
        assert "Invalid API identifier specified" in e.value.response["Error"]["Message"]
        assert "foobar" in e.value.response["Error"]["Message"]

    @pytest.mark.parametrize("url_function", [path_based_url, host_based_url])
    def test_create_rest_api_with_custom_id(self, create_rest_apigw, url_function, aws_client):
        apigw_name = f"gw-{short_uid()}"
        test_id = "testId123"
        api_id, name, _ = create_rest_apigw(name=apigw_name, tags={TAG_KEY_CUSTOM_ID: test_id})
        assert test_id == api_id
        assert apigw_name == name
        api_id, name = get_rest_api(aws_client.apigateway, restApiId=test_id)
        assert test_id == api_id
        assert apigw_name == name

        spec_file = load_file(TEST_IMPORT_MOCK_INTEGRATION)
        aws_client.apigateway.put_rest_api(restApiId=test_id, body=spec_file, mode="overwrite")

        url = url_function(test_id, stage_name="latest", path="/echo/foobar")
        response = requests.get(url)

        assert response.ok
        assert response._content == b'{"echo": "foobar", "response": "mocked"}'

    def test_api_gateway_kinesis_integration(self, aws_client):
        # create target Kinesis stream
        stream = resource_util.create_kinesis_stream(
            aws_client.kinesis, self.TEST_STREAM_KINESIS_API_GW
        )
        stream.wait_for()

        # create API Gateway and connect it to the target stream
        result = self.connect_api_gateway_to_kinesis(
            "test_gateway1", self.TEST_STREAM_KINESIS_API_GW
        )

        # generate test data
        test_data = {
            "records": [
                {"data": '{"foo": "bar1"}'},
                {"data": '{"foo": "bar2"}'},
                {"data": '{"foo": "bar3"}'},
            ]
        }

        url = path_based_url(
            api_id=result["id"],
            stage_name=self.TEST_STAGE_NAME,
            path=self.API_PATH_DATA_INBOUND,
        )

        # list Kinesis streams via API Gateway
        result = requests.get(url)
        result = json.loads(to_str(result.content))
        assert "StreamNames" in result

        # post test data to Kinesis via API Gateway
        result = requests.post(url, data=json.dumps(test_data))
        result = json.loads(to_str(result.content))
        assert 0 == result["FailedRecordCount"]
        assert len(test_data["records"]) == len(result["Records"])

        # clean up
        aws_client.kinesis.delete_stream(StreamName=self.TEST_STREAM_KINESIS_API_GW)

    def test_api_gateway_sqs_integration_with_event_source(self, aws_client):
        # create target SQS stream
        queue_name = f"queue-{short_uid()}"
        queue_url = resource_util.create_sqs_queue(queue_name)["QueueUrl"]

        # create API Gateway and connect it to the target queue
        result = connect_api_gateway_to_sqs(
            "test_gateway4",
            stage_name=self.TEST_STAGE_NAME,
            queue_arn=queue_name,
            path=self.API_PATH_DATA_INBOUND,
        )

        # create event source for sqs lambda processor
        self.create_lambda_function(aws_client.awslambda, self.TEST_LAMBDA_SQS_HANDLER_NAME)
        event_source_data = {
            "FunctionName": self.TEST_LAMBDA_SQS_HANDLER_NAME,
            "EventSourceArn": arns.sqs_queue_arn(queue_name),
            "Enabled": True,
        }
        add_event_source(event_source_data)

        # generate test data
        test_data = {"spam": "eggs & beans"}

        url = path_based_url(
            api_id=result["id"],
            stage_name=self.TEST_STAGE_NAME,
            path=self.API_PATH_DATA_INBOUND,
        )
        result = requests.post(url, data=json.dumps(test_data))
        assert 200 == result.status_code

        parsed_json = xmltodict.parse(result.content)
        result = parsed_json["SendMessageResponse"]["SendMessageResult"]

        body_md5 = result["MD5OfMessageBody"]

        assert "b639f52308afd65866c86f274c59033f" == body_md5

        # clean up
        aws_client.sqs.delete_queue(QueueUrl=queue_url)
        aws_client.awslambda.delete_function(FunctionName=self.TEST_LAMBDA_SQS_HANDLER_NAME)

    def test_api_gateway_sqs_integration(self, aws_client):
        # create target SQS stream
        queue_name = f"queue-{short_uid()}"
        resource_util.create_sqs_queue(queue_name)

        # create API Gateway and connect it to the target queue
        result = connect_api_gateway_to_sqs(
            "test_gateway4",
            stage_name=self.TEST_STAGE_NAME,
            queue_arn=queue_name,
            path=self.API_PATH_DATA_INBOUND,
        )

        # generate test data
        test_data = {"spam": "eggs"}

        url = path_based_url(
            api_id=result["id"],
            stage_name=self.TEST_STAGE_NAME,
            path=self.API_PATH_DATA_INBOUND,
        )
        result = requests.post(url, data=json.dumps(test_data))
        assert 200 == result.status_code

        messages = queries.sqs_receive_message(queue_name)["Messages"]
        assert 1 == len(messages)
        assert test_data == json.loads(base64.b64decode(messages[0]["Body"]))

    def test_update_rest_api_deployment(self, create_rest_apigw, aws_client):
        api_id, _, root = create_rest_apigw(name="test_gateway5")

        create_rest_resource_method(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=root,
            httpMethod="GET",
            authorizationType="none",
        )

        create_rest_api_integration(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=root,
            httpMethod="GET",
            type="HTTP",
            uri="http://httpbin.org/robots.txt",
            integrationHttpMethod="POST",
        )
        create_rest_api_integration_response(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=root,
            httpMethod="GET",
            statusCode="200",
            selectionPattern="foobar",
            responseTemplates={},
        )

        deployment_id, _ = create_rest_api_deployment(
            aws_client.apigateway, restApiId=api_id, description="my deployment"
        )
        patch_operations = [{"op": "replace", "path": "/description", "value": "new-description"}]
        deployment = update_rest_api_deployment(
            aws_client.apigateway,
            restApiId=api_id,
            deploymentId=deployment_id,
            patchOperations=patch_operations,
        )
        assert deployment["description"] == "new-description"

    def test_api_gateway_lambda_integration(
        self, create_rest_apigw, create_lambda_function, aws_client
    ):
        """
        API gateway to lambda integration test returns a response with the same body as the lambda
        function input event.
        """
        fn_name = f"test-{short_uid()}"
        create_lambda_function(
            func_name=fn_name,
            handler_file=TEST_LAMBDA_AWS_PROXY,
            runtime=Runtime.python3_9,
        )
        lambda_arn = arns.lambda_function_arn(fn_name)

        api_id, _, root = create_rest_apigw(name="aws lambda api")
        resource_id, _ = create_rest_resource(
            aws_client.apigateway, restApiId=api_id, parentId=root, pathPart="test"
        )

        # create method and integration
        create_rest_resource_method(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        create_rest_api_integration(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            integrationHttpMethod="GET",
            type="AWS_PROXY",
            uri=f"arn:aws:apigateway:{aws_stack.get_region()}:lambda:path//2015-03-31/function"
            f"s/{lambda_arn}/invocations",
        )

        url = api_invoke_url(api_id=api_id, stage="local", path="/test")
        response = requests.get(url)
        body = response.json()
        assert response.status_code == 200
        # authorizer contains an object that does not contain the authorizer type ('lambda', 'sns')
        # TODO this should not only be empty, but the key should not exist (like in aws)
        assert not body.get("requestContext").get("authorizer")

    @markers.parity.aws_validated
    def test_api_gateway_lambda_integration_aws_type(
        self, create_lambda_function, create_rest_apigw, aws_client
    ):
        region_name = aws_client.apigateway._client_config.region_name
        fn_name = f"test-{short_uid()}"
        create_lambda_function(
            func_name=fn_name,
            handler_file=TEST_LAMBDA_PYTHON_ECHO,
            runtime=Runtime.python3_9,
        )
        lambda_arn = aws_client.awslambda.get_function(FunctionName=fn_name)["Configuration"][
            "FunctionArn"
        ]

        api_id, _, root = create_rest_apigw(name="aws lambda api")
        resource_id, _ = create_rest_resource(
            aws_client.apigateway, restApiId=api_id, parentId=root, pathPart="test"
        )
        create_rest_resource_method(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            authorizationType="NONE",
        )
        create_rest_api_integration(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            integrationHttpMethod="POST",
            type="AWS",
            uri=f"arn:aws:apigateway:{region_name}:lambda:path//2015-03-31/functions/"
            f"{lambda_arn}/invocations",
            requestTemplates={
                "application/json": '#set($allParams = $input.params())\n{\n"body-json" : $input.json("$"),\n"params" : {\n#foreach($type in $allParams.keySet())\n    #set($params = $allParams.get($type))\n"$type" : {\n    #foreach($paramName in $params.keySet())\n    "$paramName" : "$util.escapeJavaScript($params.get($paramName))"\n        #if($foreach.hasNext),#end\n    #end\n}\n    #if($foreach.hasNext),#end\n#end\n},\n"stage-variables" : {\n#foreach($key in $stageVariables.keySet())\n"$key" : "$util.escapeJavaScript($stageVariables.get($key))"\n    #if($foreach.hasNext),#end\n#end\n},\n"context" : {\n    "api-id" : "$context.apiId",\n    "api-key" : "$context.identity.apiKey",\n    "http-method" : "$context.httpMethod",\n    "stage" : "$context.stage",\n    "source-ip" : "$context.identity.sourceIp",\n    "user-agent" : "$context.identity.userAgent",\n    "request-id" : "$context.requestId",\n    "resource-id" : "$context.resourceId",\n    "resource-path" : "$context.resourcePath"\n    }\n}\n'
            },
        )
        create_rest_api_method_response(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            statusCode="200",
            responseParameters={
                "method.response.header.Content-Type": False,
                "method.response.header.Access-Control-Allow-Origin": False,
            },
        )
        create_rest_api_integration_response(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            statusCode="200",
            responseTemplates={"text/html": "$input.path('$')"},
            responseParameters={
                "method.response.header.Access-Control-Allow-Origin": "'*'",
                "method.response.header.Content-Type": "'text/html'",
            },
        )
        deployment_id, _ = create_rest_api_deployment(aws_client.apigateway, restApiId=api_id)
        stage = create_rest_api_stage(
            aws_client.apigateway, restApiId=api_id, stageName="local", deploymentId=deployment_id
        )

        update_rest_api_stage(
            aws_client.apigateway,
            restApiId=api_id,
            stageName="local",
            patchOperations=[{"op": "replace", "path": "/cacheClusterEnabled", "value": "true"}],
        )
        aws_account_id = aws_client.sts.get_caller_identity()["Account"]
        source_arn = f"arn:aws:execute-api:{region_name}:{aws_account_id}:{api_id}/*/*/test"

        aws_client.awslambda.add_permission(
            FunctionName=lambda_arn,
            StatementId=str(short_uid()),
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=source_arn,
        )

        url = api_invoke_url(api_id, stage=stage, path="/test")
        response = requests.post(url, json={"test": "test"})

        assert response.headers["Content-Type"] == "text/html"
        assert response.headers["Access-Control-Allow-Origin"] == "*"

    @pytest.mark.parametrize("int_type", ["custom", "proxy"])
    def test_api_gateway_http_integrations(
        self, int_type, echo_http_server, monkeypatch, aws_client
    ):
        monkeypatch.setattr(config, "DISABLE_CUSTOM_CORS_APIGATEWAY", False)

        backend_base_url = echo_http_server
        backend_url = f"{backend_base_url}/{self.API_PATH_HTTP_BACKEND}"

        # create API Gateway and connect it to the HTTP_PROXY/HTTP backend
        result = self.connect_api_gateway_to_http(
            int_type, "test_gateway2", backend_url, path=self.API_PATH_HTTP_BACKEND
        )

        url = path_based_url(
            api_id=result["id"],
            stage_name=self.TEST_STAGE_NAME,
            path=self.API_PATH_HTTP_BACKEND,
        )

        # make sure CORS headers are present
        origin = "localhost"
        result = requests.options(url, headers={"origin": origin})
        assert result.status_code == 200
        assert re.match(result.headers["Access-Control-Allow-Origin"].replace("*", ".*"), origin)
        assert "POST" in result.headers["Access-Control-Allow-Methods"]
        assert "PATCH" in result.headers["Access-Control-Allow-Methods"]

        custom_result = json.dumps({"foo": "bar"})

        # make test GET request to gateway
        result = requests.get(url)
        assert 200 == result.status_code
        expected = custom_result if int_type == "custom" else "{}"
        assert expected == json.loads(to_str(result.content))["data"]

        # make test POST request to gateway
        data = json.dumps({"data": 123})
        result = requests.post(url, data=data)
        assert 200 == result.status_code
        expected = custom_result if int_type == "custom" else data
        assert expected == json.loads(to_str(result.content))["data"]

        # make test POST request with non-JSON content type
        data = "test=123"
        ctype = "application/x-www-form-urlencoded"
        result = requests.post(url, data=data, headers={"content-type": ctype})
        assert 200 == result.status_code
        content = json.loads(to_str(result.content))
        headers = CaseInsensitiveDict(content["headers"])
        expected = custom_result if int_type == "custom" else data
        assert expected == content["data"]
        assert ctype == headers["content-type"]

    @pytest.mark.parametrize("use_hostname", [True, False])
    @pytest.mark.parametrize("disable_custom_cors", [True, False])
    @pytest.mark.parametrize("origin", ["http://allowed", "http://denied"])
    def test_invoke_endpoint_cors_headers(
        self, use_hostname, disable_custom_cors, origin, monkeypatch, aws_client
    ):
        monkeypatch.setattr(config, "DISABLE_CUSTOM_CORS_APIGATEWAY", disable_custom_cors)
        monkeypatch.setattr(
            cors, "ALLOWED_CORS_ORIGINS", cors.ALLOWED_CORS_ORIGINS + ["http://allowed"]
        )

        responses = [
            {
                "statusCode": "200",
                "httpMethod": "OPTIONS",
                "responseParameters": {
                    "method.response.header.Access-Control-Allow-Origin": "'http://test.com'",
                    "method.response.header.Vary": "'Origin'",
                },
            }
        ]
        api_id = self.create_api_gateway_and_deploy(
            aws_client.apigateway,
            integration_type="MOCK",
            integration_responses=responses,
            stage_name=self.TEST_STAGE_NAME,
        )

        # invoke endpoint with Origin header
        endpoint = self._get_invoke_endpoint(
            api_id, stage=self.TEST_STAGE_NAME, path="/", use_hostname=use_hostname
        )
        response = requests.options(endpoint, headers={"Origin": origin})

        # assert response codes and CORS headers
        if disable_custom_cors:
            if origin == "http://allowed":
                assert response.status_code == 204
                assert "http://allowed" in response.headers["Access-Control-Allow-Origin"]
            else:
                assert response.status_code == 403
        else:
            assert response.status_code == 200
            assert "http://test.com" in response.headers["Access-Control-Allow-Origin"]

    def test_api_gateway_lambda_proxy_integration(self, aws_client):
        self._test_api_gateway_lambda_proxy_integration(
            aws_client.awslambda, self.TEST_LAMBDA_PROXY_BACKEND, self.API_PATH_LAMBDA_PROXY_BACKEND
        )

    def test_api_gateway_lambda_proxy_integration_with_path_param(self, aws_client):
        self._test_api_gateway_lambda_proxy_integration(
            aws_client.awslambda,
            self.TEST_LAMBDA_PROXY_BACKEND_WITH_PATH_PARAM,
            self.API_PATH_LAMBDA_PROXY_BACKEND_WITH_PATH_PARAM,
        )

    def test_api_gateway_lambda_proxy_integration_with_is_base_64_encoded(self, aws_client):
        # Test the case where `isBase64Encoded` is enabled.
        content = b"hello, please base64 encode me"

        def _mutate_data(data) -> None:
            data["return_is_base_64_encoded"] = True
            data["return_raw_body"] = base64.b64encode(content).decode("utf8")

        test_result = self._test_api_gateway_lambda_proxy_integration_no_asserts(
            aws_client.awslambda,
            self.TEST_LAMBDA_PROXY_BACKEND_WITH_IS_BASE64,
            self.API_PATH_LAMBDA_PROXY_BACKEND_WITH_IS_BASE64,
            data_mutator_fn=_mutate_data,
        )

        # Ensure that `invoke_rest_api_integration_backend` correctly decodes the base64 content
        assert test_result.result.status_code == 203
        assert test_result.result.content == content

    def _test_api_gateway_lambda_proxy_integration_no_asserts(
        self,
        lambda_client,
        fn_name: str,
        path: str,
        data_mutator_fn: Optional[Callable] = None,
    ) -> ApiGatewayLambdaProxyIntegrationTestResult:
        """
        Perform the setup needed to do a POST against a Lambda Proxy Integration;
        then execute the POST.

        :param data_mutator_fn: a Callable[[Dict], None] that lets us mutate the
          data dictionary before sending it off to the lambda.
        """
        self.create_lambda_function(lambda_client, fn_name)
        # create API Gateway and connect it to the Lambda proxy backend
        lambda_uri = arns.lambda_function_arn(fn_name)
        invocation_uri = "arn:aws:apigateway:%s:lambda:path/2015-03-31/functions/%s/invocations"
        target_uri = invocation_uri % (aws_stack.get_region(), lambda_uri)

        result = testutil.connect_api_gateway_to_http_with_lambda_proxy(
            "test_gateway2", target_uri, path=path, stage_name=self.TEST_STAGE_NAME
        )

        api_id = result["id"]
        path_map = get_rest_api_paths(api_id)
        _, resource = get_resource_for_path(path, path_map)

        # make test request to gateway and check response
        path_with_replace = path.replace("{test_param1}", "foo1")
        path_with_params = path_with_replace + "?foo=foo&bar=bar&bar=baz"

        url = path_based_url(api_id=api_id, stage_name=self.TEST_STAGE_NAME, path=path_with_params)

        # These values get read in `lambda_integration.py`
        data = {"return_status_code": 203, "return_headers": {"foo": "bar123"}}
        if data_mutator_fn:
            assert callable(data_mutator_fn)
            data_mutator_fn(data)
        result = requests.post(
            url,
            data=json.dumps(data),
            headers={"User-Agent": "python-requests/testing"},
        )

        return ApiGatewayLambdaProxyIntegrationTestResult(
            data=data,
            resource=resource,
            result=result,
            url=url,
            path_with_replace=path_with_replace,
        )

    def _test_api_gateway_lambda_proxy_integration(
        self,
        lambda_client,
        fn_name: str,
        path: str,
    ) -> None:
        test_result = self._test_api_gateway_lambda_proxy_integration_no_asserts(
            lambda_client, fn_name, path
        )
        data, resource, result, url, path_with_replace = test_result

        assert result.status_code == 203
        assert result.headers.get("foo") == "bar123"
        assert "set-cookie" in result.headers

        try:
            parsed_body = json.loads(to_str(result.content))
        except json.decoder.JSONDecodeError as e:
            raise Exception(
                "Couldn't json-decode content: {}".format(to_str(result.content))
            ) from e
        assert parsed_body.get("return_status_code") == 203
        assert parsed_body.get("return_headers") == {"foo": "bar123"}
        assert parsed_body.get("queryStringParameters") == {"foo": "foo", "bar": "baz"}

        request_context = parsed_body.get("requestContext")
        source_ip = request_context["identity"].pop("sourceIp")

        assert re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", source_ip)

        expected_path = "/" + self.TEST_STAGE_NAME + "/lambda/foo1"
        assert expected_path == request_context["path"]
        assert request_context.get("stageVariables") is None
        assert get_aws_account_id() == request_context["accountId"]
        assert resource.get("id") == request_context["resourceId"]
        assert self.TEST_STAGE_NAME == request_context["stage"]
        assert "python-requests/testing" == request_context["identity"]["userAgent"]
        assert "POST" == request_context["httpMethod"]
        assert "HTTP/1.1" == request_context["protocol"]
        assert "requestTimeEpoch" in request_context
        assert "requestTime" in request_context
        assert "requestId" in request_context

        # assert that header keys are lowercase (as in AWS)
        headers = parsed_body.get("headers") or {}
        header_names = list(headers.keys())
        assert "Host" in header_names
        assert "Content-Length" in header_names
        assert "User-Agent" in header_names

        result = requests.delete(url, data=json.dumps(data))
        assert 204 == result.status_code

        # send message with non-ASCII chars
        body_msg = "🙀 - 参よ"
        result = requests.post(url, data=json.dumps({"return_raw_body": body_msg}))
        assert body_msg == to_str(result.content)

        # send message with binary data
        binary_msg = b"\xff \xaa \x11"
        result = requests.post(url, data=binary_msg)
        result_content = json.loads(to_str(result.content))
        assert "/yCqIBE=" == result_content["body"]
        assert ["isBase64Encoded"]

    def test_api_gateway_lambda_proxy_integration_any_method(self, aws_client):
        self._test_api_gateway_lambda_proxy_integration_any_method(
            aws_client.awslambda,
            self.TEST_LAMBDA_PROXY_BACKEND_ANY_METHOD,
            self.API_PATH_LAMBDA_PROXY_BACKEND_ANY_METHOD,
        )

    def test_api_gateway_lambda_proxy_integration_any_method_with_path_param(self, aws_client):
        self._test_api_gateway_lambda_proxy_integration_any_method(
            aws_client.awslambda,
            self.TEST_LAMBDA_PROXY_BACKEND_ANY_METHOD_WITH_PATH_PARAM,
            self.API_PATH_LAMBDA_PROXY_BACKEND_ANY_METHOD_WITH_PATH_PARAM,
        )

    def test_api_gateway_lambda_asynchronous_invocation(
        self, create_rest_apigw, create_lambda_function, aws_client
    ):
        api_gateway_name = f"api_gateway_{short_uid()}"
        rest_api_id, _, _ = create_rest_apigw(name=api_gateway_name)

        fn_name = f"test-{short_uid()}"
        create_lambda_function(
            handler_file=TEST_LAMBDA_NODEJS, func_name=fn_name, runtime=Runtime.nodejs16_x
        )
        lambda_arn = arns.lambda_function_arn(fn_name)

        spec_file = load_file(TEST_IMPORT_REST_API_ASYNC_LAMBDA)
        spec_file = spec_file.replace("${lambda_invocation_arn}", lambda_arn)

        aws_client.apigateway.put_rest_api(restApiId=rest_api_id, body=spec_file, mode="overwrite")
        url = path_based_url(api_id=rest_api_id, stage_name="latest", path="/wait/3")
        result = requests.get(url)
        assert result.status_code == 200
        assert result.content == b""

    def test_api_gateway_mock_integration(self, create_rest_apigw, aws_client):
        rest_api_name = f"apigw-{short_uid()}"
        rest_api_id, _, _ = create_rest_apigw(name=rest_api_name)

        spec_file = load_file(TEST_IMPORT_MOCK_INTEGRATION)
        aws_client.apigateway.put_rest_api(restApiId=rest_api_id, body=spec_file, mode="overwrite")

        url = path_based_url(api_id=rest_api_id, stage_name="latest", path="/echo/foobar")
        response = requests.get(url)
        assert response._content == b'{"echo": "foobar", "response": "mocked"}'

    @pytest.mark.xfail(reason="Behaviour is not AWS compliant, need to recreate this test")
    def test_api_gateway_authorizer_crud(self, aws_client):
        authorizer = aws_client.apigateway.create_authorizer(
            restApiId=self.TEST_API_GATEWAY_ID, **self.TEST_API_GATEWAY_AUTHORIZER
        )

        authorizer_id = authorizer.get("id")

        create_result = aws_client.apigateway.get_authorizer(
            restApiId=self.TEST_API_GATEWAY_ID, authorizerId=authorizer_id
        )

        # ignore boto3 stuff
        del create_result["ResponseMetadata"]

        create_expected = clone(self.TEST_API_GATEWAY_AUTHORIZER)
        create_expected["id"] = authorizer_id

        assert create_expected == create_result

        aws_client.apigateway.update_authorizer(
            restApiId=self.TEST_API_GATEWAY_ID,
            authorizerId=authorizer_id,
            patchOperations=self.TEST_API_GATEWAY_AUTHORIZER_OPS,
        )

        update_result = aws_client.apigateway.get_authorizer(
            restApiId=self.TEST_API_GATEWAY_ID, authorizerId=authorizer_id
        )

        # ignore boto3 stuff
        del update_result["ResponseMetadata"]

        update_expected = apply_patch(create_expected, self.TEST_API_GATEWAY_AUTHORIZER_OPS)

        assert update_expected == update_result

        aws_client.apigateway.delete_authorizer(
            restApiId=self.TEST_API_GATEWAY_ID, authorizerId=authorizer_id
        )

        with pytest.raises(Exception):
            aws_client.apigateway.get_authorizer(
                restApiId=self.TEST_API_GATEWAY_ID, authorizerId=authorizer_id
            )

    def test_malformed_response_apigw_invocation(self, create_lambda_function, aws_client):
        lambda_name = f"test_lambda_{short_uid()}"
        lambda_resource = "/api/v1/{proxy+}"
        lambda_path = "/api/v1/hello/world"

        create_lambda_function(
            func_name=lambda_name,
            zip_file=testutil.create_zip_file(TEST_LAMBDA_NODEJS_APIGW_502, get_content=True),
            runtime=Runtime.nodejs16_x,
            handler="apigw_502.handler",
        )

        lambda_uri = arns.lambda_function_arn(lambda_name)
        target_uri = f"arn:aws:apigateway:{aws_stack.get_region()}:lambda:path/2015-03-31/functions/{lambda_uri}/invocations"
        result = testutil.connect_api_gateway_to_http_with_lambda_proxy(
            "test_gateway",
            target_uri,
            path=lambda_resource,
            stage_name="testing",
        )
        api_id = result["id"]
        url = path_based_url(api_id=api_id, stage_name="testing", path=lambda_path)
        result = requests.get(url)

        assert result.status_code == 502
        assert result.headers.get("Content-Type") == "application/json"
        assert json.loads(result.content)["message"] == "Internal server error"

    def test_api_gateway_handle_domain_name(self, aws_client):
        domain_name = f"{short_uid()}.example.com"
        apigw_client = aws_client.apigateway
        rs = apigw_client.create_domain_name(domainName=domain_name)
        assert 201 == rs["ResponseMetadata"]["HTTPStatusCode"]
        rs = apigw_client.get_domain_name(domainName=domain_name)
        assert 200 == rs["ResponseMetadata"]["HTTPStatusCode"]
        assert domain_name == rs["domainName"]
        apigw_client.delete_domain_name(domainName=domain_name)

    def _test_api_gateway_lambda_proxy_integration_any_method(self, lambda_client, fn_name, path):
        self.create_lambda_function(lambda_client, fn_name)

        # create API Gateway and connect it to the Lambda proxy backend
        lambda_uri = arns.lambda_function_arn(fn_name)
        target_uri = arns.apigateway_invocations_arn(lambda_uri)

        result = testutil.connect_api_gateway_to_http_with_lambda_proxy(
            "test_gateway3",
            target_uri,
            methods=["ANY"],
            path=path,
            stage_name=self.TEST_STAGE_NAME,
        )

        # make test request to gateway and check response
        path = path.replace("{test_param1}", "foo1")
        url = path_based_url(api_id=result["id"], stage_name=self.TEST_STAGE_NAME, path=path)
        data = {}

        for method in ("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
            body = json.dumps(data) if method in ("POST", "PUT", "PATCH") else None
            result = getattr(requests, method.lower())(url, data=body)
            if method != "DELETE":
                assert 200 == result.status_code
                parsed_body = json.loads(to_str(result.content))
                assert method == parsed_body.get("httpMethod")
            else:
                assert 204 == result.status_code

    def test_apigateway_with_custom_authorization_method(self, create_rest_apigw, aws_client):

        # create Lambda function
        lambda_name = f"apigw-lambda-{short_uid()}"
        self.create_lambda_function(aws_client.awslambda, lambda_name)
        lambda_uri = arns.lambda_function_arn(lambda_name)

        # create REST API
        api_id, _, _ = create_rest_apigw(name="test-api")
        root_res_id = aws_client.apigateway.get_resources(restApiId=api_id)["items"][0]["id"]

        # create authorizer at root resource
        authorizer = aws_client.apigateway.create_authorizer(
            restApiId=api_id,
            name="lambda_authorizer",
            type="TOKEN",
            authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/ \
                2015-03-31/functions/{}/invocations".format(
                lambda_uri
            ),
            identitySource="method.request.header.Auth",
        )

        # create method with custom authorizer
        is_api_key_required = True
        method_response = aws_client.apigateway.put_method(
            restApiId=api_id,
            resourceId=root_res_id,
            httpMethod="GET",
            authorizationType="CUSTOM",
            authorizerId=authorizer["id"],
            apiKeyRequired=is_api_key_required,
        )

        assert authorizer["id"] == method_response["authorizerId"]

        # clean up
        aws_client.awslambda.delete_function(FunctionName=lambda_name)

    def test_base_path_mapping(self, create_rest_apigw, aws_client):
        rest_api_id, _, _ = create_rest_apigw(name="my_api", description="this is my api")

        # CREATE
        domain_name = "domain1.example.com"
        aws_client.apigateway.create_domain_name(domainName=domain_name)
        root_res_id = aws_client.apigateway.get_resources(restApiId=rest_api_id)["items"][0]["id"]
        res_id = aws_client.apigateway.create_resource(
            restApiId=rest_api_id, parentId=root_res_id, pathPart="path"
        )["id"]
        aws_client.apigateway.put_method(
            restApiId=rest_api_id, resourceId=res_id, httpMethod="GET", authorizationType="NONE"
        )
        aws_client.apigateway.put_integration(
            restApiId=rest_api_id, resourceId=res_id, httpMethod="GET", type="MOCK"
        )
        depl_id = aws_client.apigateway.create_deployment(restApiId=rest_api_id)["id"]
        aws_client.apigateway.create_stage(
            restApiId=rest_api_id, deploymentId=depl_id, stageName="dev"
        )
        base_path = "foo"
        result = aws_client.apigateway.create_base_path_mapping(
            domainName=domain_name,
            basePath=base_path,
            restApiId=rest_api_id,
            stage="dev",
        )
        assert result["ResponseMetadata"]["HTTPStatusCode"] in [200, 201]

        # LIST
        result = aws_client.apigateway.get_base_path_mappings(domainName=domain_name)
        assert 200 == result["ResponseMetadata"]["HTTPStatusCode"]
        expected = {"basePath": base_path, "restApiId": rest_api_id, "stage": "dev"}
        assert [expected] == result["items"]

        # GET
        result = aws_client.apigateway.get_base_path_mapping(
            domainName=domain_name, basePath=base_path
        )
        assert 200 == result["ResponseMetadata"]["HTTPStatusCode"]
        assert expected == select_attributes(result, ["basePath", "restApiId", "stage"])

        # UPDATE
        result = aws_client.apigateway.update_base_path_mapping(
            domainName=domain_name, basePath=base_path, patchOperations=[]
        )
        assert 200 == result["ResponseMetadata"]["HTTPStatusCode"]

        # DELETE
        aws_client.apigateway.delete_base_path_mapping(domainName=domain_name, basePath=base_path)
        with pytest.raises(Exception):
            aws_client.apigateway.get_base_path_mapping(domainName=domain_name, basePath=base_path)
        with pytest.raises(Exception):
            aws_client.apigateway.delete_base_path_mapping(
                domainName=domain_name, basePath=base_path
            )

    def test_base_path_mapping_root(self, aws_client):
        client = aws_client.apigateway
        response = client.create_rest_api(name="my_api2", description="this is my api")
        rest_api_id = response["id"]

        # CREATE
        domain_name = "domain2.example.com"
        client.create_domain_name(domainName=domain_name)
        root_res_id = client.get_resources(restApiId=rest_api_id)["items"][0]["id"]
        res_id = client.create_resource(
            restApiId=rest_api_id, parentId=root_res_id, pathPart="path"
        )["id"]
        client.put_method(
            restApiId=rest_api_id, resourceId=res_id, httpMethod="GET", authorizationType="NONE"
        )
        client.put_integration(
            restApiId=rest_api_id, resourceId=res_id, httpMethod="GET", type="MOCK"
        )
        depl_id = client.create_deployment(restApiId=rest_api_id)["id"]
        client.create_stage(restApiId=rest_api_id, deploymentId=depl_id, stageName="dev")
        result = client.create_base_path_mapping(
            domainName=domain_name,
            basePath="",
            restApiId=rest_api_id,
            stage="dev",
        )
        assert result["ResponseMetadata"]["HTTPStatusCode"] in [200, 201]

        base_path = "(none)"
        # LIST
        result = client.get_base_path_mappings(domainName=domain_name)
        assert 200 == result["ResponseMetadata"]["HTTPStatusCode"]
        expected = {"basePath": "(none)", "restApiId": rest_api_id, "stage": "dev"}
        assert [expected] == result["items"]

        # GET
        result = client.get_base_path_mapping(domainName=domain_name, basePath=base_path)
        assert 200 == result["ResponseMetadata"]["HTTPStatusCode"]
        assert expected == select_attributes(result, ["basePath", "restApiId", "stage"])

        # UPDATE
        result = client.update_base_path_mapping(
            domainName=domain_name, basePath=base_path, patchOperations=[]
        )
        assert 200 == result["ResponseMetadata"]["HTTPStatusCode"]

        # DELETE
        client.delete_base_path_mapping(domainName=domain_name, basePath=base_path)
        with pytest.raises(Exception):
            client.get_base_path_mapping(domainName=domain_name, basePath=base_path)
        with pytest.raises(Exception):
            client.delete_base_path_mapping(domainName=domain_name, basePath=base_path)

    def test_api_account(self, create_rest_apigw, aws_client):
        rest_api_id, _, _ = create_rest_apigw(name="my_api", description="test 123")

        result = aws_client.apigateway.get_account()
        assert "UsagePlans" in result["features"]
        result = aws_client.apigateway.update_account(
            patchOperations=[{"op": "add", "path": "/features/-", "value": "foobar"}]
        )
        assert "foobar" in result["features"]

    def test_put_integration_dynamodb_proxy_validation_without_request_template(self, aws_client):
        api_id = self.create_api_gateway_and_deploy(aws_client.apigateway)
        url = path_based_url(api_id=api_id, stage_name="staging", path="/")
        response = requests.put(
            url,
            json.dumps({"id": "id1", "data": "foobar123"}),
        )

        assert 400 == response.status_code

    def test_put_integration_dynamodb_proxy_validation_with_request_template(self, aws_client):
        request_templates = {
            "application/json": json.dumps(
                {
                    "TableName": "MusicCollection",
                    "Item": {
                        "id": {"S": "$input.path('id')"},
                        "data": {"S": "$input.path('data')"},
                    },
                }
            )
        }

        api_id = self.create_api_gateway_and_deploy(
            aws_client.apigateway, request_templates=request_templates
        )
        url = path_based_url(api_id=api_id, stage_name="staging", path="/")

        response = requests.put(
            url,
            json.dumps({"id": "id1", "data": "foobar123"}),
        )

        assert 200 == response.status_code
        dynamo_client = aws_stack.connect_to_resource("dynamodb")
        table = dynamo_client.Table("MusicCollection")
        result = table.get_item(Key={"id": "id1"})
        assert "foobar123" == result["Item"]["data"]

    def test_multiple_api_keys_validate(self, aws_client):
        request_templates = {
            "application/json": json.dumps(
                {
                    "TableName": "MusicCollection",
                    "Item": {
                        "id": {"S": "$input.path('id')"},
                        "data": {"S": "$input.path('data')"},
                    },
                }
            )
        }

        api_id = self.create_api_gateway_and_deploy(
            aws_client.apigateway, request_templates=request_templates, is_api_key_required=True
        )
        url = path_based_url(api_id=api_id, stage_name="staging", path="/")

        # Create multiple usage plans
        usage_plan_ids = []
        for i in range(2):
            payload = {
                "name": f"APIKEYTEST-PLAN-{i}",
                "description": "Description",
                "quota": {"limit": 10, "period": "DAY", "offset": 0},
                "throttle": {"rateLimit": 2, "burstLimit": 1},
                "apiStages": [{"apiId": api_id, "stage": "staging"}],
                "tags": {"tag_key": "tag_value"},
            }
            usage_plan_ids.append(aws_client.apigateway.create_usage_plan(**payload)["id"])

        api_keys = []
        key_type = "API_KEY"
        # Create multiple API Keys in each usage plan
        for usage_plan_id, i in itertools.product(usage_plan_ids, range(2)):
            api_key = aws_client.apigateway.create_api_key(
                name=f"testMultipleApiKeys{i}", enabled=True
            )
            payload = {
                "usagePlanId": usage_plan_id,
                "keyId": api_key["id"],
                "keyType": key_type,
            }
            aws_client.apigateway.create_usage_plan_key(**payload)
            api_keys.append(api_key["value"])

        response = requests.put(
            url,
            json.dumps({"id": "id1", "data": "foobar123"}),
        )
        # when the api key is not passed as part of the header
        assert 403 == response.status_code

        # check that all API keys work
        for key in api_keys:
            response = requests.put(
                url,
                json.dumps({"id": "id1", "data": "foobar123"}),
                headers={"X-API-Key": key},
            )
            # when the api key is passed as part of the header
            assert 200 == response.status_code

        for usage_plan_id in usage_plan_ids:
            aws_client.apigateway.delete_usage_plan(usagePlanId=usage_plan_id)

    @markers.parity.aws_validated
    @pytest.mark.parametrize("action", ["StartExecution", "DeleteStateMachine"])
    def test_apigateway_with_step_function_integration(
        self,
        action,
        create_lambda_function,
        create_rest_apigw,
        create_iam_role_with_policy,
        aws_client,
    ):
        region_name = aws_client.apigateway._client_config.region_name
        aws_account_id = aws_client.sts.get_caller_identity()["Account"]

        # create lambda
        fn_name = f"lambda-sfn-apigw-{short_uid()}"
        create_lambda_function(
            handler_file=TEST_LAMBDA_PYTHON_ECHO,
            func_name=fn_name,
            runtime=Runtime.python3_9,
        )
        lambda_arn = arns.lambda_function_arn(
            function_name=fn_name, account_id=aws_account_id, region_name=region_name
        )

        # create state machine and permissions for step function to invoke lambda
        role_name = f"sfn_role-{short_uid()}"
        role_arn = arns.role_arn(role_name, account_id=aws_account_id)
        create_iam_role_with_policy(
            RoleName=role_name,
            PolicyName=f"sfn-role-policy-{short_uid()}",
            RoleDefinition=STEPFUNCTIONS_ASSUME_ROLE_POLICY,
            PolicyDefinition=APIGATEWAY_LAMBDA_POLICY,
        )

        state_machine_name = f"test-{short_uid()}"
        state_machine_def = {
            "Comment": "Hello World example",
            "StartAt": "step1",
            "States": {
                "step1": {"Type": "Task", "Resource": "__tbd__", "End": True},
            },
        }
        state_machine_def["States"]["step1"]["Resource"] = lambda_arn
        result = aws_client.stepfunctions.create_state_machine(
            name=state_machine_name,
            definition=json.dumps(state_machine_def),
            roleArn=role_arn,
            type="EXPRESS",
        )
        sm_arn = result["stateMachineArn"]

        # create REST API with integrations
        rest_api, _, root_id = create_rest_apigw(
            name=f"test-{short_uid()}", description="test-step-function-integration"
        )
        aws_client.apigateway.put_method(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="POST",
            authorizationType="NONE",
        )
        create_rest_api_method_response(
            aws_client.apigateway,
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="POST",
            statusCode="200",
        )

        # give permission to api gateway to invoke step function
        uri = f"arn:aws:apigateway:{region_name}:states:action/{action}"
        assume_role_arn = create_iam_role_with_policy(
            RoleName=f"role-apigw-{short_uid()}",
            PolicyName=f"policy-apigw-{short_uid()}",
            RoleDefinition=APIGATEWAY_ASSUME_ROLE_POLICY,
            PolicyDefinition=APIGATEWAY_STEPFUNCTIONS_POLICY,
        )

        def _prepare_integration(request_template=None, response_template=None):
            aws_client.apigateway.put_integration(
                restApiId=rest_api,
                resourceId=root_id,
                httpMethod="POST",
                integrationHttpMethod="POST",
                type="AWS",
                uri=uri,
                credentials=assume_role_arn,
                requestTemplates=request_template,
            )

            aws_client.apigateway.put_integration_response(
                restApiId=rest_api,
                resourceId=root_id,
                selectionPattern="",
                responseTemplates=response_template,
                httpMethod="POST",
                statusCode="200",
            )

        test_data = {"test": "test-value"}
        url = api_invoke_url(api_id=rest_api, stage="dev", path="/")

        req_template = {
            "application/json": """
            {
            "input": "$util.escapeJavaScript($input.json('$'))",
            "stateMachineArn": "%s"
            }
            """
            % sm_arn
        }
        match action:
            case "StartExecution":
                _prepare_integration(req_template, response_template={})
                aws_client.apigateway.create_deployment(restApiId=rest_api, stageName="dev")

                # invoke stepfunction via API GW, assert results
                def _invoke_start_step_function():
                    resp = requests.post(url, data=json.dumps(test_data))
                    assert resp.ok
                    content = json.loads(to_str(resp.content.decode()))
                    assert "executionArn" in content
                    assert "startDate" in content

                retry(_invoke_start_step_function, retries=15, sleep=0.8)

            case "StartSyncExecution":
                resp_template = {APPLICATION_JSON: "$input.path('$.output')"}
                _prepare_integration(req_template, resp_template)
                aws_client.apigateway.create_deployment(restApiId=rest_api, stageName="dev")
                input_data = {"input": json.dumps(test_data), "name": "MyExecution"}

                def _invoke_start_sync_step_function():
                    input_data["name"] += "1"
                    resp = requests.post(url, data=json.dumps(input_data))
                    assert resp.ok
                    content = json.loads(to_str(resp.content.decode()))
                    assert test_data == content

                retry(_invoke_start_sync_step_function, retries=15, sleep=0.8)

            case "DeleteStateMachine":
                _prepare_integration({}, {})
                aws_client.apigateway.create_deployment(restApiId=rest_api, stageName="dev")

                def _invoke_step_function():
                    resp = requests.post(url, data=json.dumps({"stateMachineArn": sm_arn}))
                    assert resp.ok

                retry(_invoke_step_function, retries=15, sleep=0.8)

    def test_api_gateway_http_integration_with_path_request_parameter(
        self, create_rest_apigw, echo_http_server, aws_client
    ):
        # start test HTTP backend
        backend_base_url = echo_http_server
        backend_url = backend_base_url + "/person/{id}"

        # create rest api
        api_id, _, _ = create_rest_apigw(name="test")
        parent_response = aws_client.apigateway.get_resources(restApiId=api_id)
        parent_id = parent_response["items"][0]["id"]
        resource_1 = aws_client.apigateway.create_resource(
            restApiId=api_id, parentId=parent_id, pathPart="person"
        )
        resource_1_id = resource_1["id"]
        resource_2 = aws_client.apigateway.create_resource(
            restApiId=api_id, parentId=resource_1_id, pathPart="{id}"
        )
        resource_2_id = resource_2["id"]
        aws_client.apigateway.put_method(
            restApiId=api_id,
            resourceId=resource_2_id,
            httpMethod="GET",
            authorizationType="NONE",
            apiKeyRequired=False,
            requestParameters={"method.request.path.id": True},
        )
        aws_client.apigateway.put_integration(
            restApiId=api_id,
            resourceId=resource_2_id,
            httpMethod="GET",
            integrationHttpMethod="GET",
            type="HTTP",
            uri=backend_url,
            timeoutInMillis=3000,
            contentHandling="CONVERT_TO_BINARY",
            requestParameters={"integration.request.path.id": "method.request.path.id"},
        )
        aws_client.apigateway.create_deployment(restApiId=api_id, stageName="test")

        def _test_invoke(url):
            result = requests.get(url)
            content = json.loads(to_str(result.content))
            assert 200 == result.status_code
            assert re.search(
                "http://.*localhost.*/person/123",
                content["url"],
            )

        for use_hostname in [True, False]:
            for use_ssl in [True, False] if use_hostname else [False]:
                url = self._get_invoke_endpoint(
                    api_id,
                    stage="test",
                    path="/person/123",
                    use_hostname=use_hostname,
                    use_ssl=use_ssl,
                )
                _test_invoke(url)

    def _get_invoke_endpoint(
        self, api_id, stage="test", path="/", use_hostname=False, use_ssl=False
    ):
        path = path or "/"
        path = path if path.startswith(path) else f"/{path}"
        proto = "https" if use_ssl else "http"
        if use_hostname:
            return (
                f"{proto}://{api_id}.execute-api.{LOCALHOST_HOSTNAME}:{config.EDGE_PORT}/"
                f"{stage}{path}"
            )
        return (
            f"{proto}://localhost:{config.EDGE_PORT}/restapis/{api_id}/{stage}/_user_request_{path}"
        )

    def test_api_gateway_s3_get_integration(self, create_rest_apigw, aws_client):
        s3_client = aws_client.s3

        bucket_name = f"test-bucket-{short_uid()}"
        apigateway_name = f"test-api-{short_uid()}"
        object_name = "test.json"
        object_content = '{ "success": "true" }'
        object_content_type = "application/json"

        api_id, _, _ = create_rest_apigw(name=apigateway_name)

        try:
            resource_util.get_or_create_bucket(bucket_name)
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=object_content,
                ContentType=object_content_type,
            )

            self.connect_api_gateway_to_s3(
                aws_client.apigateway, bucket_name, object_name, api_id, "GET"
            )

            aws_client.apigateway.create_deployment(restApiId=api_id, stageName="test")
            url = path_based_url(api_id, "test", f"/{object_name}")
            result = requests.get(url)
            assert 200 == result.status_code
            assert object_content == result.text
            assert object_content_type == result.headers["content-type"]
        finally:
            s3_client.delete_object(Bucket=bucket_name, Key=object_name)
            s3_client.delete_bucket(Bucket=bucket_name)

    def test_api_mock_integration_response_params(self, aws_client):
        resps = [
            {
                "statusCode": "204",
                "httpMethod": "OPTIONS",
                "responseParameters": {
                    "method.response.header.Access-Control-Allow-Methods": "'POST,OPTIONS'",
                    "method.response.header.Vary": "'Origin'",
                },
            }
        ]
        api_id = self.create_api_gateway_and_deploy(
            aws_client.apigateway, integration_type="MOCK", integration_responses=resps
        )

        url = path_based_url(api_id=api_id, stage_name=self.TEST_STAGE_NAME, path="/")
        result = requests.options(url)
        assert result.ok
        assert "Origin" == result.headers.get("vary")
        assert "POST,OPTIONS" == result.headers.get("Access-Control-Allow-Methods")

    def test_api_gateway_update_resource_path_part(self, create_rest_apigw, aws_client):
        api_id, _, _ = create_rest_apigw(name="test-api")
        root_res_id = aws_client.apigateway.get_resources(restApiId=api_id)["items"][0]["id"]
        api_resource = aws_client.apigateway.create_resource(
            restApiId=api_id, parentId=root_res_id, pathPart="test"
        )

        response = aws_client.apigateway.update_resource(
            restApiId=api_id,
            resourceId=api_resource.get("id"),
            patchOperations=[
                {"op": "replace", "path": "/pathPart", "value": "demo1"},
            ],
        )
        assert response.get("pathPart") == "demo1"
        response = aws_client.apigateway.get_resource(
            restApiId=api_id, resourceId=api_resource.get("id")
        )
        assert response.get("pathPart") == "demo1"

    def test_response_headers_invocation_with_apigw(self, create_lambda_function, aws_client):
        lambda_name = f"test_lambda_{short_uid()}"
        lambda_resource = "/api/v1/{proxy+}"
        lambda_path = "/api/v1/hello/world"

        create_lambda_function(
            func_name=lambda_name,
            zip_file=testutil.create_zip_file(
                TEST_LAMBDA_NODEJS_APIGW_INTEGRATION, get_content=True
            ),
            runtime=Runtime.nodejs16_x,
            handler="apigw_integration.handler",
        )

        lambda_uri = arns.lambda_function_arn(lambda_name)
        target_uri = f"arn:aws:apigateway:{aws_stack.get_region()}:lambda:path/2015-03-31/functions/{lambda_uri}/invocations"
        result = testutil.connect_api_gateway_to_http_with_lambda_proxy(
            "test_gateway",
            target_uri,
            path=lambda_resource,
            stage_name=self.TEST_STAGE_NAME,
        )
        api_id = result["id"]
        url = path_based_url(api_id=api_id, stage_name=self.TEST_STAGE_NAME, path=lambda_path)
        result = requests.get(url)

        assert result.status_code == 300
        assert result.headers["Content-Type"] == "application/xml"
        body = xmltodict.parse(result.content)
        assert body.get("message") == "completed"

    # =====================================================================
    # Helper methods
    # =====================================================================

    def connect_api_gateway_to_s3(self, apigw_client, bucket_name, file_name, api_id, method):
        """Connects the root resource of an api gateway to the given object of an s3 bucket."""
        s3_uri = "arn:aws:apigateway:{}:s3:path/{}/{{proxy}}".format(
            aws_stack.get_region(), bucket_name
        )

        test_role = "test-s3-role"
        role_arn = arns.role_arn(role_name=test_role)
        resources = apigw_client.get_resources(restApiId=api_id)
        # using the root resource '/' directly for this test
        root_resource_id = resources["items"][0]["id"]
        proxy_resource = apigw_client.create_resource(
            restApiId=api_id, parentId=root_resource_id, pathPart="{proxy+}"
        )
        apigw_client.put_method(
            restApiId=api_id,
            resourceId=proxy_resource["id"],
            httpMethod=method,
            authorizationType="NONE",
            apiKeyRequired=False,
            requestParameters={},
        )
        apigw_client.put_integration(
            restApiId=api_id,
            resourceId=proxy_resource["id"],
            httpMethod=method,
            type="AWS",
            integrationHttpMethod=method,
            uri=s3_uri,
            credentials=role_arn,
            requestParameters={"integration.request.path.proxy": "method.request.path.proxy"},
        )

    def connect_api_gateway_to_kinesis(self, gateway_name, kinesis_stream):
        template = self.APIGATEWAY_DATA_INBOUND_TEMPLATE % kinesis_stream
        resource_path = self.API_PATH_DATA_INBOUND.replace("/", "")
        resources = {
            resource_path: [
                {
                    "httpMethod": "POST",
                    "authorizationType": "NONE",
                    "requestModels": {"application/json": "Empty"},
                    "integrations": [
                        {
                            "type": "AWS",
                            "uri": "arn:aws:apigateway:%s:kinesis:action/PutRecords"
                            % aws_stack.get_region(),
                            "requestTemplates": {"application/json": template},
                        }
                    ],
                },
                {
                    "httpMethod": "GET",
                    "authorizationType": "NONE",
                    "requestModels": {"application/json": "Empty"},
                    "integrations": [
                        {
                            "type": "AWS",
                            "uri": "arn:aws:apigateway:%s:kinesis:action/ListStreams"
                            % aws_stack.get_region(),
                            "requestTemplates": {"application/json": "{}"},
                        }
                    ],
                },
            ]
        }
        return resource_util.create_api_gateway(
            name=gateway_name, resources=resources, stage_name=self.TEST_STAGE_NAME
        )

    def connect_api_gateway_to_http(
        self, int_type, gateway_name, target_url, methods=None, path=None
    ):
        if methods is None:
            methods = []
        if not methods:
            methods = ["GET", "POST"]
        if not path:
            path = "/"
        resources = {}
        resource_path = path.replace("/", "")
        req_templates = (
            {"application/json": json.dumps({"foo": "bar"})} if int_type == "custom" else {}
        )
        resources[resource_path] = [
            {
                "httpMethod": method,
                "integrations": [
                    {
                        "type": "HTTP" if int_type == "custom" else "HTTP_PROXY",
                        "uri": target_url,
                        "requestTemplates": req_templates,
                        "responseTemplates": {},
                    }
                ],
            }
            for method in methods
        ]
        return resource_util.create_api_gateway(
            name=gateway_name, resources=resources, stage_name=self.TEST_STAGE_NAME
        )

    @staticmethod
    def create_lambda_function(lambda_client, fn_name):
        testutil.create_lambda_function(handler_file=TEST_LAMBDA_PYTHON, func_name=fn_name)
        lambda_client.get_waiter("function_active_v2").wait(FunctionName=fn_name)

    def test_apigw_test_invoke_method_api(
        self, create_rest_apigw, create_lambda_function, aws_client
    ):
        # create test Lambda
        fn_name = f"test-{short_uid()}"
        create_lambda_function(
            handler_file=TEST_LAMBDA_NODEJS, func_name=fn_name, runtime=Runtime.nodejs16_x
        )
        lambda_arn_1 = arns.lambda_function_arn(fn_name)

        # create REST API and test resource
        rest_api_id, _, _ = create_rest_apigw(name="test", description="test")
        root_resource = aws_client.apigateway.get_resources(restApiId=rest_api_id)
        resource = aws_client.apigateway.create_resource(
            restApiId=rest_api_id, parentId=root_resource["items"][0]["id"], pathPart="foo"
        )

        # create method and integration
        aws_client.apigateway.put_method(
            restApiId=rest_api_id,
            resourceId=resource["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )
        aws_client.apigateway.put_integration(
            restApiId=rest_api_id,
            resourceId=resource["id"],
            httpMethod="GET",
            integrationHttpMethod="GET",
            type="AWS",
            uri="arn:aws:apigateway:{}:lambda:path//2015-03-31/functions/{}/invocations".format(
                aws_stack.get_region(), lambda_arn_1
            ),
        )

        # run test_invoke_method API #1
        response = aws_client.apigateway.test_invoke_method(
            restApiId=rest_api_id,
            resourceId=resource["id"],
            httpMethod="GET",
            pathWithQueryString="/foo",
        )
        assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
        assert 200 == response.get("status")
        assert "response from" in json.loads(response.get("body")).get("body")

        # run test_invoke_method API #2
        response = aws_client.apigateway.test_invoke_method(
            restApiId=rest_api_id,
            resourceId=resource["id"],
            httpMethod="GET",
            pathWithQueryString="/foo",
            body='{"test": "val123"}',
            headers={"content-type": "application/json"},
        )
        assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
        assert 200 == response.get("status")
        assert "response from" in json.loads(response.get("body")).get("body")
        assert "val123" in json.loads(response.get("body")).get("body")

    @staticmethod
    def create_api_gateway_and_deploy(
        apigw_client,
        request_templates=None,
        response_templates=None,
        is_api_key_required=False,
        integration_type=None,
        integration_responses=None,
        stage_name="staging",
    ):
        response_templates = response_templates or {}
        request_templates = request_templates or {}
        integration_type = integration_type or "AWS"
        response = apigw_client.create_rest_api(name="my_api", description="this is my api")
        api_id = response["id"]
        resources = apigw_client.get_resources(restApiId=api_id)
        root_resources = [resource for resource in resources["items"] if resource["path"] == "/"]
        root_id = root_resources[0]["id"]

        kwargs = {}
        if integration_type == "AWS":
            resource_util.create_dynamodb_table("MusicCollection", partition_key="id")
            kwargs[
                "uri"
            ] = "arn:aws:apigateway:us-east-1:dynamodb:action/PutItem&Table=MusicCollection"

        if not integration_responses:
            integration_responses = [{"httpMethod": "PUT", "statusCode": "200"}]

        for resp_details in integration_responses:
            apigw_client.put_method(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod=resp_details["httpMethod"],
                authorizationType="NONE",
                apiKeyRequired=is_api_key_required,
            )

            apigw_client.put_method_response(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod=resp_details["httpMethod"],
                statusCode="200",
            )

            apigw_client.put_integration(
                restApiId=api_id,
                resourceId=root_id,
                httpMethod=resp_details["httpMethod"],
                integrationHttpMethod=resp_details["httpMethod"],
                type=integration_type,
                requestTemplates=request_templates,
                **kwargs,
            )

            apigw_client.put_integration_response(
                restApiId=api_id,
                resourceId=root_id,
                selectionPattern="",
                responseTemplates=response_templates,
                **resp_details,
            )

        apigw_client.create_deployment(restApiId=api_id, stageName=stage_name)
        return api_id

    @markers.parity.aws_validated
    @pytest.mark.parametrize("stage_name", ["local", "dev"])
    def test_apigw_stage_variables(
        self, create_lambda_function, create_rest_apigw, stage_name, aws_client
    ):
        aws_account_id = aws_client.sts.get_caller_identity()["Account"]
        region_name = aws_client.apigateway._client_config.region_name
        api_id, _, root = create_rest_apigw(name="aws lambda api")
        resource_id, _ = create_rest_resource(
            aws_client.apigateway, restApiId=api_id, parentId=root, pathPart="test"
        )
        create_rest_resource_method(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            authorizationType="NONE",
        )

        fn_name = f"test-{short_uid()}"
        create_lambda_function(
            func_name=fn_name,
            handler_file=TEST_LAMBDA_PYTHON_ECHO,
            runtime=LAMBDA_RUNTIME_PYTHON39,
        )
        lambda_arn = aws_client.awslambda.get_function(FunctionName=fn_name)["Configuration"][
            "FunctionArn"
        ]

        if stage_name == "dev":
            uri = f"arn:aws:apigateway:{region_name}:lambda:path/2015-03-31/functions/arn:aws:lambda:{region_name}:{aws_account_id}:function:${{stageVariables.lambdaFunction}}/invocations"
        else:
            uri = f"arn:aws:apigateway:{region_name}:lambda:path/2015-03-31/functions/arn:aws:lambda:{region_name}:{aws_account_id}:function:{fn_name}/invocations"

        create_rest_api_integration(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            integrationHttpMethod="POST",
            type="AWS",
            uri=uri,
            requestTemplates={"application/json": '{ "version": "$stageVariables.version" }'},
        )
        create_rest_api_method_response(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            statusCode="200",
            responseParameters={
                "method.response.header.Content-Type": False,
                "method.response.header.Access-Control-Allow-Origin": False,
            },
        )
        create_rest_api_integration_response(
            aws_client.apigateway,
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="POST",
            statusCode="200",
        )
        deployment_id, _ = create_rest_api_deployment(aws_client.apigateway, restApiId=api_id)

        stage_variables = (
            {"lambdaFunction": fn_name, "version": "1.0"} if stage_name == "dev" else {}
        )
        create_rest_api_stage(
            aws_client.apigateway,
            restApiId=api_id,
            stageName=stage_name,
            deploymentId=deployment_id,
            variables=stage_variables,
        )

        source_arn = f"arn:aws:execute-api:{region_name}:{aws_account_id}:{api_id}/*/*/test"
        aws_client.awslambda.add_permission(
            FunctionName=lambda_arn,
            StatementId=str(short_uid()),
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=source_arn,
        )

        url = api_invoke_url(api_id, stage=stage_name, path="/test")
        response = requests.post(url, json={"test": "test"})

        if stage_name == "local":
            assert response.json() == {"version": ""}
        else:
            assert response.json() == {"version": "1.0"}


@pytest.mark.skipif(not use_docker(), reason="Rust lambdas cannot be executed in local executor")
@pytest.mark.skipif(get_arch() == "arm64", reason="Lambda only available for amd64")
def test_apigateway_rust_lambda(
    create_rest_apigw, create_lambda_function, create_iam_role_with_policy, aws_client
):
    function_name = f"test-rust-function-{short_uid()}"
    api_gateway_name = f"api_gateway_{short_uid()}"
    role_name = f"test_apigateway_role_{short_uid()}"
    policy_name = f"test_apigateway_policy_{short_uid()}"
    stage_name = "test"
    first_name = f"test_name_{short_uid()}"
    lambda_create_response = create_lambda_function(
        func_name=function_name,
        zip_file=load_file(TEST_LAMBDA_HTTP_RUST, mode="rb"),
        handler="bootstrap.is.the.handler",
        runtime="provided.al2",
    )
    role_arn = create_iam_role_with_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        RoleDefinition=APIGATEWAY_ASSUME_ROLE_POLICY,
        PolicyDefinition=APIGATEWAY_LAMBDA_POLICY,
    )
    lambda_arn = lambda_create_response["CreateFunctionResponse"]["FunctionArn"]
    rest_api_id, _, _ = create_rest_apigw(name=api_gateway_name)

    root_resource_id = aws_client.apigateway.get_resources(restApiId=rest_api_id)["items"][0]["id"]
    aws_client.apigateway.put_method(
        restApiId=rest_api_id,
        resourceId=root_resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    aws_client.apigateway.put_method_response(
        restApiId=rest_api_id, resourceId=root_resource_id, httpMethod="GET", statusCode="200"
    )
    lambda_target_uri = arns.apigateway_invocations_arn(
        lambda_uri=lambda_arn, region_name=aws_client.apigateway.meta.region_name
    )
    aws_client.apigateway.put_integration(
        restApiId=rest_api_id,
        resourceId=root_resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=lambda_target_uri,
        credentials=role_arn,
    )
    aws_client.apigateway.create_deployment(restApiId=rest_api_id, stageName=stage_name)
    url = path_based_url(
        api_id=rest_api_id, stage_name=stage_name, path=f"/?first_name={first_name}"
    )
    result = requests.get(url)
    assert result.text == f"Hello, {first_name}!"


def test_apigw_call_api_with_aws_endpoint_url(aws_client):
    headers = aws_stack.mock_aws_request_headers("apigateway")
    headers["Host"] = "apigateway.us-east-2.amazonaws.com:4566"
    url = f"{get_edge_url()}/apikeys?includeValues=true&name=test%40example.org"
    response = requests.get(url, headers=headers)
    assert response.ok
    content = json.loads(to_str(response.content))
    assert isinstance(content.get("item"), list)


@pytest.mark.parametrize("method", ["GET", "ANY"])
@pytest.mark.parametrize("url_function", [path_based_url, host_based_url])
def test_rest_api_multi_region(
    method, url_function, create_rest_apigw, aws_client, aws_client_factory
):
    apigateway_client_eu = aws_client_factory(region_name="eu-west-1").apigateway
    apigateway_client_us = aws_client_factory(region_name="us-west-1").apigateway

    api_eu_id, _, root_resource_eu_id = create_rest_apigw(
        name="test-eu-region", region_name="eu-west-1"
    )
    api_us_id, _, root_resource_us_id = create_rest_apigw(
        name="test-us-region", region_name="us-west-1"
    )

    resource_eu_id, _ = create_rest_resource(
        apigateway_client_eu, restApiId=api_eu_id, parentId=root_resource_eu_id, pathPart="demo"
    )
    resource_us_id, _ = create_rest_resource(
        apigateway_client_us, restApiId=api_us_id, parentId=root_resource_us_id, pathPart="demo"
    )

    create_rest_resource_method(
        apigateway_client_eu,
        restApiId=api_eu_id,
        resourceId=resource_eu_id,
        httpMethod=method,
        authorizationType="None",
    )
    create_rest_resource_method(
        apigateway_client_us,
        restApiId=api_us_id,
        resourceId=resource_us_id,
        httpMethod=method,
        authorizationType="None",
    )

    lambda_name = f"lambda-{short_uid()}"
    testutil.create_lambda_function(
        handler_file=TEST_LAMBDA_NODEJS,
        func_name=lambda_name,
        runtime=Runtime.nodejs16_x,
        region_name="eu-west-1",
    )
    testutil.create_lambda_function(
        handler_file=TEST_LAMBDA_NODEJS,
        func_name=lambda_name,
        runtime=Runtime.nodejs16_x,
        region_name="us-west-1",
    )
    lambda_eu_west_1_client = aws_client_factory(region_name="eu-west-1").awslambda
    lambda_us_west_1_client = aws_client_factory(region_name="us-west-1").awslambda
    lambda_eu_west_1_client.get_waiter("function_active_v2").wait(FunctionName=lambda_name)
    lambda_us_west_1_client.get_waiter("function_active_v2").wait(FunctionName=lambda_name)
    lambda_eu_arn = arns.lambda_function_arn(lambda_name, region_name="eu-west-1")
    uri_eu = arns.apigateway_invocations_arn(lambda_eu_arn, region_name="eu-west-1")

    integration_uri, _ = create_rest_api_integration(
        apigateway_client_eu,
        restApiId=api_eu_id,
        resourceId=resource_eu_id,
        httpMethod=method,
        type="AWS_PROXY",
        integrationHttpMethod=method,
        uri=uri_eu,
    )

    lambda_us_arn = arns.lambda_function_arn(lambda_name, region_name="us-west-1")
    uri_us = arns.apigateway_invocations_arn(lambda_us_arn, region_name="us-west-1")

    integration_uri, _ = create_rest_api_integration(
        apigateway_client_us,
        restApiId=api_us_id,
        resourceId=resource_us_id,
        httpMethod=method,
        type="AWS_PROXY",
        integrationHttpMethod=method,
        uri=uri_us,
    )

    # test valid authorization using bearer token
    endpoint = url_function(api_eu_id, stage_name="local", path="/demo")
    result = requests.get(endpoint, headers={}, verify=False)
    assert result.status_code == 200
    endpoint = url_function(api_us_id, stage_name="local", path="/demo")
    result = requests.get(endpoint, headers={}, verify=False)
    assert result.status_code == 200

    delete_rest_api(apigateway_client_eu, restApiId=api_eu_id)
    delete_rest_api(apigateway_client_us, restApiId=api_us_id)
    testutil.delete_lambda_function(name=lambda_name, region_name="eu-west-1")
    testutil.delete_lambda_function(name=lambda_name, region_name="us-west-1")


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("url_function", [path_based_url, host_based_url])
@pytest.mark.parametrize("passthrough_behaviour", ["WHEN_NO_MATCH", "NEVER", "WHEN_NO_TEMPLATES"])
def test_mock_integration_response(
    method, url_function, passthrough_behaviour, create_rest_apigw, aws_client
):
    api_id, _, root_resource_id = create_rest_apigw(name="mock-api")
    resource_id, _ = create_rest_resource(
        aws_client.apigateway, restApiId=api_id, parentId=root_resource_id, pathPart="{id}"
    )
    create_rest_resource_method(
        aws_client.apigateway,
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        authorizationType="NONE",
    )
    integration_uri, _ = create_rest_api_integration(
        aws_client.apigateway,
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        type="MOCK",
        integrationHttpMethod=method,
        passthroughBehavior=passthrough_behaviour,
        requestTemplates={"application/json": '{"statusCode":200}'},
    )
    status_code = create_rest_api_method_response(
        aws_client.apigateway,
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        statusCode="200",
        responseModels={"application/json": "Empty"},
    )
    create_rest_api_integration_response(
        aws_client.apigateway,
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        statusCode=status_code,
        responseTemplates={
            "application/json": '{"statusCode": 200, "id": $input.params().path.id}'
        },
    )

    endpoint = url_function(api_id, stage_name="local", path="/42")
    result = requests.request(
        method,
        endpoint,
        headers={"Content-Type": "application/json"},
        verify=False,
    )
    assert result.status_code == 200
    assert to_str(result.content) == '{"statusCode": 200, "id": 42}'


def test_tag_api(create_rest_apigw, aws_client):
    api_name = f"api-{short_uid()}"
    tags = {"foo": "bar"}

    # add resource tags
    api_id, _, _ = create_rest_apigw(name=api_name, tags={TAG_KEY_CUSTOM_ID: "c0stIOm1d"})
    assert api_id == "c0stIOm1d"

    api_arn = arns.apigateway_restapi_arn(api_id=api_id)
    aws_client.apigateway.tag_resource(resourceArn=api_arn, tags=tags)

    # receive and assert tags
    tags_saved = aws_client.apigateway.get_tags(resourceArn=api_arn)["tags"]
    assert tags == tags_saved

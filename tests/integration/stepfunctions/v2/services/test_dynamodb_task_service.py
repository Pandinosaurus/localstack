import json

import pytest

from localstack.testing.pytest import markers
from localstack.utils.strings import short_uid
from tests.integration.stepfunctions.templates.services.services_templates import (
    ServicesTemplates as ST,
)
from tests.integration.stepfunctions.utils import create_and_record_execution, is_old_provider

pytestmark = pytest.mark.skipif(
    condition=is_old_provider(), reason="Test suite for v2 provider only."
)


@markers.snapshot.skip_snapshot_verify(
    paths=[
        "$..loggingConfiguration",
        "$..tracingConfiguration",
        "$..previousEventId",
        # # TODO: add support for Sdk Http metadata.
        "$..SdkHttpMetadata",
        "$..SdkResponseMetadata",
    ]
)
class TestTaskServiceDynamoDB:
    def test_put_get_item(
        self,
        aws_client,
        create_iam_role_for_sfn,
        create_state_machine,
        dynamodb_create_table,
        sfn_snapshot,
    ):
        sfn_snapshot.add_transformer(sfn_snapshot.transform.dynamodb_api())

        table_name = f"sfn_test_table_{short_uid()}"
        dynamodb_create_table(table_name=table_name, partition_key="id", client=aws_client.dynamodb)

        template = ST.load_sfn_template(ST.DYNAMODB_PUT_GET_ITEM)
        definition = json.dumps(template)

        exec_input = json.dumps(
            {
                "TableName": table_name,
                "Item": {"data": {"S": "HelloWorld"}, "id": {"S": "id1"}},
                "Key": {"id": {"S": "id1"}},
            }
        )
        create_and_record_execution(
            aws_client.stepfunctions,
            create_iam_role_for_sfn,
            create_state_machine,
            sfn_snapshot,
            definition,
            exec_input,
        )

    def test_put_delete_item(
        self,
        aws_client,
        create_iam_role_for_sfn,
        create_state_machine,
        dynamodb_create_table,
        sfn_snapshot,
    ):
        sfn_snapshot.add_transformer(sfn_snapshot.transform.dynamodb_api())

        table_name = f"sfn_test_table_{short_uid()}"
        dynamodb_create_table(table_name=table_name, partition_key="id", client=aws_client.dynamodb)

        template = ST.load_sfn_template(ST.DYNAMODB_PUT_DELETE_ITEM)
        definition = json.dumps(template)

        exec_input = json.dumps(
            {
                "TableName": table_name,
                "Item": {"data": {"S": "HelloWorld"}, "id": {"S": "id1"}},
                "Key": {"id": {"S": "id1"}},
            }
        )
        create_and_record_execution(
            aws_client.stepfunctions,
            create_iam_role_for_sfn,
            create_state_machine,
            sfn_snapshot,
            definition,
            exec_input,
        )

    def test_put_update_get_item(
        self,
        aws_client,
        create_iam_role_for_sfn,
        create_state_machine,
        dynamodb_create_table,
        sfn_snapshot,
    ):
        sfn_snapshot.add_transformer(sfn_snapshot.transform.dynamodb_api())

        table_name = f"sfn_test_table_{short_uid()}"
        dynamodb_create_table(table_name=table_name, partition_key="id", client=aws_client.dynamodb)

        template = ST.load_sfn_template(ST.DYNAMODB_PUT_UPDATE_GET_ITEM)
        definition = json.dumps(template)

        exec_input = json.dumps(
            {
                "TableName": table_name,
                "Item": {"data": {"S": "HelloWorld"}, "id": {"S": "id1"}},
                "Key": {"id": {"S": "id1"}},
                "UpdateExpression": "set S=:r",
                "ExpressionAttributeValues": {":r": {"S": "HelloWorldUpdated"}},
            }
        )
        create_and_record_execution(
            aws_client.stepfunctions,
            create_iam_role_for_sfn,
            create_state_machine,
            sfn_snapshot,
            definition,
            exec_input,
        )

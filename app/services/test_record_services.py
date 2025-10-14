from app.utils.pressure_test import single_test_chatflow_non_stream_pressure,validate_entry
from app.models.test_chatflow_record import TestRecord

def test_chatflow_non_stream_pressure_wrapper(testrecord:TestRecord):

    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_query = testrecord.query
    input_dify_username = testrecord.username
    input_data_dict
    return
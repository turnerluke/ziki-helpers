from ziki_helpers.toast_api.connector import is_iso_datetime, is_camel_case, is_snake_case, snake_case_to_camel_case




def test_string_helpers():

    assert is_iso_datetime('2021-06-01T00:00:00-04:00')
    assert not is_iso_datetime('2021-13-01T00:00:00-04:00:00')

    assert is_camel_case('testString')
    assert not is_camel_case('TestString')
    assert not is_camel_case('test_string')

    assert is_snake_case('test_string')
    assert not is_snake_case('TestString')
    assert not is_snake_case('testString')

    assert snake_case_to_camel_case('test_string') == 'testString'
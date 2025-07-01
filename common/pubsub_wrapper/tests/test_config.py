import json
from unittest.mock import MagicMock, patch

from pubsub_wrapper.config import load_config


def test_load_config_parses_values():
    responses = {
        '/stockapp/devtest/PGHOST': 'localhost',
        '/stockapp/devtest/PGUSER': 'user',
        '/stockapp/devtest/PGPASSWORD': 'pass',
        '/stockapp/devtest/PGDATABASE': 'db',
        '/stockapp/devtest/PGPORT': '5432',
        '/stockapp/devtest/symbols': json.dumps([['AAPL', '1d']]),
        '/stockapp/devtest/TA': json.dumps(['macd']),
        '/stockapp/devtest/STRATEGIES': json.dumps(['macd_rsi']),
        '/stockapp/devtest/container_registry': 'reg',
        '/stockapp/devtest/redis_url': 'redis://localhost:6379',
    }

    def get_parameter(Name, WithDecryption=True):
        return {'Parameter': {'Value': responses[Name]}}

    mock_ssm = MagicMock()
    mock_ssm.get_parameter.side_effect = get_parameter

    with patch('boto3.client', return_value=mock_ssm):
        cfg = load_config('devtest', '/stockapp')

    assert cfg['PGHOST'] == 'localhost'
    assert cfg['PGUSER'] == 'user'
    assert cfg['PGPASSWORD'] == 'pass'
    assert cfg['PGDATABASE'] == 'db'
    assert cfg['PGPORT'] == 5432
    assert cfg['symbols'] == [['AAPL', '1d']]
    assert cfg['TA'] == ['macd']
    assert cfg['STRATEGIES'] == ['macd_rsi']
    assert cfg['container_registry'] == 'reg'
    assert cfg['redis_url'] == 'redis://localhost:6379'

from arcana2.core.entrypoint import MainCmd


def test_commands():
    assert sorted(MainCmd.commands) == ['help', 'run', 'run-bids', 'wrap4xnat']


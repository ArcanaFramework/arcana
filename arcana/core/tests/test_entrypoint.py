from arcana.core.entrypoint import MainCmd


def test_commands():
    assert sorted(MainCmd.commands) == ['help', 'run', 'wrap4xnat']


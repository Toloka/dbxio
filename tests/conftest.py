import pytest
from tenacity import Retrying


@pytest.fixture
def default_retrying():
    return Retrying()

import pytest

@pytest.fixture
def sampleData():
    return [1,2,3]

def test_sum(sampleData):
    assert sum(sampleData)==6



# for parametrized

@pytest.fixture
def base():
    return 2


@pytest.mark.parametrize("power,expected",[(1,2),(3,8),(4,16),(5,26)])
def test_expo(base,power,expected):
    assert base**power==expected
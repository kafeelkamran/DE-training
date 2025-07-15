import pytest

@pytest.fixture
def sampleData():
    return [1,2,3]

def test_sum(sampleData):
    assert sum(sampleData)==6

#for parametrized
@pytest.fixture
def base():
    return 2

@pytest.mark.parametrize("power,expected",[(2,4),(3,8),(4,16),(5,32)])
def test_exponentiation(base,power,expected):
    assert base**power==expected

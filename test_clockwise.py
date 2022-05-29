from clockwise.clockwise import clockwiseMatrix

def test_clockwise():
    result = clockwiseMatrix([[1,2],[3,4]])
    assert "1 2 4 3" == result

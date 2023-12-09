from clockwise.clockwise import clockwiseMatrix

def test_clockwise():
    result = clockwiseMatrix([[1,2],[3,4]])
    assert "1 2 4 3" == result
    
    result = clockwiseMatrix([[2, 3, 4, 8],
                             [5, 7, 9, 12],
                             [1, 0, 6, 10]])
    assert "2 3 4 8 12 10 6 0 1 5 7 9" == result

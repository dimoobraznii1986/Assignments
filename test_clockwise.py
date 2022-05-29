from clockwise.clockwise import clockwiseMatrix

TestMatrix1 = [[1,2],
              [3,4]]
 
def test_clockwise(TestMatrix1):
    result = clockwiseMatrix(TestMatrix1)
    assert "1 2 3 4" == result

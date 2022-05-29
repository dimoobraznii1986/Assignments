from clockwise.clockwise import clockwiseMatrix

TestMatricx = [[1,2],
               [3,4]]
 
def test_clockwise(TestMatrix):
  result = clockwiseMatrix(TestMatrix)
  assert "1 2 3 4" == result

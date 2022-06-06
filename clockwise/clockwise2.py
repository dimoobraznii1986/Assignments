def clockWise2(Matrix):
  # Define size of matrix 
  rows = len(matrix)
  columns = len(matrix[0])        
  n = rows*columns

  #starting point
  x = 0 
  y = -1 
  i=1

  #direction, where -1 - backwards, 0 - same, 1 forward
  d_row = 0 
  d_column = 1 

  output = []

  #Adding matrix for checking
  seen = [[0 for i in range(columns)] for j in range(rows)]


  while i <= n:
    if 0 <= x + d_row < rows and 0 <= y + d_column < columns and seen[x + d_row][y + d_column] == 0:
      print("cycle")
      x = x + d_row
      y = y + d_column
      
      #Assign the True for element that was checked
      seen[x][y] = True
      
      #Write result to the new list
      output.append(str(matrix[x][y]))
      
      i += 1
    else:
      print(d_column, d_row)
      if d_column == 1:
        d_row = 1
        d_column = 0
        print("take1")
      elif d_row == 1:
        d_row = 0
        d_column = -1
        print("take2")
      elif d_column == -1:
        d_row = -1
        d_column = 0
        print("take3")
      elif d_row == -1:
        d_row = 0
        d_column = 1
        print("take4")
  
  return ' '.join(output)
  

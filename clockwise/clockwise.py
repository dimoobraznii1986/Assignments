def clockwiseMatrix(matrix):    
    #define size of matrix
    h=len(matrix)
    w=len(matrix[0])

    print("Number rows: " + str(w))
    print("Number columns: " + str(h))

    #starting coordinates
    x = 0 #rows
    y = 0 #columns
    
    result = []

    while x < w and y < w:
        #Print the 1st row
        for i in range(y,w):
            result.append(str(matrix[x][i]))

        x += 1

        #Print last column elements
        for i in range(x,h):
            result.append(str(matrix[i][w - 1]))

        w -= 1

        #Print bottom row backwards
        if x < h:
            for i in range(w - 1, y - 1, -1):
                result.append(str(matrix[h-1][i]))

            h -= 1

        #Print first column without printed element
        if y < w:
            for i in range(h - 1, x - 1, -1):
                result.append(str(matrix[i][y]))

            y += 1
            
    return ' '.join(result)

def clockwiseMatrix(matrix):    
    #define size of matrix
    h=len(matrix)
    w=len(matrix[0])

    print("Number rows: " + str(w))
    print("Number columns: " + str(h))

    #starting coordinates
    x = 0 #rows
    y = 0 #columns

    while x < w and y < w:
        #Print the 1st row
        for i in range(y,w):
            print(matrix[x][i], end=" ")

        x += 1

        #Print last column elements
        for i in range(x,h):
            print(matrix[i][w - 1], end=" ")

        w -= 1

        #Print bottom row backwards
        if x < h:
            for i in range(w - 1, y - 1, -1):
                print(matrix[h-1][i], end =" ")

            h -= 1

        #Print first column without printed element
        if y < w:
            for i in range(h - 1, x - 1, -1):
                print(matrix[i][y], end=" ")

            y += 1

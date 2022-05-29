# Assignment

## Part 1 Software Version Analytics

Please review this detail documnet [Data Platform Considerations](https://github.com/dimoobraznii1986/e-assignment/blob/main/data-platform-proposal.md)

## Part 2 Clockwise Matrics

### Repo Structure
[![Test Multiple Python Versions](https://github.com/dimoobraznii1986/e-assignment/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/dimoobraznii1986/e-assignment/actions/workflows/main.yml)

The solution has:
- function `clockwiseMatrix` 
- CI pipeline
	- linting
	- testing
	- formating

### Task
*Write a function that, given a matrix of integers, builds a string with the entries of that matrix
appended in clockwise order.*

*Feel free to use any language you would like, but here is an example function signature `def clockwiseMatrix(input):`*

```python
input = [[2, 3, 4, 8],
	[5, 7, 9, 12],
	[1, 0, 6, 10]]

clockwiseMatrix(input)
>>2, 3, 4, 8, 12, 10, 6, 0, 1, 5, 7, 9

input = [[1, 2],
	[3, 4]]

clockwiseMatrix(input)
>>1,2,4,3
```

*How can you verify that your solution is correct?*

### Solution

Let’s look the first example:

```python
[[2, 3, 4, 8],
[5, 7, 9, 12],
[1, 0, 6, 10]]
```

I’ve assigned coordinates for each element, my goal is to go through the elements clockwise and print the elements in the order:

![Untitled](img/Untitled%208.png)

The matrix has the following attributes:

- **w** - width of matrix (number of columns)
- **h** - height of matrix (number of rows)
- **x,y** - coordinates of each element, such as 0,0;0,1;0,2 and so on.

The function code is [here](https://github.com/dimoobraznii1986/e-assignment/blob/main/clockwise/clockwise.py)


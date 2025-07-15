square = [x**2 for x in range(10)]
even = [x for x in range(10) if x%2==0]
print(square)
print(even)

print("=========IN MAP=========")
squareMap = list(map(lambda x:x**2, range(10)))
evenMap = list(filter(lambda x:x%2==0, range(10)))
print(squareMap)
print(evenMap)
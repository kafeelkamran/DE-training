a = [1,2]
b = a
c = list(a)
print(id(a),id(b),id(c))
print(a is b)
print(a == c)
print(c == b)
print(a is c)

print(a);
print(b);
print(c);


x = 10
def modify(val):
    global x
    x += 5
    val += 5
    return val

print(modify(x))
print(x)
# Python CLI
print('Hello World')
i = 0
type(i)
help(int)
s = 'Hello World'
s = "Hello World"
type(s)
help(str)
help()
str

# Predefined functions
i = 1
j = 2
i + j
o = 
o = '1,2013-07-25 00:00:00.0,1185,CLOSED'
type(o)
len(o)
o[0]
o[1]
o[2:23]
help(str)
help(o.split)
o.split(",")
type(o.split(","))
type(o.split(",")[1])
o.split(",")[3]
o.split(",")[1][:4]
type(o.split(",")[1][:4])
2 > 10
'2' > '10'
int(o.split(",")[1][:4])
int(o.split(",")[1][:4]) if(o.split(",")[1][:4].isdigit()) else "Invalid Number"

#Tuple
t = (1, "Hello")
type(t)
t[0]
t[1]
o = '1,2013-07-25 00:00:00.0,1185,CLOSED'
(int(o.split(',')[0]), o.split(',')[1])
len(t)

def sumOfIntegers(lb, ub):
	total = 0
	for i in range(lb, ub + 1):
		total += i
	return total


print(str(sumOfIntegers(5, 10)))

def sumLambda(lb, ub, f):
    total = 0
    for i in range(lb, ub+1):
        total += f(i)
    return total

print(str(sumLambda(5, 10, lambda i: i)))
print(str(sumLambda(5, 10, lambda i: i * i)))
print(str(sumLambda(5, 10, lambda i: i if(i%2==0) else 0)))

# Collections and Map Reduce APIs 
l = [1,2,3,4]
type(l)
d = {1:"Hello",2:"World"}
s = {1,1,1,2,2,3,4,5}
type(s)
d[1]
help(s)
help(d)
help(l)


# Video Progress 1:10:00 minutes




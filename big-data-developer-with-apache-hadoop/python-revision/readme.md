# Quick revision of programming language – Python 3.x (including Dataframes)
````text
	- As part of this session we will see quick revision of Python as programming languages with emphasis on collections and map reduce APIs.

    	- Python CLI and IDE
    	- Variables
    	- Pre-defined functions
        	- Operators
        	- String Manipulation
        	- Type Conversion
    	- User-defined functions
    	- Collections and Map Reduce APIs
    	- Usage of Tuples
    	- Development life cycle using Python
````

# Python CLI and IDE
````text
	- As any other programming language, Python also have multiple graphical interfaces to explore, validate and implement the code. 
	- Python also have web based development tools such as Jupyter Notebook.
    		- Python is to validate simple code snippets before adding to the modules associated with a project.
    		- We can also use web based interfaces such as jupyter notebook to run and validate Python code snippets.
    		- IDE (Integrated Development Environment) – we should be comfortable with IDE and should use IDEs to build applications using any programming language. 
		- Following are some IDEs which are popular with respect to Python.
        	- Eclipse with Python plugin
        	- PyCharm (we will be using this for demo)
    		- Once we develop the projects or modules using IDE, we have to build zip file to deploy as an application.
````

# Variables
````text
	- Let us understand some important concepts around Python Variables.
    	- Every variable is object
    	- We cannot specify data type, it will be inherited based up on the value used for initialization of the corresponding variable.
    	- You can assign different values of different types to the same variable and the variable inherit the data type (dynamically typed)
    	- We can get the type of variable by using type function
    	- We can get help on a class or function by passing it to help
````

# Pre-defined Functions
````text
	- It is very important for us to understand the broad spectrum of pre-defined functions to leverage them as part of data processing.

	- Operator related functions
    		- Python support all types of Arithmetic operations using operators such as + for addition, – for subtraction etc.

	- String Manipulation functions
    		- Extracting information from fixed length fields – substring
    		- Extracting information from variable length fields – split and then use index as part of []
    		- Case conversion functions
    		- Trimming characters
    		- Padding characters
    		- and more

	- Type conversion functions
	    	- As part of processing the data if we extract information from a string, the output will be returned as string even if original type is of non string type (such as int, float etc)
    		- It is important to convert type of our data to its original data type
    		- It can be achieved by invoking constructors such as int, float etc

	- Collections related functions
    		- Python gives us rich APIs to manipulate collections.
    		- Most of these APIs take anonymous functions as arguments
    		- There are functions that are part of collection classes and also generic functions that work on all collections.
````

````python
o = '1,2013-07-25 00:00:00.0,1185,CLOSED'

len(o) #length of the string
o.split(',')[3] #extract status
int(o.split(',')[1][:4]) #get year as number using split, substring and int
(int(o.split(',')[0]), o.split(',')[1]) #convert o to tuple with order id as int and date
````

# User-defined Functions
````text
	- Now let us see how we can develop user defined functions.
    		- Function starts with key word def
    		- We need to define arguments, data type is not required
    		- Return type is also cannot be specified
    		- We can use varrying number of arguments, key word arguments while defining functions
    		- Python also supports anonymous or lambda functions
````

````python
def sumOfIntegers(lb, ub):
    total = 0
    for i in range(lb, ub+1):
        total += i
    return total

print(str(sumOfIntegers(5, 10)))
````

````python
def sumLambda(lb, ub, f):
    total = 0
    for i in range(lb, ub+1):
        total += f(i)
    return total

print(str(sumLambda(5, 10, lambda i: i)))
print(str(sumLambda(5, 10, lambda i: i * i)))
print(str(sumLambda(5, 10, lambda i: i if(i%2==0) else 0)))
````

# Collections and Map Reduce APIs
````text
	- Python Collections have support for many type of Collection objects and have APIs to process different types of Collections. 
	- Collections contain group of homogeneous elements.
    		- At a higher level it support list, set and dict
    		- list is group of elements with length and index. Index starts with 0.
    		- set is group of unique elements
    		- dict is group of key value pairs. Keys are unique.
    		- There are several APIs and packages such as itertools, collections, pandas etc to manipulate collections in Python
````

# Usage of Tuples
````text
	- While collection is like a table with homogeneous elements, tuple is like a row with columns of different type.
    		- Tuple contains group of heterogeneous elements
    		- It is nothing but object with attributes with no names
    		- As there are no names, we can access elements in tuple by position
    		- We use [0] to access first element, [1] to access second element.
    		- Tuples are represented in ()
````

# Development life cycle
````text
	- Let us define problem statement and understand how we can develop the logic using Python as Programming Language. 
	- This will be quick revision about all important Python programming constructs.

    	- Data Set: order_items
    	- Table Structure:
        	- order_item_id (primary key)
        	- order_item_order_id (foreign key to orders.order_id)
        	- order_item_product_id (foreign key to products.product_id)
        	- order_item_quantity
        	- order_item_subtotal
        	- order_item_product_price
    	- Task 1: Get revenue for given order_item_order_id
        	- Define function getOrderRevenue with 2 arguments order_items and order_id
        	- Use map reduce APIs to filter order items for given order id, to extract order_item_subtotal and add it to get revenue
        	- Return order revenue
    	- Task 2: Get revenue for each order_item_order_id
        	- Define function getRevenuePerOrder with 1 argument order_items
        	- Use map reduce APIs to get order_item_order_id and order_item_subtotal, then group by order_item_order_id and then process the values for each order_item_order_id
        	- Return a collection which contain order_item_order_id and revenue_per_order_id
````

````python
def readData(dataPath):
  dataFile = open(dataPath)
  dataStr = dataFile.read()
  dataList = dataStr.splitlines()
  return dataList

def getRevenueForOrderId(orderItems, orderId):
    orderItemsFiltered = filter(lambda oi: int(oi.split(",")[1]) == 2, orderItems)
    orderItemSubtotals = map(lambda oi: float(oi.split(",")[4]), orderItemsFiltered)
    
    import functools as ft
    orderRevenue = ft.reduce(lambda x, y: x + y, orderItemSubtotals)
    return orderRevenue
    
orderItemsPath = '/data/retail_db/order_items/part-00000'
orderItems = readData(orderItemsPath)

orderRevenue = getRevenueForOrderId(orderItems, 2)
print(str(orderRevenue))
````

````python
def readData(dataPath):
  dataFile = open(dataPath)
  dataStr = dataFile.read()
  dataList = dataStr.splitlines()
  return dataList

def getRevenuePerOrder(orderItems):
    #Using itertools
    import itertools as it
    # help(it.groupby)
    orderItems.sort(key=lambda oi: int(oi.split(",")[1]))
    orderItemsGroupByOrderId = it.groupby(orderItems, lambda oi: int(oi.split(",")[1]))
    revenuePerOrder = map(lambda orderItems: 
                          (orderItems[0], sum(map(lambda oi: 
                                                  float(oi.split(",")[4]), orderItems[1]
                                                 )
                                             )
                          ), 
                          orderItemsGroupByOrderId)
    return revenuePerOrder

orderItemsPath = '/data/retail_db/order_items/part-00000'
orderItems = readData(orderItemsPath)
revenuePerOrder = getRevenuePerOrder(orderItems)

for i in list(revenuePerOrder)[:10]:
    print(i)
````

````python
import sys
import pandas as pd

def readData(dataPath):
  dataDF = pd.read_csv(dataPath, header=None,
                             names=['order_item_id', 'order_item_order_id',
                                    'order_item_product_id', 'order_item_quantity',
                                    'order_item_subtotal', 'order_item_product_price'])
  return dataDF

def getRevenuePerOrder(orderItems):
    revenuePerOrder = orderItems.groupby(by=['order_item_order_id'])['order_item_subtotal'].sum()

    return revenuePerOrder

orderItemsPath = sys.argv[1]
orderItems = readData(orderItemsPath)
revenuePerOrder = getRevenuePerOrder(orderItems)

import sys
import pandas as pd

def readData(dataPath):
  dataDF = pd.read_csv(dataPath, header=None,
                             names=['order_item_id', 'order_item_order_id',
                                    'order_item_product_id', 'order_item_quantity',
                                    'order_item_subtotal', 'order_item_product_price'])
  return dataDF

def getRevenuePerOrder(orderItems):
    revenuePerOrder = orderItems.groupby(by=['order_item_order_id'])['order_item_subtotal'].sum()

    return revenuePerOrder

orderItemsPath = sys.argv[1]
orderItems = readData(orderItemsPath)
revenuePerOrder = getRevenuePerOrder(orderItems)

print(revenuePerOrder.iloc[:10])
````

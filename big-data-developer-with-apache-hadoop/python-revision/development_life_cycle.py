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

#############################################################33

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


##############################################################################

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
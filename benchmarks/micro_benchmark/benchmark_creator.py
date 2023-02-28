
import csv

# define the data
data = [('Alex', 25)]
data1 = [('Maria', 10)]
data2 = [('Lukas', 20)]
# create the CSV file
with open('duplicates1.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    for i in range (15):
        writer.writerows(data  * 10000000)
    for i in range (15):
        writer.writerows(data1  * 10000000)
    for i in range (15):
        writer.writerows(data2  * 10000000)
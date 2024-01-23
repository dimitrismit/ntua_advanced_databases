import csv
import sys

file = sys.argv[1]

reader = csv.reader(open(file, "rU"), delimiter='|')
writer = csv.writer(open(file, 'w'), delimiter=',')
writer.writerows(reader)

print("Delimiter successfully changed")


# Python code to  merge two list of dictionaries 
# based on some value.
  
from collections import defaultdict
  
# List initialization
Input1 = [{'roll_no': ['123445', '1212'], 'school_id': 1},
          {'roll_no': ['HA-4848231'], 'school_id': 2}]
  
Input2 = [{'roll_no': ['473427'], 'school_id': 2},
          {'roll_no': ['092112'], 'school_id': 5}]
  
  
# Using defaultdic
temp = defaultdict(list) 
  
# Using extend
for elem in Input1:
    temp[elem['school_id']].extend(elem['roll_no'])
  
for elem in Input2:
    temp[elem['school_id']].extend(elem['roll_no'])
  
Output = [{"roll_no":y, "school_id":x} for x, y in temp.items()]
  
# printing
print(Output)
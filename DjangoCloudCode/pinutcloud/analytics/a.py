from collections import deque

circular_queue = deque([1,2], maxlen=4)
print "normal",circular_queue
circular_queue.append(3)
print "append 3",circular_queue
circular_queue.extend([4])
print "extend 4",circular_queue

# at this point you have [1,2,3,4]
print(circular_queue.pop())  # [1,2,3] --> 4
print "pop",circular_queue

# key step. effectively rotate the pointer
circular_queue.rotate(-1)  # negative to the left. positive to the right
print "rotate -1",circular_queue

# at this point you have [2,3,1]
print "pop" ,circular_queue.pop()  # [2,3] --> 1


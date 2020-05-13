import math 
import mmh3
from bitarray import bitarray 


class BloomFilter(object):

#array size
# m = bit array size
# n = number of expected keys to be stored
# p = Probability of desired false positive rate

    def array_size (self,n,p):
        m= - (n * math.log(p)) / (math.log(2)**2)
        length = int(m)
        return length  


    def hash_time(self,length,n):
        t = (length/n)* math.log(2)
        time = int(t)
        return time


    def __init__(self,capacity,false_rate):
        self.capacity = capacity
        self.false_rate = false_rate
        size= self.array_size(capacity,false_rate)
        self.size = size
        hash_times = self.hash_time(self.size,capacity)
        self.hash_times = hash_times
        bit_array = bitarray(self.size)
        bit_array.setall(0)
        self.bit_array = bit_array
    

    def add(self,info): 
        array_list=[]
        for i in range(self.hash_times):
            j = mmh3.hash(info,i)%self.size
            array_list.append(j)
            self.bit_array[j] = 1


    def is_member(self,info):
        for i in range(self.hash_times):
            j = mmh3.hash(info,i)%self.size
            if(self.bit_array[j] == 1):
                return True
            else:
                return False    


     
      


         
 


       


        





             

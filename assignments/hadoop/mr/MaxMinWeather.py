from mrjob.job import MRJob

class MaxMinWeather(MRJob):

    def mapper(self,_,line):
        yield('max_temp',float(line.split()[5]))
        yield('min_temp',float(line.split()[6]))
    
    def reducer(self,key,value):
        if key == 'max_temp':
            yield(key,max(value))
        else:
            yield(key,min(value))
        

if __name__ == '__main__':
    MaxMinWeather.run()

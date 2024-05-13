#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import logging
class LogConfiguration:
    
    def __init__(self, Filename, name):
        print(Filename)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s:%(name)s:[%(levelname)s]:%(message)s', '%Y%m%d %H%M%S')

        file_handler = logging.FileHandler(Filename)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)


        #stream_handler = logging.StreamHandler()
     
        #stream_handler.setLevel(logging.DEBUG)
        #stream_handler.setFormatter(formatter)


        self.logger.addHandler(file_handler)

        #self.logger.addHandler(stream_handler)


    def getLog(self):
        return self.logger


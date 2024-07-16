import os
import sys

from app.code.python.el_class import ElClass

obj = ElClass()

if __name__ == '__main__':

    obj.extract_load(batch_size=25000,isIncrement=False)



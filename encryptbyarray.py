# -*- coding: utf-8 -*-

import array
import os
from numpy import random
from hashlib import md5

HEAD = [101, 110, 99, 114, 121, 112, 116, 116, 97, 103]


# 生成盐
def __genkey():
    # 随机选择一个长度作为key的长度
    klen = random.randint(10, 30)
    # 生成一个klen长度列表,其中的每一个值都为1-127的随机数
    key = map(lambda i: random.randint(1, 128), range(klen))
    # 返回key 用作盐(列表)
    return list(key)


# 加盐加密
def __encrypt(data, key):
    # 将json变为数组对象
    b = array.array('B', data.encode('utf-8'))
    # 将数组对象转为列表
    l = b.tolist()
    # 看可以分为多少组
    groups = len(l) // len(tuple(key)) + 1
    # 将盐这个列表扩充尽量接近l的长度
    keys = key * groups
    # 加密对经过zip对l(最终json的列表)和盐进行处理,并将处理结果的每一个元组进行位运算
    el = list(map(lambda i: i[0] ^ i[1], zip(keys, l)))
    # 最终加密
    el = HEAD + [len(key)] + key + el
    # 将加密结果转为数组对象
    eb = array.array('B', el)
    # 将加密结果转为string并返回
    return eb.tostring()


# 传入的text为最终的json,生成盐
def encryptText(text, key=[], head=HEAD):
    # 盐默认为空,如果没有
    if not key:
        # 生成一个盐
        key = __genkey()
    # 加盐加密具体实现
    eb = __encrypt(text, key)
    # 返回加密后的结果(string)
    return eb


def encryptFile(f, newname, key=[], head=HEAD):
    if not key:
        key = __genkey()
    data = open(f, 'rb').read()
    m = md5()
    m.update(data)
    print('md5: ', m.hexdigest())
    es = __encrypt(data, key)
    if newname:
        dirpath = os.path.dirname(f)
        enpath = dirpath + os.sep + newname
    else:
        enpath = f + '.enc'
    ef = open(enpath, 'wb')
    print('encrypt file:', enpath)
    ef.write(es)
    ef.flush()
    ef.close()


if __name__ == '__main__':
    hehe = encryptText("hehe")
    print(hehe)


def print_hehe(hehe):
    print(hehe)

class Test(object):
    def __init__(self):
        print('aa',self)
    def print_word(self,words):
        print("bbbbbb",words)

if __name__ == '__main__':
    print(__name__)
    test=Test()
    test.print_word("hehe")
    print_hehe("qqqqqq")
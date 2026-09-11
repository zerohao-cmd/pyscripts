def func():
    l= []
    def inner(value: int):
        l.append(value)
        return l
    return inner

f = func()

inner_l = list(map(f,  [1, 2, 3]))
# 输出
print(inner_l)
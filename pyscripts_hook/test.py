from sc_hook import Int32, IOType, hook

if __name__ == "__main__":

    @hook(io_types=[IOType.Rest])
    def func_1(a: Int32, b: Int32) -> Int32:
        return a + b

    print(func_1.endpoint_id)
    print(func_1.io_types)
    print(func_1.params_type)
    print(func_1.return_type)
    print(func_1(1, 1))

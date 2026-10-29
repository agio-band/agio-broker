

class Router:
    def __init__(self):
        self.functions = {}

    def add(self, func_name):
        def wrapper(func):
            self.functions[func_name] = func
            return func
        return wrapper

    def execute_func(self, name, request, request_id):
        func = self.functions.get(name)
        if not func:
            raise Exception('Function not found: {name}'.format(name=name))
        return func(request, request_id)

router = Router()

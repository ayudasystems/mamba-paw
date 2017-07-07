def task(name):
    def wrapper(func):
        setattr(func, 'name', name)
        setattr(func, 'paw', True)
        return func
    return wrapper

from typing import Callable, TypeVar

import pytest

from methodflow.pipeline import PipelineMixin, ProcessStep

T = TypeVar("T")


@ProcessStep
def fake_standalone_func(x: T) -> T:
    return x


@pytest.fixture()
def fake_owner_obj():
    class Cls(PipelineMixin):
        X = "X"

        def __init__(self):
            self.x = "x"

        @PipelineMixin.op()
        def a(self):
            return self.x

        @PipelineMixin.op()
        @staticmethod
        def b():
            return 1

        @PipelineMixin.op()
        @classmethod
        def c(cls):
            return cls.X

    return Cls()


def test_standalone_func_exe():
    assert isinstance(fake_standalone_func, ProcessStep)
    assert isinstance(fake_standalone_func.func, Callable)
    assert fake_standalone_func._bound_func is None

    assert fake_standalone_func.func(1) == 1
    assert fake_standalone_func(1) == fake_standalone_func.func(1)


def test_standalone_func_member():
    class Cls:
        def __init__(self):
            self.x = "x"

    Cls.f = fake_standalone_func
    cls = Cls()

    assert isinstance(cls.f, ProcessStep)
    assert cls.f() == cls


def test_cls_decorator(fake_owner_obj):
    assert isinstance(fake_owner_obj.a, ProcessStep)

    assert fake_owner_obj.a() == "x"
    assert fake_owner_obj.b() == 1
    assert fake_owner_obj.c() == "X"

    assert fake_owner_obj.a._func == type(fake_owner_obj).a._func
    assert fake_owner_obj.a._bound_func() == "x"
    with pytest.raises(TypeError):
        type(fake_owner_obj).a()
    assert type(fake_owner_obj).a(fake_owner_obj) == "x"

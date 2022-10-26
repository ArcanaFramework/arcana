import attrs


@attrs.define
class A:

    a = attrs.field()
    b = attrs.field()


a = A(1, 2)


class B(A):

    c = 1


b = B(3, 5)


class C:

    d = 4


c = C()

attrs.asdict(c)

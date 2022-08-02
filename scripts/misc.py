import attr


@attr.s
class A:

    a = attr.ib()
    b = attr.ib()


a = A(1, 2)


class B(A):

    c = 1


b = B(3, 5)


class C:

    d = 4


c = C()

attr.asdict(c)

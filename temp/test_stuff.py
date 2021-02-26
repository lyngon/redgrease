from typing import Any, List, Optional

from redgrease.utils import to_bytes

# import attr


# @attr.s(auto_attribs=True, frozen=True)
class Execution:
    def __init__(self, result: Any, errors: Optional[List] = None):
        self.result = result
        self.errors = errors

    def __bool__(self) -> bool:
        if self.errors:
            return False
        else:
            return bool(self.result)

    def __iter__(self):
        try:
            return iter(self.result)
        except (TypeError, AttributeError):
            return iter([] if self.result is None else [self.result])

    def __len__(self) -> int:
        try:
            return len(self.result)
        except (TypeError, AttributeError):
            return 0 if self.result is None else 1

    def __getitem__(self, *args, **kwargs):
        try:
            return self.result.__getitem__(*args, **kwargs)
        except (TypeError, AttributeError):
            if args and args[0] == 0:
                return self.result
            raise

    def __contains__(self, val) -> bool:
        try:
            return self.result.__contains__(val)
        except (TypeError, AttributeError):
            return val == self.result

    def __bytes__(self):
        return to_bytes(self.result)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Execution):
            return self.result == other.result and self.errors == other.errors

        if self.errors:
            return False

        return self.result == other

    def __repr__(self) -> str:
        if self.errors:
            return f"{self.__class__.__name__}({self.result}, errors={self.errors})"
        else:
            return f"{self.__class__.__name__}({self.result})"

    def __str__(self) -> str:
        return repr(self)


e1 = Execution(True)
e2 = Execution(["a", "o", "b"], ["Oh noes!"])
e3 = Execution(False)
e4 = Execution("Anders Astrom")
e5 = Execution(list(range(20)), ["meh"])
e6 = Execution([False, True])

for ex in [e1, e2, e3, e4, e5, e6]:
    print("====")
    print(ex)
    print(f"it is {bool(ex)}")
    print(f"it has length {len(ex)}")
    print(f"the first element is {ex[0]}")
    for res in ex:
        print(f"{res}")
    try:
        print(f"...have a slice: {ex[1:-1]}")
    except Exception:
        print("not sliceable")
    for i in [True, "o", 3]:
        print(f"{i} is{' ' if i in ex else ' NOT '}inside")
    try:
        print(f"bytes version: {bytes(ex)}")  # type: ignore
    except Exception:
        print("no byte allowed")

    for i in [True, "Anders Astrom", ["a", "o", "b"]]:
        print(f"it is{' ' if i == ex else ' NOT '}equal to {i}")

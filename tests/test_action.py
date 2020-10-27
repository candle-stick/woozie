from hypothesis import strategies as st
from hypothesis import given, assume
from src.action import Action
from collections.abc import Hashable


@given(a=st.text(), b=st.text(), c=st.text())
def test_action_equality(a, b, c):
    assume(a)
    assume(b)
    assume(c)
    assume(b != a)

    action1 = Action(name=a, action_type=c)
    action2 = Action(name=b, action_type=c)
    action3 = Action(name=a, action_type=c)

    assert action1 != action2
    assert action1 == action3
    assert action2 != action3


@given(
    st.text(),
    st.text(),
    st.one_of(st.none(), st.sets(elements=st.text())),
    st.one_of(st.none(), st.dictionaries(keys=st.text(), values=st.text())),
)
def test_action_hashable(name, action_type, dependencies, config):
    """Requirement for Action to be a node in networkx graph."""
    assume(name)
    assume(action_type)
    assert isinstance(Action(name, action_type, dependencies, config), Hashable)

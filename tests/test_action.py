from src.action import Action

def test_action_equality():
    a = Action(name='a')
    b = Action(name='b')
    c = Action(name='a')

    assert a!=b
    assert a==c
    assert b!=c
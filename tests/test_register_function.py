def test_register_function(fresh_db):
    @fresh_db.register_function
    def reverse_string(s):
        return "".join(reversed(list(s)))

    result = fresh_db.execute('select reverse_string("hello")').fetchone()[0]
    assert result == "olleh"


def test_register_function_multiple_arguments(fresh_db):
    @fresh_db.register_function
    def a_times_b_plus_c(a, b, c):
        return a * b + c

    result = fresh_db.execute("select a_times_b_plus_c(2, 3, 4)").fetchone()[0]
    assert result == 10

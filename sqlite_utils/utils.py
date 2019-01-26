def iter_pairs(iterator):
    # Yields (item, next_item, is_last) pairs
    # Last row is (item, None, True)
    first = True
    next_item = None
    while True:
        next_done = False
        if first:
            item, done = next_with_done(iterator)
            if done:
                break
            next_item, next_done = next_with_done(iterator)
            first = False
        else:
            item = next_item
            next_item, next_done = next_with_done(iterator)

        yield item, next_item, next_done

        if next_done:
            break


def next_with_done(iterator):
    try:
        return next(iterator), False
    except StopIteration:
        return None, True

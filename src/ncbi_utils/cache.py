import gzip
import pickle
import hashlib
from pathlib import Path


def hash_from_tuple(tuple_):
    str_tuple = tuple(map(str, tuple_))
    return hashlib.md5(" ".join(str_tuple).encode()).hexdigest()


class MissingCachedResult(RuntimeError):
    pass


def save_cache(value_, cache_path, use_gzip=False):
    if use_gzip:
        fhand = gzip.open(cache_path, "wb")
    else:
        fhand = open(cache_path, "wb")
    pickle.dump(value_, fhand)


def load_cache(cache_path):
    if not cache_path.exists():
        raise MissingCachedResult()
    try:
        return pickle.load(gzip.open(cache_path, "rb"))
    except gzip.BadGzipFile:
        return pickle.load(open(cache_path, "rb"))


def get_result(
    funct,
    cache_path,
    args=None,
    kwargs=None,
    use_gzip=False,
    update_cache=False,
):
    if not update_cache and cache_path.exists():
        return load_cache(cache_path)

    if args is None:
        args = tuple()
    if kwargs is None:
        kwargs = {}

    result = funct(*args, **kwargs)
    save_cache(result, cache_path=cache_path, use_gzip=use_gzip)
    return result


def get_cached_result_from_dir(
    funct,
    cache_dir: Path,
    args: tuple,
    update_cache=False,
    use_gzip=False,
):
    args = tuple(args)
    hash = hash_from_tuple(args)

    cache_dir.mkdir(exist_ok=True)

    extension = "pickle.gz" if use_gzip else "pickle"

    cache_path = cache_dir / f"{funct.__name__}.{hash}.{extension}"
    result = get_result(
        funct,
        cache_path=cache_path,
        args=args,
        update_cache=update_cache,
    )
    return result

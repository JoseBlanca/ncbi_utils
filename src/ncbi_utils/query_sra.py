import json
from pathlib import Path
import pickle
import hashlib

import requests

NCBI_EUTILS_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
NCBI_SEARCH_BASE_URL = NCBI_EUTILS_BASE_URL + "esearch.fcgi?"


def _hash(value):
    try:
        hash(value)
    except TypeError:
        raise ValueError(
            "All arguments should be hasheable, but this one failed: " + str(value)
        )
    pickled = pickle.dumps(value)
    return hashlib.md5(pickled).hexdigest()


def cache_call(funct, cache_dir: Path, args=None, kwargs=None):
    if args is None:
        args = tuple()
    if kwargs is None:
        kwargs = {}

    hashes = [_hash(arg) for arg in args]
    hashes.extend((_hash(kwargs[arg]) for arg in sorted(kwargs.keys())))

    hash_ = _hash(tuple(hashes))
    cache_path = cache_dir / f"{funct.__name__}_{hash_}"
    if cache_path.exists():
        with cache_path.open("rb") as fhand:
            result = pickle.load(fhand)
    else:
        result = funct(*args, **kwargs)
        with cache_path.open("wb") as fhand:
            pickle.dump(result, fhand)
    return result


def search_id_for_experiment_acc(acc: str) -> str:
    url = NCBI_SEARCH_BASE_URL + f"db=sra&term={acc}[Accession]&retmode=json&retmax=1"
    response = requests.get(url)
    assert response.status_code == 200
    content = response.content
    search_result = json.loads(content)
    if not search_result["esearchresult"]:
        raise ValueError(f"acc {acc} not found in the sra database")

    if len(search_result["esearchresult"]["idlist"]) != 1:
        raise RuntimeError(f"We expected just 1 id in idlist for acc: {acc}")

    id_ = search_result["esearchresult"]["idlist"][0]
    if not id_.isdigit():
        raise RuntimeError(
            f"We expected an all digit id for acc {acc}, but we got: {id_}"
        )

    return id_


if __name__ == "__main__":
    # id_ = search_id_for_experiment_acc("SRX27341610")
    cache_dir = Path("__file__").absolute().parent / "cache"
    cache_dir.mkdir(exist_ok=True)

    id_ = cache_call(
        search_id_for_experiment_acc, args=("SRX27341610",), cache_dir=cache_dir
    )
    print(id_)

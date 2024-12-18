import codecs
from typing import Callable, Dict

def loadMovieNames() ->  Dict[int, str]: 
    movieNames: Dict[int, str] = {}
    with codecs.open("./data/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields: str = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def getLookupFunctionForDict(idToNameDictionary: Dict[int, str]) -> Callable[[int], str]:
    def lookupNameByMovieId(movieId: int) -> str:
        return idToNameDictionary[movieId]
    return lookupNameByMovieId
from asyncio import Task, create_task, gather, run
from os import path
from typing import Dict, Generator, List

from aiofiles import open as async_open
from aiohttp import ClientSession


def chunks(lst: List, n: int) -> Generator[List, None, None]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def convert(obj: Dict) -> Dict:
    prefix = str(obj["AUTORIDAD"]).lower().replace(" ", "_")

    r: Dict = {}

    r[prefix + "_total_valids"] = -1
    if "CON_VALIDOS" in obj.keys():
        r[prefix + "_total_valids"] = obj["CON_VALIDOS"]

    r[prefix + "_total_emiteds"] = -1
    if "CON_EMITIDOS" in obj.keys():
        r[prefix + "_total_emiteds"] = obj["CON_EMITIDOS"]

    r[prefix + "_code"] = -1
    if "CCODI_AUTO" in obj.keys():
        r[prefix + "_code"] = obj["CCODI_AUTO"]

    r[prefix + "_congresal"] = -1
    if "congresal" in obj.keys():
        r[prefix + "_congresal"] = obj["congresal"]

    r[prefix + "_list_code"] = -1
    if "NLISTA" in obj.keys():
        r[prefix + "_list_code"] = obj["NLISTA"]

    return r


async def extract_mesa_data(session: ClientSession, url: str) -> Dict:
    async with session.get(url) as response:
        try:
            data = await response.json()

            metadata = data["procesos"]["generalPre"]["presidencial"]
            results = data["procesos"]["generalPre"]["votos"]

            counts = list(map(convert, results))

            row = {
                "ubigeo": metadata["CCODI_UBIGEO"],
                "place": metadata["TNOMB_LOCAL"],
                "address": metadata["TDIRE_LOCAL"],
                # "": metadata["CCENT_COMPU"],
                "department": metadata["DEPARTAMENTO"],
                "province": metadata["PROVINCIA"],
                "district": metadata["DISTRITO"],
                "copy_code": metadata["CCOPIA_ACTA"],
                # "": metadata["NNUME_HABILM"],
                "observation": metadata["OBSERVACION"],
                "description": metadata["OBSERVACION_TXT"],
                "candidates": metadata["N_CANDIDATOS"],
                "total_citizens": metadata["TOT_CIUDADANOS_VOTARON"],
            }

            for count in counts:
                for k in count.keys():
                    row[k] = count[k]

            return row

        except BaseException as e:
            print(f"[ERROR] {e}")

    return {}


async def process_chunk(start: int = 0, end: int = int(1e6), folder: str = "./"):
    base = "https://api.resultadossep.eleccionesgenerales2021.pe/mesas/detalle"
    # start, end = 5930, int(1e6)  # six digits

    mesa_range = list(range(start, end+1))
    total_requests_at_time = 100

    filename = path.join(folder, f"dataset_{start}_{end}.csv")

    async with async_open(filename, mode="w+") as file:
        header: str = ""
        for part in chunks(mesa_range, total_requests_at_time):
            async with ClientSession() as session:
                http_tasks: List[Task[Dict]] = []
                for mesa_number in part:
                    url = f"{base}/{mesa_number:06d}"
                    print(url)

                    co = extract_mesa_data(session, url)
                    http_tasks.append(create_task(co))

                result = await gather(*http_tasks)

                # contents = await f.read()
                io_tasks: List[Task[int]] = []
                for r in result:
                    if len(r) == 0:
                        continue

                    line = ",".join(map(lambda x: str(x), r.values())) + "\n"
                    header = ",".join(r.keys())
                    io_tasks.append(create_task(file.write(line)))

                await gather(*io_tasks)

    # prepend the header to our csv
    with open(filename, "r+") as f:
        content = f.read()
        f.seek(0, 0)
        f.write(header.rstrip('\r\n') + '\n' + content)


if __name__ == "__main__":
    for c in chunks(list(range(1, int(1e6))), 10000):
        part = list(c)
        run(process_chunk(
            start=part[0],
            end=part[-1],
            folder="dataset"
        ))

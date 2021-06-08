from asyncio.tasks import create_task
from typing import Any, Dict, Generator, List
import pandas as pd
import aiohttp
import asyncio
import aiofiles


def chunks(lst: List, n: int) -> Generator[List, None, None]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def extract_mesa_data(session: aiohttp.ClientSession, url: str) -> Dict:
    async with session.get(url) as response:
        data = await response.json()
        # print(data)
        metadata = data["procesos"]["generalPre"]["presidencial"]
        # print(metadata)
        results = data["procesos"]["generalPre"]["votos"]

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

        print(row)

        return row


async def main_process():
    base = "https://api.resultadossep.eleccionesgenerales2021.pe/mesas/detalle"
    mesa_range = range(1, 3)  # int(1e6))  # six digits
    total_requests_at_time = 100

    dataset = pd.DataFrame({
        "ubigeo": [""],
        "place": [""],
        "address": [""],
        "department": [""],
        "province": [""],
        "district": [""],
        "copy_code": [""],
        "observation": [""],
        "description": [""],
        "candidates": [""],
        "total_citizens": [0],
    })

    for part in chunks(mesa_range, total_requests_at_time):
        async with aiohttp.ClientSession() as session:
            tasks: List[asyncio.Task[Dict]] = []
            for mesa_number in part:
                url = f"{base}/{mesa_number:06d}"
                print(url)
                tasks.append(create_task(extract_mesa_data(session, url)))

            result = await asyncio.gather(*tasks)

            async with aiofiles.open('example.csv', mode='rw+') as f:
                # contents = await f.read()
                tasks: List[asyncio.Task[None]] = []
                for r in result:
                    line = ",".join(r.values())
                    tasks.append(create_task(f.write(line)))

                r = await asyncio.gather(*tasks)
                print(r)

asyncio.run(main_process())

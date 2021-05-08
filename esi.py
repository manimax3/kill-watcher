import aiohttp

esi_endpoint = "https://esi.evetech.net/latest"


async def fetch_killmail(killid, hash):
    async with aiohttp.ClientSession() as s:

        # If we dont have a hash we can ask zkillboard
        if hash is None:
            async with s.get(
                    f"https://zkillboard.com/api/killID/{killid}/") \
                    as response:
                zkb_data = await response.json()
                zkb_data = zkb_data[0]["zkb"]
                hash = zkb_data["hash"]

        async with s.get(
            f"{esi_endpoint}/killmails/{killid}/{hash}/?datasource=tranquility") \
                as response:
            killmail = await response.json()
            return killmail


async def fetch_system(system_id):
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{esi_endpoint}/universe/systems/{system_id}/?datasource=tranquility") \
                as response:
            system = await response.json()
            return system


async def fetch_corporation(corp_id, alliance=False):
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{esi_endpoint}/corporations/{corp_id}/") \
                as response:
            corp_info = await response.json()
            if not alliance:
                return corp_info

        if "alliance_id" not in corp_info:
            return corp_info, None

        async with s.get(
            f"{esi_endpoint}/alliances/{corp_info['alliance_id']}/") \
                as response:
            alli_info = await response.json()
            return corp_info, alli_info

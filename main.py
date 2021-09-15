import httpx
import pandas as pd
import json
import asyncio
from loguru import logger
import os
import gc

from util import *

FIRM_URL = r'https://iir.ia.org.hk/IISPublicRegisterRestfulAPI/v1/search/firm?seachIndicator=licNo&searchValue={LICENSE_ID}&status=A&page=1&pagesize=1'
FIRM_DETAIL_URL = r'https://iir.ia.org.hk/IISPublicRegisterRestfulAPI/v1/search/firmDetail?key={FIRM_KEY}&licStatus=all'
INDIV_URL = r'https://iir.ia.org.hk/IISPublicRegisterRestfulAPI/v1/search/individual?seachIndicator=licNo&searchValue={LICENSE_ID}&status=all&page=1&pagesize=10'
INDIV_DETAIL_URL = r'https://iir.ia.org.hk/IISPublicRegisterRestfulAPI/v1/search/individualDetail?key={FIRM_KEY}&licStatus=all'
LICENSE_TYPE = {"AGY":"Insurance Agency", "BKR":"Insurance Broker Company", "IND":"Individual Insurance Agent", "TRA":"Technical Representative (Agent)", "TRB":"Technical Representative (Broker)"}
BATCH_COUNT = 10

POLII_KEYS = {
    'key': ('key', 'k'), 
    'License No': ('licenseNo', 'k'), 
    'English Name': ('engName', 'k'), 
    'Chineese Name': ('chiName', 'k'), 
    'License Type': ('licenseType', 'lt'), 
    'License Period Start Date': ('licenseStartDate', 'd'), 
    'License Period End Date': ('licenseEndDate', 'd'), 
    'Business Address': ('businessEngAddress', 'k'), 
    'Responsible Officer(s) English Name': ('', '.', ['officers', 'name', 0, -1]), 
    'Responsible Officer(s) Chineese Name': ('', '.', ['officers', 'name', -1, None])
}
POLII_COLUMNS = ['key', 'License No', 'English Name', 'Chineese Name', 'License Type', 'License Period Start Date', 'License Period End Date', 'Business Address', 'Responsible Officer(s) English Name', 'Responsible Officer(s) Chineese Name']
POLII = []
CLD = []
PLD = []
PUBA = []
NOTES = []
CNDS = []

async def fetch_polii(data):
    POLII_VALUES = []
    for i in POLII_COLUMNS:
        key, _type, *args = POLII_KEYS[i]
        if _type == 'k':
            POLII_VALUES.append(data[key])
        elif _type == 'd':
            val = data[key]
            if val is not None:
                val = get_date(val)

            POLII_VALUES.append(val)
        elif _type == '.':
            args = args[0]
            if args[0] not in data:
                POLII_VALUES.append("")
                continue

            val = ','.join(map(lambda x: ' '.join(x[args[1]].split(" ")[args[2]: args[3]]), data[args[0]]))
            POLII_VALUES.append(val)
        elif _type == 'c':
            POLII_VALUES.append(args[0])
        elif _type == 'lt':
            POLII_VALUES.append(LICENSE_TYPE[data[key]])
    
    POLII.append(POLII_VALUES)

async def fetch_cld(data):
    for i in data['appointments']:
        app_data = {
            'licence': data['licenseNo'],
            'key': data['key'],
            'Appointing Principal': i['appointing_en'],
            'Appointing Principal License Number': i['licenceNo'],
            'Line of Business': i['lineBusiness'],
            'Date of appointment for the Relevant Line of Business': get_date(i['roAppointingDate']),
        }

        CLD.append(app_data)

async def fetch_pld(data):
    for i in data['licenseeRecords']:
        lic_data = {
            'licence': data['licenseNo'],
            'key': data['key'],
            'License Type': LICENSE_TYPE[i['type']],
            'License Period Start Date': get_date(i['periodStart']),
            'License Period End Date': get_date(i['periodEnd']),
            'Appointing Principal': i['appointing'],
            'Appointing Principal License Number': i['parentLicenseNo'],
            'Line of Business': i['lineBusiness'],
            'Appointment Start Date': get_date(i['startDate']),
            'Termination Date': get_date(i['endDate'])
        }

        PLD.append(lic_data)

async def fetch_puba(data):
    for i in data['publicActions']:
        action_data = {
            'licence': data['licenseNo'],
            'key': data['key'],
            'Date of Action': get_date(i['actionDate']),
            'Action Taken': i['actionTakenEN'],
            'Press Release': ','.join(i['pressReleases'])
        }

        PUBA.append(action_data)

async def fetch_notes(data):
    NOTES.append({
            'licence': data['licenseNo'],
            'key': data['key'],
            'Notes': data['notes']
    })

async def fetch_condns(data):
    for i in data['licenseConds']:
        cnd_data = {
            'licence': data['licenseNo'],
            'key': data['key'],
            'Effect Start Date': get_date(i['effectDate']),
            'Effect End Date': get_date(i['effectEndDate']),
            'Condition': i['condition']
        }

        CNDS.append(cnd_data)

async def generate_fetch_all_firm(lid, tries=3):
    logger.debug(f"Trying License ID: {lid}")
    if tries < 1:
        logger.debug(f"License ID: {lid} not found")
        return 

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(FIRM_URL.format(LICENSE_ID = lid))
            data = r.json()

            if len(data['data']) < 1:
                logger.debug(f"License ID: {lid} not found")
                return

            logger.debug(f"Found License ID: {lid}")
            data = data['data'][0]
            key = data['key']
            r = await client.get(FIRM_DETAIL_URL.format(FIRM_KEY = key))
            logger.debug(f"Details of License ID: {lid} fetched successfully. Parsing them.")
            data_final = r.json()
            data.update(data_final)
            
            await fetch_polii(data)
            await fetch_cld(data)
            await fetch_pld(data)
            await fetch_puba(data)
            await fetch_notes(data)
            await fetch_condns(data)

        except:
            logger.debug(f"License ID: {lid} not found")
            return

async def main():
    global POLII, CLD, PLD, PUBA, NOTES, CNDS
    digits = range(1000, 10000)
    logger.info("Scrapper started [firm data].")

    for batch in generate_batch(digits, BATCH_COUNT):
        list(i.clear() for i in (POLII, CLD, PLD, PUBA, NOTES, CNDS))
        gc.collect()

        lids = [*(f'FA{i:0=4d}' for i in batch), *(f'FB{i:0=4d}' for i in batch), *(f'GB{i:0=4d}' for i in batch)]
        aws = list(generate_fetch_all_firm(i) for i in lids)

        await asyncio.gather(*aws)

        logger.info("Dumping so far scrapped data")
        print(len(POLII))
        if len(POLII):
            _POLII = pd.DataFrame(POLII, columns = POLII_COLUMNS)
            with open("extracted/firm/polii.csv", 'a', encoding="utf-8") as f:
                _POLII.to_csv(f, mode="a", header=not f.tell())
                #POLII.to_json("extracted/firm/polii.json", mode="a", header=False)
            _POLII.to_xml("extracted/firm/polii.xml", mode="a")
            logger.info(f"Dumped {len(POLII)} Particulars of Licensed Insurance Intermediatory to extracted/firm/polii.csv, polii.xml, polii.json")
            del _POLII
            POLII.clear()

        if len(CLD):
            _CLD = pd.DataFrame(CLD)
            with open("extracted/firm/cld.csv", 'a', encoding="utf-8") as f:
                _CLD.to_csv(f, mode="a", header=not f.tell())
                #CLD.to_json("extracted/firm/cld.json", mode="a", header=False)

            _CLD.to_xml("extracted/firm/cld.xml", mode="a")
            del _CLD
            logger.info(f"Dumped {len(CLD)} Current License Details to extracted/firm/cld.csv, cld.xml, cld.json")
            CLD.clear()

        if len(PLD):
            _PLD = pd.DataFrame(PLD)
            with open("extracted/firm/pld.csv", 'a', encoding="utf-8") as f:
                _PLD.to_csv(f, mode="a", header=not f.tell())
                #PLD.to_json("extracted/firm/pld.json", mode="a", header=False)

            _PLD.to_xml("extracted/firm/pld.xml", mode="a")
            del _PLD
            logger.info(f"Dumped {len(PLD)} Previous License Details to extracted/firm/pld.csv, pld.xml, pld.json")
            PLD.clear()

        if len(PUBA):
            _PUBA = pd.DataFrame(PUBA)
            with open("extracted/firm/puba.csv", 'a', encoding="utf-8") as f:
                _PUBA.to_csv(f, mode="a", header=not f.tell())
                #PUBA.to_json("extracted/firm/puba.json", mode="a", header=False)

            _PUBA.to_xml("extracted/firm/puba.xml", mode="a")
            del _PUBA
            logger.info(f"Dumped {len(PUBA)} Public Disciplinary Actions Taken to extracted/firm/puba.csv, puba.xml, puba.json")
            PUBA.clear()

        if len(NOTES):
            _NOTES = pd.DataFrame(NOTES)
            with open("extracted/firm/notes.csv", 'a', encoding="utf-8") as f:
                _NOTES.to_csv(f, mode="a", header=not f.tell())
                #NOTES.to_json("extracted/firm/notes.json", mode="a", header=False)

            _NOTES.to_xml("extracted/firm/notes.xml", mode="a")
            del _NOTES
            logger.info(f"Dumped {len(NOTES)} Notes to extracted/firm/notes.csv, notes.xml, notes.json")
            NOTES.clear()

        if len(CNDS):
            _CNDS = pd.DataFrame(CNDS)
            with open("extracted/firm/cnds.csv", 'a', encoding="utf-8") as f:
                _CNDS.to_csv(f, mode="a", header=not f.tell())
                #CNDS.to_json("extracted/firm/cnds.json", mode="a", header=False)

            _CNDS.to_xml("extracted/firm/cnds.xml", mode="a")
            del _CNDS
            logger.info(f"Dumped {len(CNDS)} Conditions of License to extracted/firm/cndns.csv, cndns.xml, cndns.json")
            CNDS.clear()

    logger.info("Scrapper has stopped.")

async def generate_fetch_all_indiv(lid, tries=3):
    logger.debug(f"Trying License ID: {lid}")
    if tries < 1:
        logger.debug(f"License ID: {lid} not found")
        return 

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(INDIV_URL.format(LICENSE_ID = lid))
            data = r.json()

            if len(data['data']) < 1:
                logger.debug(f"License ID: {lid} not found")
                return

            logger.debug(f"Found License ID: {lid}")
            data = data['data'][0]
            key = data['key']
            r = await client.get(INDIV_DETAIL_URL.format(FIRM_KEY = key))
            logger.debug(f"Details of License ID: {lid} fetched successfully. Parsing them.")
            data_final = r.json()
            data.update(data_final)
            
            await fetch_polii(data)
            await fetch_cld(data)
            await fetch_pld(data)
            await fetch_puba(data)
            await fetch_notes(data)
            await fetch_condns(data)
            print("HELLO?")

        except:
            logger.debug(f"License ID: {lid} not found")
            return

def generate_indiv_batch(BATCH_COUNT):
    alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    for i in alpha:
        for j in range(1000, 10000, BATCH_COUNT):
            yield list(f'{i}{k:0=4d}' for k in range(j, j+BATCH_COUNT))


async def main_indiv():
    global POLII, CLD, PLD, PUBA, NOTES, CNDS, BATCH_COUNT
    logger.info("Scrapper started [individual data].")

    for batch in generate_indiv_batch(BATCH_COUNT):
        gc.collect()

        lids = [*(f'I{i}' for i in batch), *(f'J{i}' for i in batch)]
        aws = list(generate_fetch_all_indiv(i) for i in lids)

        await asyncio.gather(*aws)

        logger.info("Dumping so far scrapped data")
        print(len(POLII))
        if len(POLII):
            _POLII = pd.DataFrame(POLII, columns = POLII_COLUMNS)
            with open("extracted/individual/polii.csv", 'a', encoding="utf-8") as f:
                _POLII.to_csv(f, mode="a", header=not f.tell())
                #POLII.to_json("extracted/firm/polii.json", mode="a", header=False)
            _POLII.to_xml("extracted/individual/polii.xml", mode="a")
            logger.info(f"Dumped {len(POLII)} Particulars of Licensed Insurance Intermediatory to extracted/individual/polii.csv, polii.xml, polii.json")
            del _POLII
            POLII.clear()

        if len(CLD):
            _CLD = pd.DataFrame(CLD)
            with open("extracted/individual/cld.csv", 'a', encoding="utf-8") as f:
                _CLD.to_csv(f, mode="a", header=not f.tell())
                #CLD.to_json("extracted/firm/cld.json", mode="a", header=False)

            _CLD.to_xml("extracted/individual/cld.xml", mode="a")
            del _CLD
            logger.info(f"Dumped {len(CLD)} Current License Details to extracted/individual/cld.csv, cld.xml, cld.json")
            CLD.clear()

        if len(PLD):
            _PLD = pd.DataFrame(PLD)
            with open("extracted/individual/pld.csv", 'a', encoding="utf-8") as f:
                _PLD.to_csv(f, mode="a", header=not f.tell())
                #PLD.to_json("extracted/firm/pld.json", mode="a", header=False)

            _PLD.to_xml("extracted/individual/pld.xml", mode="a")
            del _PLD
            logger.info(f"Dumped {len(PLD)} Previous License Details to extracted/individual/pld.csv, pld.xml, pld.json")
            PLD.clear()

        if len(PUBA):
            _PUBA = pd.DataFrame(PUBA)
            with open("extracted/individual/puba.csv", 'a', encoding="utf-8") as f:
                _PUBA.to_csv(f, mode="a", header=not f.tell())
                #PUBA.to_json("extracted/firm/puba.json", mode="a", header=False)

            _PUBA.to_xml("extracted/individual/puba.xml", mode="a")
            del _PUBA
            logger.info(f"Dumped {len(PUBA)} Public Disciplinary Actions Taken to extracted/individual/puba.csv, puba.xml, puba.json")
            PUBA.clear()

        if len(NOTES):
            _NOTES = pd.DataFrame(NOTES)
            with open("extracted/individual/notes.csv", 'a', encoding="utf-8") as f:
                _NOTES.to_csv(f, mode="a", header=not f.tell())
                #NOTES.to_json("extracted/firm/notes.json", mode="a", header=False)

            _NOTES.to_xml("extracted/individual/notes.xml", mode="a")
            del _NOTES
            logger.info(f"Dumped {len(NOTES)} Notes to extracted/individual/notes.csv, notes.xml, notes.json")
            NOTES.clear()

        if len(CNDS):
            _CNDS = pd.DataFrame(CNDS)
            with open("extracted/individual/cnds.csv", 'a', encoding="utf-8") as f:
                _CNDS.to_csv(f, mode="a", header=not f.tell())
                #CNDS.to_json("extracted/firm/cnds.json", mode="a", header=False)

            _CNDS.to_xml("extracted/individual/cnds.xml", mode="a")
            del _CNDS
            logger.info(f"Dumped {len(CNDS)} Conditions of License to extracted/individual/cndns.csv, cndns.xml, cndns.json")
            CNDS.clear()

    logger.info("Scrapper has stopped.")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.run_until_complete(main_indiv())